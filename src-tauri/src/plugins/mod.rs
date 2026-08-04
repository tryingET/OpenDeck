pub mod info_param;
pub mod manifest;
mod webserver;

use crate::APP_HANDLE;
use crate::built_info::TARGET;
use crate::shared::{CATEGORIES, Category, config_dir, convert_icon, is_flatpak, log_dir};

use std::collections::HashMap;
use std::process::{Child, Command, Stdio};
use std::sync::{
	Arc, LazyLock,
	atomic::{AtomicU8, Ordering},
	mpsc,
};
use std::{
	fs,
	path::{self, Path},
};

use tauri::{AppHandle, Manager};

use futures::StreamExt;
use tokio::net::{TcpListener, TcpStream};
use tokio_tungstenite::tungstenite::{
	handshake::server::{ErrorResponse, Request, Response},
	http::{StatusCode, header::ORIGIN},
	protocol::WebSocketConfig,
};

use anyhow::anyhow;
use log::{error, warn};
use tokio::sync::{Mutex, RwLock, Semaphore, oneshot};

pub enum PluginChildType {
	Wine,
	Native,
	Node,
}

enum PluginInstance {
	Webview,
	Wine(Child),
	Native(Child),
	Node(Child),
}

pub static DEVICE_NAMESPACES: LazyLock<RwLock<HashMap<String, String>>> = LazyLock::new(|| RwLock::new(HashMap::new()));
static INSTANCES: LazyLock<Mutex<HashMap<String, PluginInstance>>> = LazyLock::new(|| Mutex::new(HashMap::new()));
static PLUGIN_LIFECYCLE_LOCKS: LazyLock<Mutex<HashMap<String, Arc<Mutex<()>>>>> = LazyLock::new(|| Mutex::new(HashMap::new()));

pub async fn lock_plugin_lifecycle(uuid: &str) -> tokio::sync::OwnedMutexGuard<()> {
	let lock = PLUGIN_LIFECYCLE_LOCKS.lock().await.entry(uuid.to_owned()).or_insert_with(|| Arc::new(Mutex::new(()))).clone();
	lock.lock_owned().await
}

pub(crate) const ALTERNATIVE_ELGATO_PLUGIN_UUID: &str = "opendeck_alternative_elgato_implementation";
const MAX_WEBSOCKET_MESSAGE_BYTES: usize = 1024 * 1024;
const MAX_PENDING_WEBSOCKET_REGISTRATIONS: usize = 128;
const CHILD_STOP_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(3);

pub(crate) fn is_safe_plugin_uuid(uuid: &str) -> bool {
	!uuid.is_empty()
		&& uuid.len() <= 255
		&& uuid.ends_with(".sdPlugin")
		&& uuid.bytes().all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'-' | b'_'))
		&& Path::new(uuid).file_name().and_then(|name| name.to_str()) == Some(uuid)
		&& Path::new(uuid).components().count() == 1
}

fn canonical_installed_plugin_path(uuid: &str) -> Option<path::PathBuf> {
	if !is_safe_plugin_uuid(uuid) {
		return None;
	}

	let plugin_root = config_dir().join("plugins").canonicalize().ok()?;
	let plugin_path = plugin_root.join(uuid).canonicalize().ok()?;
	(plugin_path.starts_with(&plugin_root) && plugin_path.is_dir() && plugin_path.join("manifest.json").is_file()).then_some(plugin_path)
}

pub(crate) fn is_installed_plugin_uuid(uuid: &str) -> bool {
	canonical_installed_plugin_path(uuid).is_some()
}

pub(crate) fn is_known_plugin_registration_uuid(uuid: &str) -> bool {
	uuid == ALTERNATIVE_ELGATO_PLUGIN_UUID || is_installed_plugin_uuid(uuid)
}

#[derive(Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
enum WebSocketOrigin {
	Native = 1,
	PluginServer = 2,
	SandboxedPropertyInspector = 3,
}

fn websocket_origin(origin: Option<&str>, port_base: u16) -> Option<WebSocketOrigin> {
	match origin {
		None => Some(WebSocketOrigin::Native),
		Some("null") => Some(WebSocketOrigin::SandboxedPropertyInspector),
		Some(origin) => {
			let webserver_port = port_base + 2;
			(origin == format!("http://localhost:{webserver_port}") || origin == format!("http://127.0.0.1:{webserver_port}")).then_some(WebSocketOrigin::PluginServer)
		}
	}
}

#[cfg(test)]
fn websocket_origin_allowed(origin: Option<&str>, port_base: u16) -> bool {
	websocket_origin(origin, port_base).is_some()
}

pub(crate) fn is_allowed_external_url(url: &str) -> bool {
	reqwest::Url::parse(url).is_ok_and(|url| matches!(url.scheme(), "http" | "https" | "mailto"))
}

pub static PORT_BASE: LazyLock<u16> = LazyLock::new(|| {
	let mut base = 57116;
	loop {
		let websocket_result = std::net::TcpListener::bind(("127.0.0.1", base));
		let webserver_result = std::net::TcpListener::bind(("127.0.0.1", base + 2));
		if websocket_result.is_ok() && webserver_result.is_ok() {
			log::debug!("Using ports {} and {}", base, base + 2);
			break;
		}
		base += 1;
	}
	base
});

/// Attach a kernel-enforced "die when parent dies" signal to a plugin child process.
#[cfg(target_os = "linux")]
fn attach_parent_death_signal(command: &mut Command) {
	use std::os::unix::process::CommandExt;
	// SAFETY: `libc::prctl` is async-signal-safe.
	unsafe {
		command.pre_exec(move || {
			if libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGTERM as libc::c_ulong) != 0 {
				return Err(std::io::Error::last_os_error());
			}
			Ok(())
		});
	}
}

async fn stop_child(mut child: Child) -> Result<(), anyhow::Error> {
	let stop = tokio::task::spawn_blocking(move || -> Result<(), std::io::Error> {
		if child.try_wait()?.is_none() {
			child.kill()?;
		}
		let _ = child.wait()?;
		Ok(())
	});
	match tokio::time::timeout(CHILD_STOP_TIMEOUT, stop).await {
		Ok(Ok(result)) => result.map_err(Into::into),
		Ok(Err(error)) => Err(anyhow!("plugin stop task failed: {error}")),
		Err(_) => Err(anyhow!("plugin process did not stop within {} seconds", CHILD_STOP_TIMEOUT.as_secs())),
	}
}

type SpawnFactory = Box<dyn FnOnce() -> Result<(String, PluginChildType, Command), anyhow::Error> + Send>;

pub struct SpawnRequest {
	factory: SpawnFactory,
	completion: oneshot::Sender<Result<(), String>>,
}

async fn request_spawn(spawner_tx: &mpsc::Sender<SpawnRequest>, factory: SpawnFactory) -> anyhow::Result<()> {
	let (completion, result) = oneshot::channel();
	spawner_tx.send(SpawnRequest { factory, completion }).map_err(|error| anyhow!(error.to_string()))?;
	match result.await {
		Ok(Ok(())) => Ok(()),
		Ok(Err(error)) => Err(anyhow!(error)),
		Err(error) => Err(anyhow!("plugin spawner dropped its response: {error}")),
	}
}

/// Initialise a plugin from a given directory.
pub async fn initialise_plugin(path: path::PathBuf, spawner_tx: mpsc::Sender<SpawnRequest>) -> anyhow::Result<()> {
	let plugin_uuid = path
		.file_name()
		.and_then(|name| name.to_str())
		.ok_or_else(|| anyhow!("plugin directory name is not valid UTF-8"))?
		.to_owned();
	if !is_safe_plugin_uuid(&plugin_uuid) {
		return Err(anyhow!("unsafe plugin directory name: {plugin_uuid}"));
	}
	if INSTANCES.lock().await.contains_key(&plugin_uuid) {
		return Err(anyhow!("plugin {plugin_uuid} is already running"));
	}
	let supplied_path = path.canonicalize()?;
	let path = canonical_installed_plugin_path(&plugin_uuid).ok_or_else(|| anyhow!("plugin directory is outside the canonical plugin root: {plugin_uuid}"))?;
	if supplied_path != path {
		return Err(anyhow!("supplied plugin path does not match the canonical installed path: {plugin_uuid}"));
	}
	let plugin_uuid_2 = plugin_uuid.clone();

	let mut manifest = manifest::read_manifest(&path)?;

	if let Some(icon) = manifest.category_icon {
		let category_icon_path = path.join(icon);
		manifest.category_icon = Some(convert_icon(category_icon_path.to_string_lossy().to_string()));
	}

	for action in &mut manifest.actions {
		plugin_uuid.clone_into(&mut action.plugin);

		let action_icon_path = path.join(action.icon.clone());
		action.icon = convert_icon(action_icon_path.to_str().unwrap().to_owned());

		if !action.property_inspector.is_empty() {
			action.property_inspector = path.join(&action.property_inspector).to_string_lossy().to_string();
		} else if let Some(ref property_inspector) = manifest.property_inspector_path {
			action.property_inspector = path.join(property_inspector).to_string_lossy().to_string();
		}

		if let Some(encoder) = &mut action.encoder {
			if !encoder.icon.is_empty() {
				let encoder_icon_path = path.join(encoder.icon.clone());
				encoder.icon = convert_icon(encoder_icon_path.to_str().unwrap().to_owned());
			}

			if !encoder.background.is_empty() {
				let encoder_background_path = path.join(encoder.background.clone());
				encoder.background = convert_icon(encoder_background_path.to_str().unwrap().to_owned());
			}
		}

		for state in &mut action.states {
			if state.image == "actionDefaultImage" {
				state.image.clone_from(&action.icon);
			} else {
				let state_icon = path.join(state.image.clone());
				state.image = convert_icon(state_icon.to_str().unwrap().to_owned());
			}

			match state.family.clone().to_lowercase().trim() {
				"arial" => "Liberation Sans",
				"arial black" => "Archivo Black",
				"comic sans ms" => "Comic Neue",
				"courier" | "Courier New" => "Courier Prime",
				"georgia" => "Tinos",
				"impact" => "Anton",
				"microsoft sans serif" | "Times New Roman" => "Liberation Serif",
				"tahoma" | "Verdana" => "Open Sans",
				"trebuchet ms" => "Fira Sans",
				_ => continue,
			}
			.clone_into(&mut state.family);
		}
	}

	{
		let mut categories = CATEGORIES.write().await;
		if let Some(category) = categories.get_mut(&manifest.category) {
			for action in manifest.actions {
				if let Some(index) = category.actions.iter().position(|v| v.uuid == action.uuid) {
					category.actions.remove(index);
				}
				category.actions.push(action);
			}
		} else {
			let mut category: Category = Category {
				icon: manifest.category_icon,
				actions: vec![],
			};
			for action in manifest.actions {
				category.actions.push(action);
			}
			if !category.actions.is_empty() {
				categories.insert(manifest.category, category);
			}
		}
	}

	if let Some(namespace) = manifest.device_namespace {
		DEVICE_NAMESPACES.write().await.insert(namespace, plugin_uuid.to_owned());
	}

	#[cfg(target_os = "windows")]
	let platform = "windows";
	#[cfg(target_os = "macos")]
	let platform = "mac";
	#[cfg(target_os = "linux")]
	let platform = "linux";

	let mut code_path = manifest.code_path;
	let mut use_wine = false;
	let mut supported = false;

	// Determine the method used to run the plugin based on its supported operating systems and the current operating system.
	for os in manifest.os {
		if os.platform == platform {
			#[cfg(target_os = "windows")]
			if manifest.code_path_windows.is_some() {
				code_path = manifest.code_path_windows.clone();
			}
			#[cfg(target_os = "macos")]
			if manifest.code_path_macos.is_some() {
				code_path = manifest.code_path_macos;
			}
			#[cfg(target_os = "linux")]
			if manifest.code_path_linux.is_some() {
				code_path = manifest.code_path_linux;
			}
			code_path = manifest.code_paths.and_then(|p| p.get(TARGET).cloned()).or(code_path);

			use_wine = false;

			supported = true;
			break;
		} else if os.platform == "windows" {
			use_wine = true;
			supported = true;
		}
	}

	if code_path.is_none() && use_wine {
		code_path = manifest.code_path_windows;
	}

	if !supported || code_path.is_none() {
		return Err(anyhow!("unsupported on platform {}", platform));
	}

	let code_path = code_path.unwrap();
	let args = [
		"-port".to_owned(),
		PORT_BASE.to_string(),
		"-pluginUUID".to_owned(),
		plugin_uuid.clone(),
		"-registerEvent".to_owned(),
		"registerPlugin".to_owned(),
		"-info".to_owned(),
	];

	if code_path.to_lowercase().ends_with(".html") || code_path.to_lowercase().ends_with(".htm") || code_path.to_lowercase().ends_with(".xhtml") {
		let url = format!("http://localhost:{}/", *PORT_BASE + 2) + path.join(code_path).to_str().unwrap();
		let window = tauri::WebviewWindowBuilder::new(APP_HANDLE.get().unwrap(), plugin_uuid.replace('.', "_"), tauri::WebviewUrl::External(url.parse()?))
			.title(manifest.name)
			.visible(false)
			.build()?;

		if fs::exists(path.join("debug")).unwrap_or(false) {
			let _ = window.show();
			window.open_devtools();
		}

		let info = info_param::make_info(plugin_uuid.to_owned(), manifest.version, false).await;
		window.eval(format!(
			r#"const opendeckInit = () => {{
				try {{
					if (document.readyState !== "complete") throw new Error("not ready");
					if (typeof connectOpenActionSocket === "function") connectOpenActionSocket({port}, "{uuid}", "{event}", `{info}`);
					else connectElgatoStreamDeckSocket({port}, "{uuid}", "{event}", `{info}`);
				}} catch (e) {{
					setTimeout(opendeckInit, 10);
				}}
			}};
			opendeckInit();
			"#,
			port = *PORT_BASE,
			uuid = plugin_uuid,
			event = "registerPlugin",
			info = serde_json::to_string(&info)?
		))?;

		INSTANCES.lock().await.insert(plugin_uuid, PluginInstance::Webview);
	} else if code_path.to_lowercase().ends_with(".js") || code_path.to_lowercase().ends_with(".mjs") || code_path.to_lowercase().ends_with(".cjs") {
		// Check for Node.js installation and version in one go.
		let command = if is_flatpak() { "flatpak-spawn" } else { "node" };
		let extra_args = if is_flatpak() { vec!["--host", "node"] } else { vec![] };
		let version_output = Command::new(command).args(&extra_args).arg("--version").output();
		if version_output.is_err() || String::from_utf8(version_output.unwrap().stdout).unwrap().trim() < "v20.0.0" {
			return Err(anyhow!("Node.js version 20.0.0 or higher is required"));
		}

		let info = info_param::make_info(plugin_uuid.to_owned(), manifest.version, true).await;
		let log_file = fs::File::create(log_dir().join("plugins").join(format!("{plugin_uuid}.log")))?;

		request_spawn(
			&spawner_tx,
			Box::new(move || {
				let mut command = Command::new(command);
				command
					.current_dir(path)
					.args(extra_args)
					.arg(code_path)
					.args(args)
					.arg(serde_json::to_string(&info)?)
					.stdout(Stdio::from(log_file.try_clone()?))
					.stderr(Stdio::from(log_file));
				#[cfg(target_os = "linux")]
				attach_parent_death_signal(&mut command);
				#[cfg(target_os = "windows")]
				{
					use std::os::windows::process::CommandExt;
					command.creation_flags(0x08000000);
				}
				Ok((plugin_uuid, PluginChildType::Node, command))
			}),
		)
		.await?;
	} else if use_wine {
		let command = if is_flatpak() { "flatpak-spawn" } else { "wine" };
		let extra_args = if is_flatpak() { vec!["--host", "wine"] } else { vec![] };
		let result = Command::new(command)
			.args(&extra_args)
			.arg("--version")
			.stdout(Stdio::null())
			.stderr(Stdio::null())
			.spawn()
			.and_then(|mut child| child.wait())
			.map(|status| status.success());
		if !matches!(result, Ok(true)) {
			return Err(anyhow!("failed to detect an installation of Wine"));
		}

		let info = info_param::make_info(plugin_uuid.to_owned(), manifest.version, true).await;
		let log_file = fs::File::create(log_dir().join("plugins").join(format!("{plugin_uuid}.log")))?;

		request_spawn(
			&spawner_tx,
			Box::new(move || {
				let mut command = Command::new(command);
				command
					.current_dir(&path)
					.args(extra_args)
					.arg(code_path)
					.args(args)
					.arg(serde_json::to_string(&info)?)
					.stdout(Stdio::from(log_file.try_clone()?))
					.stderr(Stdio::from(log_file));
				if crate::store::get_settings().value.separatewine {
					command.env("WINEPREFIX", path.join("wineprefix").to_string_lossy().to_string());
				} else {
					let _ = fs::remove_dir_all(path.join("wineprefix"));
				}
				#[cfg(target_os = "linux")]
				attach_parent_death_signal(&mut command);
				Ok((plugin_uuid, PluginChildType::Wine, command))
			}),
		)
		.await?;
	} else {
		let info = info_param::make_info(plugin_uuid.to_owned(), manifest.version, false).await;
		let log_file = fs::File::create(log_dir().join("plugins").join(format!("{plugin_uuid}.log")))?;

		#[cfg(unix)]
		{
			use std::os::unix::fs::PermissionsExt;
			fs::set_permissions(path.join(&code_path), fs::Permissions::from_mode(0o755))?;
		}

		request_spawn(
			&spawner_tx,
			Box::new(move || {
				let mut command = Command::new(path.join(code_path));
				command
					.current_dir(path)
					.args(args)
					.arg(serde_json::to_string(&info)?)
					.stdout(Stdio::from(log_file.try_clone()?))
					.stderr(Stdio::from(log_file));
				#[cfg(target_os = "linux")]
				attach_parent_death_signal(&mut command);
				#[cfg(target_os = "windows")]
				{
					use std::os::windows::process::CommandExt;
					command.creation_flags(0x08000000);
				}
				Ok((plugin_uuid, PluginChildType::Native, command))
			}),
		)
		.await?;
	}

	if let Some(applications) = manifest.applications_to_monitor
		&& let Some(applications) = applications.get(platform)
	{
		crate::application_watcher::start_monitoring(&plugin_uuid_2, applications).await;
	}

	Ok(())
}

pub async fn deactivate_plugin(app: &AppHandle, uuid: &str) -> Result<(), anyhow::Error> {
	let deactivation = crate::events::begin_plugin_deactivation(uuid).await;
	let result = async {
		crate::events::disconnect_plugin(uuid).await;
		{
			let mut namespaces = DEVICE_NAMESPACES.write().await;
			if let Some((namespace, _)) = namespaces.clone().iter().find(|(_, plugin)| uuid == **plugin) {
				namespaces.remove(namespace);
				drop(namespaces);
				let devices = crate::shared::DEVICES.iter().map(|v| v.key().to_owned()).filter(|id| &id[..2] == namespace).collect::<Vec<_>>();
				for device in devices {
					crate::events::inbound::devices::deregister_device("", crate::events::inbound::PayloadEvent { payload: device }).await?;
				}
				crate::events::frontend::update_devices().await;
			}
		}

		crate::application_watcher::stop_monitoring(uuid).await;

		if let Some(instance) = INSTANCES.lock().await.remove(uuid) {
			match instance {
				PluginInstance::Webview => {
					if let Some(window) = app.get_webview_window(&uuid.replace('.', "_")) {
						window.close()?;
						tokio::time::sleep(std::time::Duration::from_millis(10)).await;
					}
				}
				PluginInstance::Node(child) | PluginInstance::Wine(child) | PluginInstance::Native(child) => {
					stop_child(child).await?;
				}
			}
			Ok(())
		} else {
			Err(anyhow!("instance of plugin {} not found", uuid))
		}
	}
	.await;
	deactivation.release().await;
	result
}

#[cfg(windows)]
pub async fn deactivate_plugins() {
	let uuids = {
		let instances = INSTANCES.lock().await;
		instances.keys().cloned().collect::<Vec<_>>()
	};

	let app = APP_HANDLE.get().unwrap();
	for uuid in uuids {
		let _ = deactivate_plugin(app, &uuid).await;
	}
}

/// Initialise plugins from the plugins directory.
pub fn initialise_plugins() {
	let plugin_dir = config_dir().join("plugins");
	let _ = fs::create_dir_all(&plugin_dir);
	let _ = fs::create_dir_all(log_dir().join("plugins"));

	tokio::spawn(init_websocket_server());
	tokio::spawn(webserver::init_webserver(plugin_dir.clone()));

	if let Ok(Ok(entries)) = APP_HANDLE.get().unwrap().path().resolve("plugins", tauri::path::BaseDirectory::Resource).map(fs::read_dir) {
		for entry in entries.flatten() {
			if let Err(error) = (|| -> Result<(), anyhow::Error> {
				let builtin_version = semver::Version::parse(&serde_json::from_slice::<manifest::PluginManifest>(&fs::read(entry.path().join("manifest.json"))?)?.version)?;
				let existing_path = plugin_dir.join(entry.file_name());
				if (|| -> Result<(), anyhow::Error> {
					let existing_version = semver::Version::parse(&serde_json::from_slice::<manifest::PluginManifest>(&fs::read(existing_path.join("manifest.json"))?)?.version)?;
					if existing_version < builtin_version {
						Err(anyhow::anyhow!("builtin version is newer than existing version"))
					} else {
						Ok(())
					}
				})()
				.is_err()
				{
					if existing_path.exists() {
						fs::rename(&existing_path, existing_path.with_extension("old"))?;
					}
					if crate::shared::copy_dir(entry.path(), &existing_path).is_err() && existing_path.with_extension("old").exists() {
						fs::rename(existing_path.with_extension("old"), &existing_path)?;
					}
					let _ = fs::remove_dir_all(existing_path.with_extension("old"));
				}
				Ok(())
			})() {
				error!("Failed to upgrade builtin plugin {}: {}", entry.file_name().to_string_lossy(), error);
			}
		}
	}

	let entries = match fs::read_dir(&plugin_dir) {
		Ok(p) => p,
		Err(error) => {
			error!("Failed to read plugins directory at {}: {}", plugin_dir.display(), error);
			panic!()
		}
	};

	let (tx, rx) = mpsc::channel::<SpawnRequest>();
	APP_HANDLE.get().unwrap().manage(tx.clone());

	// Use a dedicated spawner thread so that plugin processes don't die due to PR_SET_PDEATHSIG when the parent Tokio worker exits
	std::thread::spawn(|| {
		for request in rx {
			let result = (|| -> Result<(), anyhow::Error> {
				let (plugin_uuid, child_type, mut command) = (request.factory)()?;
				if INSTANCES.blocking_lock().contains_key(&plugin_uuid) {
					return Err(anyhow!("plugin {plugin_uuid} is already running"));
				}
				let mut child = command.spawn()?;
				let mut instances = INSTANCES.blocking_lock();
				if instances.contains_key(&plugin_uuid) {
					let _ = child.kill();
					let _ = child.wait();
					return Err(anyhow!("plugin {plugin_uuid} became active while its process was starting"));
				}
				instances.insert(
					plugin_uuid,
					match child_type {
						PluginChildType::Wine => PluginInstance::Wine(child),
						PluginChildType::Native => PluginInstance::Native(child),
						PluginChildType::Node => PluginInstance::Node(child),
					},
				);
				Ok(())
			})()
			.map_err(|error| format!("{error:#}"));
			if let Err(error) = &result {
				warn!("Failed to initialise plugin: {error}");
			}
			let _ = request.completion.send(result);
		}
	});

	// Iterate through all directory entries in the plugins folder and initialise them as plugins if appropriate
	for entry in entries {
		if let Ok(entry) = entry {
			let path = match entry.metadata().unwrap().is_symlink() {
				true => entry.path().parent().unwrap_or_else(|| path::Path::new(".")).join(fs::read_link(entry.path()).unwrap()),
				false => entry.path(),
			};
			let metadata = fs::metadata(&path).unwrap();
			if metadata.is_dir() {
				let spawner_tx = tx.clone();
				tokio::spawn(async move {
					let Some(plugin_uuid) = path.file_name().and_then(|name| name.to_str()).map(str::to_owned) else {
						return;
					};
					let _lifecycle = lock_plugin_lifecycle(&plugin_uuid).await;
					if let Err(error) = initialise_plugin(path.clone(), spawner_tx).await {
						warn!("Failed to initialise plugin at {}: {:#}", path.display(), error);
					}
				});
			}
		} else if let Err(error) = entry {
			warn!("Failed to read entry of plugins directory: {}", error)
		}
	}

	// On macOS, hidden WKWebView windows suspend JavaScript after ~7s.
	// Periodically eval a no-op to keep them alive.
	#[cfg(target_os = "macos")]
	tokio::spawn(async {
		use tauri::Manager;
		let app = APP_HANDLE.get().unwrap();
		loop {
			tokio::time::sleep(std::time::Duration::from_secs(3)).await;
			let instances = INSTANCES.lock().await;
			for (uuid, _) in instances.iter().filter(|(_, instance)| matches!(instance, PluginInstance::Webview)) {
				if let Some(window) = app.get_webview_window(&uuid.replace('.', "_")) {
					let _ = window.eval("void(0);");
				}
			}
		}
	});
}

/// Start the WebSocket server that plugins communicate with.
async fn init_websocket_server() {
	let listener = match TcpListener::bind(("127.0.0.1", *PORT_BASE)).await {
		Ok(listener) => listener,
		Err(error) => {
			error!("Failed to bind plugin WebSocket server to socket: {}", error);
			return;
		}
	};

	#[cfg(windows)]
	{
		use std::os::windows::io::AsRawSocket;
		use windows_sys::Win32::Foundation::{HANDLE_FLAG_INHERIT, SetHandleInformation};

		unsafe { SetHandleInformation(listener.as_raw_socket() as _, HANDLE_FLAG_INHERIT, 0) };
	}

	let registration_slots = Arc::new(Semaphore::new(MAX_PENDING_WEBSOCKET_REGISTRATIONS));

	while let Ok((stream, _)) = listener.accept().await {
		let Ok(registration_slot) = Arc::clone(&registration_slots).try_acquire_owned() else {
			warn!("Rejected WebSocket connection because the registration limit was reached");
			continue;
		};
		tokio::spawn(async move {
			let _registration_slot = registration_slot;
			match tokio::time::timeout(std::time::Duration::from_secs(10), read_registration(stream)).await {
				Ok(Some((event, socket))) => crate::events::register_plugin(event, socket).await,
				Ok(None) => {}
				Err(_) => warn!("Rejected WebSocket connection that did not register within 10 seconds"),
			}
		});
	}
}

/// Complete the WebSocket handshake and read the registration event without mutating session state.
#[allow(clippy::result_large_err)]
async fn read_registration(stream: TcpStream) -> Option<(crate::events::inbound::RegisterEvent, tokio_tungstenite::WebSocketStream<TcpStream>)> {
	let config = WebSocketConfig::default()
		.max_message_size(Some(MAX_WEBSOCKET_MESSAGE_BYTES))
		.max_frame_size(Some(MAX_WEBSOCKET_MESSAGE_BYTES))
		.max_write_buffer_size(MAX_WEBSOCKET_MESSAGE_BYTES * 2);
	let observed_origin = Arc::new(AtomicU8::new(0));
	let callback_origin = Arc::clone(&observed_origin);
	let mut socket = match tokio_tungstenite::accept_hdr_async_with_config(
		stream,
		move |request: &Request, response: Response| {
			let origin = request.headers().get(ORIGIN).and_then(|origin| origin.to_str().ok());
			if let Some(origin) = websocket_origin(origin, *PORT_BASE) {
				callback_origin.store(origin as u8, Ordering::Relaxed);
				Ok(response)
			} else {
				let mut response = ErrorResponse::new(Some("WebSocket origin is not allowed".to_owned()));
				*response.status_mut() = StatusCode::FORBIDDEN;
				Err(response)
			}
		},
		Some(config),
	)
	.await
	{
		Ok(socket) => socket,
		Err(error) => {
			warn!("Failed to complete WebSocket handshake: {}", error);
			return None;
		}
	};

	let Some(Ok(register_event)) = socket.next().await else {
		return None;
	};
	let Ok(register_event) = register_event.into_text() else {
		warn!("Rejected WebSocket connection without a text registration event");
		return None;
	};
	let event: crate::events::inbound::RegisterEvent = match serde_json::from_str(register_event.as_ref()) {
		Ok(event) => event,
		Err(error) => {
			warn!("Rejected WebSocket connection with invalid registration event: {error}");
			return None;
		}
	};
	let sandboxed_property_inspector = observed_origin.load(Ordering::Relaxed) == WebSocketOrigin::SandboxedPropertyInspector as u8;
	let origin_matches_registration = match &event {
		crate::events::inbound::RegisterEvent::RegisterPlugin { .. } => !sandboxed_property_inspector,
		crate::events::inbound::RegisterEvent::RegisterPropertyInspector { .. } => sandboxed_property_inspector,
	};
	if !origin_matches_registration {
		warn!("Rejected WebSocket registration from an origin that does not match its session type");
		return None;
	}
	Some((event, socket))
}

#[cfg(test)]
mod tests {
	use super::{ALTERNATIVE_ELGATO_PLUGIN_UUID, is_allowed_external_url, is_safe_plugin_uuid, websocket_origin_allowed};

	#[test]
	fn plugin_uuid_must_be_one_safe_plugin_directory_name() {
		assert!(is_safe_plugin_uuid("st.lynx.plugins.opendeck-akp03.sdPlugin"));
		assert!(!is_safe_plugin_uuid(ALTERNATIVE_ELGATO_PLUGIN_UUID));
		assert!(!is_safe_plugin_uuid("../escape.sdPlugin"));
		assert!(!is_safe_plugin_uuid("/tmp/escape.sdPlugin"));
		assert!(!is_safe_plugin_uuid("plugin"));
		assert!(!is_safe_plugin_uuid("bad name.sdPlugin"));
	}

	#[test]
	fn websocket_origin_is_native_or_exact_local_webserver() {
		assert!(websocket_origin_allowed(None, 57116));
		assert!(websocket_origin_allowed(Some("http://localhost:57118"), 57116));
		assert!(websocket_origin_allowed(Some("http://127.0.0.1:57118"), 57116));
		assert!(websocket_origin_allowed(Some("null"), 57116));
		assert!(!websocket_origin_allowed(Some("https://attacker.invalid"), 57116));
		assert!(!websocket_origin_allowed(Some("http://localhost.attacker.invalid:57118"), 57116));
	}

	#[test]
	fn external_url_scheme_allowlist_blocks_local_and_custom_handlers() {
		assert!(is_allowed_external_url("https://example.com/path"));
		assert!(is_allowed_external_url("mailto:security@example.com"));
		assert!(!is_allowed_external_url("file:///etc/passwd"));
		assert!(!is_allowed_external_url("opendeck://plugins/install/example"));
		assert!(!is_allowed_external_url("not a url"));
	}
}
