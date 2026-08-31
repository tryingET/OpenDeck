use std::path::{Path, PathBuf};

use tiny_http::{Header, Response, Server};

fn mime(extension: &str) -> String {
	match extension {
		"htm" | "html" | "xhtml" => "text/html".to_owned(),
		"js" | "cjs" | "mjs" => "text/javascript".to_owned(),
		"css" => "text/css".to_owned(),
		"png" | "jpeg" | "gif" | "webp" => format!("image/{}", extension),
		"jpg" => "image/jpeg".to_owned(),
		"svg" => "image/svg+xml".to_owned(),
		_ => "application/octet-stream".to_owned(),
	}
}

fn asset_origin_allowed(origin: &str, port_base: u16) -> bool {
	let webserver_port = port_base + 2;
	matches!(origin, "tauri://localhost" | "http://tauri.localhost" | "https://tauri.localhost")
		|| origin == format!("http://localhost:{webserver_port}")
		|| origin == format!("http://127.0.0.1:{webserver_port}")
		|| cfg!(debug_assertions) && origin == "http://localhost:5173"
}

fn property_inspector_parent_origins(include_development: bool) -> String {
	let mut origins = vec!["tauri://localhost", "http://tauri.localhost", "https://tauri.localhost"];
	if include_development {
		origins.push("http://localhost:5173");
	}
	serde_json::to_string(&origins).expect("static property inspector origins must serialize")
}

#[derive(Debug, PartialEq, Eq)]
enum AssetPathError {
	NotFound,
	OutsideRoot,
}

fn confine_asset_path(prefix: &Path, path: PathBuf) -> Result<PathBuf, AssetPathError> {
	if !path.starts_with(prefix) {
		return Err(AssetPathError::OutsideRoot);
	}
	Ok(path)
}

fn resolve_asset_path(prefix: &Path, request_path: &str) -> Result<PathBuf, AssetPathError> {
	let requested_path = Path::new(request_path);
	if requested_path.is_absolute() {
		if let Ok(path) = requested_path.canonicalize() {
			return confine_asset_path(prefix, path);
		}
	}

	let path = prefix.join(request_path.trim_start_matches(['/', '\\'])).canonicalize().map_err(|_| AssetPathError::NotFound)?;
	confine_asset_path(prefix, path)
}

/// Start a simple webserver to serve files of plugins that run in a browser environment.
pub async fn init_webserver(prefix: PathBuf) {
	let Ok(prefix) = prefix.canonicalize() else {
		log::error!("Failed to resolve plugin asset root at {}", prefix.display());
		return;
	};
	let server = {
		let listener = std::net::TcpListener::bind(("127.0.0.1", *super::PORT_BASE + 2)).unwrap();

		#[cfg(windows)]
		{
			use std::os::windows::io::AsRawSocket;
			use windows_sys::Win32::Foundation::{HANDLE_FLAG_INHERIT, SetHandleInformation};

			unsafe { SetHandleInformation(listener.as_raw_socket() as _, HANDLE_FLAG_INHERIT, 0) };
		}

		Server::from_listener(listener, None).unwrap()
	};

	for request in server.incoming_requests() {
		let Ok(mut url) = urlencoding::decode(request.url()).map(|url| url.into_owned()) else {
			let _ = request.respond(Response::empty(400));
			continue;
		};
		if let Some((path, _)) = url.split_once('?') {
			url = path.to_owned();
		}
		#[cfg(target_os = "windows")]
		let url = url.strip_prefix('/').unwrap_or(&url).replace('/', "\\");

		let is_property_inspector = url.ends_with("|opendeck_property_inspector");
		let is_property_inspector_child = url.ends_with("|opendeck_property_inspector_child");
		let requested_path = url.trim_end_matches("|opendeck_property_inspector").trim_end_matches("|opendeck_property_inspector_child");
		// Plugin assets are always confined to the canonical plugin directory, including in developer mode.
		let path = match resolve_asset_path(&prefix, requested_path) {
			Ok(path) => path,
			Err(AssetPathError::NotFound) => {
				let _ = request.respond(Response::empty(404));
				continue;
			}
			Err(AssetPathError::OutsideRoot) => {
				let _ = request.respond(Response::empty(403));
				continue;
			}
		};

		let cors_origin = request.headers().iter().find(|header| header.field.equiv("Origin")).and_then(|header| {
			let origin = header.value.to_string();
			if !asset_origin_allowed(&origin, *super::PORT_BASE) {
				return None;
			}
			Some(Header {
				field: "Access-Control-Allow-Origin".parse().ok()?,
				value: origin.parse().ok()?,
			})
		});

		// The Svelte frontend cannot call the connectElgatoStreamDeckSocket function on property inspector frames
		// because they are served from a different origin (this webserver on port 57118).
		// Instead, we have to inject a script onto all property inspector frames that receives a message
		// from the Svelte frontend over window.postMessage.

		// Additionally, Tauri cannot support window.open as seperate Tauri windows have seperate JavaScript contexts.
		// However, plugin property inspectors expect access to this function.
		// Instead, we have to inject a replacement window.open implementation that creates an IFrame element
		// and requests the Svelte frontend to maximise the property inspector.

		if is_property_inspector {
			let mut content = tokio::fs::read_to_string(&path).await.unwrap_or_default();
			let parent_origins = property_inspector_parent_origins(cfg!(debug_assertions));
			content += r#"
				<div id="opendeck_iframe_container" style="position: absolute; z-index: 100; top: 0; left: 0; width: 100%; height: 100%; display: none;"></div>
				<script>
					const opendeck_window_open = window.open;
					const opendeck_iframe_container = document.getElementById("opendeck_iframe_container");
					const opendeck_parent_origins = new Set("#;
			content += &parent_origins;
			content += r#");

					const opendeck_native_websocket = window.WebSocket;
					const opendeck_socket_registrations = new WeakMap();
					const opendeck_native_websocket_send = opendeck_native_websocket.prototype.send;
					let opendeck_next_registration = null;
					window.WebSocket = new Proxy(opendeck_native_websocket, {
						construct(target, args, newTarget) {
							const socket = Reflect.construct(target, args, newTarget);
							if (opendeck_next_registration) {
								opendeck_socket_registrations.set(socket, opendeck_next_registration);
								opendeck_next_registration = null;
							}
							return socket;
						}
					});
					opendeck_native_websocket.prototype.send = function(message) {
						const registration = opendeck_socket_registrations.get(this);
						if (registration && typeof message === "string") {
							try {
								const event = JSON.parse(message);
								if (event.event === "registerPropertyInspector" && event.uuid === registration.uuid) {
									event.token = registration.token;
									message = JSON.stringify(event);
									opendeck_socket_registrations.delete(this);
								}
							} catch (_) {}
						}
						return opendeck_native_websocket_send.call(this, message);
					};

					window.addEventListener("message", (event) => {
						if (event.source !== window.parent || !opendeck_parent_origins.has(event.origin)) return;
						const data = event.data;
						if (!data || typeof data !== "object") return;
						if (data.event == "connect" && Array.isArray(data.payload) && data.payload.length === 6) {
							const [port, uuid, registrationEvent, info, actionInfo, token] = data.payload;
							if (!Number.isInteger(port) || typeof uuid !== "string" || registrationEvent !== "registerPropertyInspector"
								|| typeof info !== "string" || typeof actionInfo !== "string" || !/^[0-9a-f]{64}$/.test(token)) return;
							event.stopImmediatePropagation();
							const registration = { uuid, token };
							opendeck_next_registration = registration;
							try {
								if (typeof connectOpenActionSocket === "function") connectOpenActionSocket(port, uuid, registrationEvent, info, actionInfo);
								else if (typeof connectElgatoStreamDeckSocket === "function") connectElgatoStreamDeckSocket(port, uuid, registrationEvent, info, actionInfo);
							} finally {
								if (opendeck_next_registration === registration) opendeck_next_registration = null;
							}
						} else if (data.event == "windowClosed") {
							event.stopImmediatePropagation();
							if (opendeck_iframe_container.firstElementChild) opendeck_iframe_container.firstElementChild.remove();
							opendeck_iframe_container.style.display = "none";
						}
					});

					window.open = (url, target) => {
						if (target && !(target == "_self" || target == "_top")) {
							top.postMessage({ event: "openUrl", payload: url.startsWith("http") ? url : new URL(url, window.location.href).href }, "*");
							return;
						}
						let iframe = document.createElement("iframe");
						iframe.style.flexGrow = "1";
						iframe.onload = () => {
							iframe.contentWindow.opener = window;
							iframe.contentWindow.onbeforeunload = () => top.postMessage({ event: "windowClosed", payload: window.name }, "*");
							iframe.contentWindow.close = () => { iframe.contentWindow.onbeforeunload(); iframe.remove(); };
							iframe.contentWindow.document.body.style.overflowY = "auto";
						};
						iframe.src = url.startsWith("http") ? url : url + "|opendeck_property_inspector_child";
						if (opendeck_iframe_container.firstElementChild) opendeck_iframe_container.firstElementChild.remove();
						opendeck_iframe_container.appendChild(iframe);
						opendeck_iframe_container.style.display = "flex";
						top.postMessage({ event: "windowOpened", payload: window.name }, "*");
						return iframe.contentWindow;
					};

					const opendeck_window_fetch = window.fetch;
					let opendeck_fetch_count = 0;
					let opendeck_fetch_promises = {};
					window.addEventListener("message", (event) => {
						if (event.source !== window.parent || !opendeck_parent_origins.has(event.origin)) return;
						const data = event.data;
						if (!data || typeof data !== "object") return;
						if (data.event == "fetchResponse") {
							const pending = data.payload && opendeck_fetch_promises[data.payload.id];
							if (!pending) return;
							event.stopImmediatePropagation();
							const response = new Response(data.payload.response.body, data.payload.response);
							Object.defineProperty(response, "url", { value: data.payload.response.url });
							pending.resolve(response);
							delete opendeck_fetch_promises[data.payload.id];
						} else if (data.event == "fetchError") {
							const pending = data.payload && opendeck_fetch_promises[data.payload.id];
							if (!pending) return;
							event.stopImmediatePropagation();
							pending.reject(data.payload.error);
							delete opendeck_fetch_promises[data.payload.id];
						}
					});
					window.fetch = (...args) => {
						if (args.length) args[0] = new URL(args[0], window.location.href).href;
						top.postMessage({ event: "fetch", payload: { args, context: window.name, id: ++opendeck_fetch_count }}, "*");
						return new Promise((resolve, reject) => { opendeck_fetch_promises[opendeck_fetch_count] = { resolve, reject }; });
					};
				</script>
			"#;

			let mut response = Response::from_string(content);
			if let Some(cors_origin) = cors_origin.clone() {
				response.add_header(cors_origin);
			}
			response.add_header(Header {
				field: "Content-Type".parse().unwrap(),
				value: "text/html".parse().unwrap(),
			});
			let _ = request.respond(response);
		} else if is_property_inspector_child {
			let mut content = tokio::fs::read_to_string(&path).await.unwrap_or_default();
			content = format!("<script>window.opener ??= window.parent;</script>{content}");

			let mut response = Response::from_string(content);
			if let Some(cors_origin) = cors_origin.clone() {
				response.add_header(cors_origin);
			}
			response.add_header(Header {
				field: "Content-Type".parse().unwrap(),
				value: "text/html".parse().unwrap(),
			});
			let _ = request.respond(response);
		} else {
			let mime_type = mime(&match path.extension() {
				Some(extension) => extension.to_string_lossy().into_owned(),
				None => "html".to_owned(),
			});

			let content_type = Header {
				field: "Content-Type".parse().unwrap(),
				value: mime_type.parse().unwrap(),
			};

			if mime_type.starts_with("text/") || mime_type == "image/svg+xml" {
				let mut response = Response::from_string(tokio::fs::read_to_string(&path).await.unwrap_or_default());
				if let Some(cors_origin) = cors_origin.clone() {
					response.add_header(cors_origin);
				}
				response.add_header(content_type);
				let _ = request.respond(response);
			} else {
				let mut response = Response::from_file(match tokio::fs::File::open(&path).await {
					Ok(file) => file.into_std().await,
					Err(_) => continue,
				});
				if let Some(cors_origin) = cors_origin {
					response.add_header(cors_origin);
				}
				response.add_header(content_type);
				let _ = request.respond(response);
			}
		}
	}
}

#[cfg(test)]
mod tests {
	use std::path::{Path, PathBuf};

	use super::{AssetPathError, asset_origin_allowed, property_inspector_parent_origins, resolve_asset_path};

	fn plugin_root() -> PathBuf {
		Path::new(env!("CARGO_MANIFEST_DIR")).canonicalize().unwrap()
	}

	#[cfg(unix)]
	struct Scratch(PathBuf);

	#[cfg(unix)]
	impl Drop for Scratch {
		fn drop(&mut self) {
			let _ = std::fs::remove_dir_all(&self.0);
		}
	}

	#[cfg(unix)]
	fn scratch() -> Scratch {
		use std::time::{SystemTime, UNIX_EPOCH};

		let nonce = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
		Scratch(std::env::temp_dir().join(format!("opendeck-webserver-test-{}-{nonce}", std::process::id())))
	}

	#[test]
	fn cors_allows_only_application_and_plugin_server_origins() {
		assert!(asset_origin_allowed("tauri://localhost", 57116));
		assert!(asset_origin_allowed("http://tauri.localhost", 57116));
		assert!(asset_origin_allowed("http://localhost:57118", 57116));
		assert!(!asset_origin_allowed("https://attacker.invalid", 57116));
		assert!(!asset_origin_allowed("http://localhost.attacker.invalid:57118", 57116));
	}

	#[test]
	fn absolute_looking_asset_urls_resolve_beneath_plugin_root() {
		let prefix = plugin_root();
		let expected = prefix.join("src/plugins/webserver.rs").canonicalize().unwrap();
		assert_eq!(resolve_asset_path(&prefix, "/src/plugins/webserver.rs"), Ok(expected));
	}

	#[test]
	fn absolute_filesystem_asset_paths_within_plugin_root_are_preserved() {
		let prefix = plugin_root();
		let expected = prefix.join("src/plugins/webserver.rs").canonicalize().unwrap();
		assert_eq!(resolve_asset_path(&prefix, expected.to_str().unwrap()), Ok(expected));
	}

	#[cfg(unix)]
	#[test]
	fn renderer_prefixed_absolute_filesystem_paths_are_preserved() {
		let prefix = plugin_root();
		let expected = prefix.join("src/plugins/webserver.rs").canonicalize().unwrap();
		let request_path = format!("/{}", expected.display());
		assert_eq!(resolve_asset_path(&prefix, &request_path), Ok(expected));
	}

	#[test]
	fn traversal_paths_are_rejected_after_url_decoding() {
		let prefix = plugin_root();
		let decoded = urlencoding::decode("/%2e%2e/package.json").unwrap();
		assert_eq!(resolve_asset_path(&prefix, &decoded), Err(AssetPathError::OutsideRoot));
	}

	#[cfg(unix)]
	#[test]
	fn existing_absolute_paths_outside_root_cannot_use_relative_fallback() {
		use std::fs;

		let scratch = scratch();
		let root = scratch.0.join("root");
		let outside = scratch.0.join("outside/icon.svg");
		fs::create_dir_all(&root).unwrap();
		fs::create_dir_all(outside.parent().unwrap()).unwrap();
		fs::write(&outside, b"outside").unwrap();

		let fallback = root.join(outside.to_string_lossy().trim_start_matches('/'));
		fs::create_dir_all(fallback.parent().unwrap()).unwrap();
		fs::write(&fallback, b"fallback").unwrap();

		let prefix = root.canonicalize().unwrap();
		let request_path = outside.canonicalize().unwrap();
		assert_eq!(resolve_asset_path(&prefix, request_path.to_str().unwrap()), Err(AssetPathError::OutsideRoot));
	}

	#[cfg(unix)]
	#[test]
	fn symlinks_cannot_escape_plugin_root() {
		use std::{fs, os::unix::fs::symlink};

		let scratch = scratch();
		fs::create_dir_all(scratch.0.join("root")).unwrap();
		fs::create_dir_all(scratch.0.join("outside")).unwrap();
		fs::write(scratch.0.join("outside/secret"), b"secret").unwrap();
		symlink(scratch.0.join("outside"), scratch.0.join("root/escape")).unwrap();

		let prefix = scratch.0.join("root").canonicalize().unwrap();
		assert_eq!(resolve_asset_path(&prefix, "/escape/secret"), Err(AssetPathError::OutsideRoot));
	}

	#[cfg(windows)]
	#[test]
	fn windows_asset_urls_resolve_beneath_plugin_root() {
		let prefix = plugin_root();
		let expected = prefix.join("src\\plugins\\webserver.rs").canonicalize().unwrap();
		assert_eq!(resolve_asset_path(&prefix, "\\src\\plugins\\webserver.rs"), Ok(expected));
	}

	#[test]
	fn release_parent_origins_exclude_development_server() {
		let release = property_inspector_parent_origins(false);
		let debug = property_inspector_parent_origins(true);
		assert!(!release.contains("http://localhost:5173"));
		assert!(debug.contains("http://localhost:5173"));
		for origin in ["tauri://localhost", "http://tauri.localhost", "https://tauri.localhost"] {
			assert!(release.contains(origin));
		}
	}
}
