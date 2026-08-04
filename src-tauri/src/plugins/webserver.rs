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
		let path = match Path::new(requested_path).canonicalize() {
			Ok(path) => path,
			Err(_) => {
				let _ = request.respond(Response::empty(404));
				continue;
			}
		};

		// Plugin assets are always confined to the canonical plugin directory, including in developer mode.
		if !path.starts_with(&prefix) {
			let _ = request.respond(Response::empty(403));
			continue;
		}

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
	use super::{asset_origin_allowed, property_inspector_parent_origins};

	#[test]
	fn cors_allows_only_application_and_plugin_server_origins() {
		assert!(asset_origin_allowed("tauri://localhost", 57116));
		assert!(asset_origin_allowed("http://tauri.localhost", 57116));
		assert!(asset_origin_allowed("http://localhost:57118", 57116));
		assert!(!asset_origin_allowed("https://attacker.invalid", 57116));
		assert!(!asset_origin_allowed("http://localhost.attacker.invalid:57118", 57116));
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
