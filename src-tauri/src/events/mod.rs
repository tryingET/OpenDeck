pub mod frontend;
pub mod inbound;
pub mod outbound;

use inbound::RegisterEvent;

use std::collections::HashMap;
use std::str::FromStr;
use std::sync::LazyLock;

use futures::{SinkExt, StreamExt, stream::SplitSink};
use tokio::net::TcpStream;
use tokio::sync::{Mutex, RwLock};
use tokio_tungstenite::{WebSocketStream, tungstenite::Message};

type Sockets = LazyLock<Mutex<HashMap<String, SplitSink<WebSocketStream<TcpStream>, Message>>>>;
static PLUGIN_SOCKETS: Sockets = LazyLock::new(|| Mutex::new(HashMap::new()));
static PROPERTY_INSPECTOR_SOCKETS: Sockets = LazyLock::new(|| Mutex::new(HashMap::new()));
static PLUGIN_QUEUES: LazyLock<RwLock<HashMap<String, Vec<Message>>>> = LazyLock::new(|| RwLock::new(HashMap::new()));
static PROPERTY_INSPECTOR_QUEUES: LazyLock<RwLock<HashMap<String, Vec<Message>>>> = LazyLock::new(|| RwLock::new(HashMap::new()));

pub async fn registered_plugins() -> Vec<String> {
	PLUGIN_SOCKETS.lock().await.keys().map(|x| x.to_owned()).collect()
}

async fn property_inspector_context_is_live(uuid: &str) -> bool {
	let Ok(context) = crate::shared::ActionContext::from_str(uuid) else {
		return false;
	};
	let locks = crate::store::profiles::acquire_locks().await;
	matches!(crate::store::profiles::get_instance(&context, &locks).await, Ok(Some(_)))
}

/// Register a validated plugin or property inspector to send and receive events with its WebSocket.
pub async fn register_plugin(event: RegisterEvent, stream: WebSocketStream<TcpStream>) {
	match event {
		RegisterEvent::RegisterPlugin { uuid } => {
			if !crate::plugins::is_known_plugin_registration_uuid(&uuid) {
				log::warn!("Rejected WebSocket registration for unknown or unsafe plugin {uuid:?}");
				return;
			}

			let (sink, stream) = stream.split();
			{
				let mut sockets = PLUGIN_SOCKETS.lock().await;
				if sockets.contains_key(&uuid) {
					log::warn!("Rejected duplicate WebSocket registration for plugin {uuid}");
					return;
				}
				sockets.insert(uuid.clone(), sink);
			}
			if let Some(queue) = PLUGIN_QUEUES.read().await.get(&uuid) {
				let mut sockets = PLUGIN_SOCKETS.lock().await;
				if let Some(sink) = sockets.get_mut(&uuid) {
					for message in queue {
						let _ = sink.feed(message.clone()).await;
					}
					let _ = sink.flush().await;
				}
			}
			log::debug!("Registered plugin {uuid}");
			tokio::spawn(async move {
				stream.for_each(|event| inbound::process_incoming_message(event, &uuid, false)).await;
				PLUGIN_SOCKETS.lock().await.remove(&uuid);
			});
		}
		RegisterEvent::RegisterPropertyInspector { uuid } => {
			if !property_inspector_context_is_live(&uuid).await {
				log::warn!("Rejected WebSocket registration for unknown property inspector context {uuid:?}");
				return;
			}

			let (sink, stream) = stream.split();
			{
				let mut sockets = PROPERTY_INSPECTOR_SOCKETS.lock().await;
				if sockets.contains_key(&uuid) {
					log::warn!("Rejected duplicate WebSocket registration for property inspector {uuid}");
					return;
				}
				sockets.insert(uuid.clone(), sink);
			}
			if let Some(queue) = PROPERTY_INSPECTOR_QUEUES.read().await.get(&uuid) {
				let mut sockets = PROPERTY_INSPECTOR_SOCKETS.lock().await;
				if let Some(sink) = sockets.get_mut(&uuid) {
					for message in queue {
						let _ = sink.feed(message.clone()).await;
					}
					let _ = sink.flush().await;
				}
			}
			tokio::spawn(async move {
				stream.for_each(|event| inbound::process_incoming_message_pi(event, &uuid)).await;
				PROPERTY_INSPECTOR_SOCKETS.lock().await.remove(&uuid);
			});
		}
	};
}
