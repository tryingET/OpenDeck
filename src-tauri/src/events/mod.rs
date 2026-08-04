pub mod frontend;
pub mod inbound;
pub mod outbound;
mod property_inspector_auth;
mod session_lifecycle;

use inbound::RegisterEvent;
pub(crate) use property_inspector_auth::issue_property_inspector_registration;
pub use session_lifecycle::begin_plugin_deactivation;
use session_lifecycle::{DEACTIVATING_PLUGINS, plugin_is_deactivating};

use std::collections::{HashMap, VecDeque};
use std::str::FromStr;
use std::sync::{
	LazyLock,
	atomic::{AtomicU64, Ordering},
};
use std::time::Duration;

use futures::{
	SinkExt, StreamExt,
	future::{AbortHandle, AbortRegistration, Abortable},
	stream::{SplitSink, SplitStream},
};
use tokio::net::TcpStream;
use tokio::sync::{Mutex, mpsc, oneshot, watch};
use tokio_tungstenite::{WebSocketStream, tungstenite::Message};

const MAX_MESSAGE_BYTES: usize = 1024 * 1024;
const MAX_PENDING_MESSAGES: usize = 64;
const MAX_PENDING_BYTES: usize = 4 * 1024 * 1024;
const MAX_PENDING_IDENTITIES: usize = 256;
const MAX_ACTIVE_SOCKETS: usize = 256;
const OUTBOUND_CHANNEL_CAPACITY: usize = 64;
const SOCKET_STOP_TIMEOUT: Duration = Duration::from_secs(2);

type SocketSink = SplitSink<WebSocketStream<TcpStream>, Message>;
type SocketStream = SplitStream<WebSocketStream<TcpStream>>;

#[derive(Default)]
struct PendingQueue {
	owner_plugin: Option<String>,
	messages: VecDeque<Message>,
	bytes: usize,
}

impl PendingQueue {
	fn bind_owner(&mut self, owner_plugin: &str) {
		if self.owner_plugin.as_deref() != Some(owner_plugin) {
			self.owner_plugin = Some(owner_plugin.to_owned());
			self.messages.clear();
			self.bytes = 0;
		}
	}

	fn push(&mut self, message: Message) -> bool {
		self.push_with_limits(message, MAX_PENDING_MESSAGES, MAX_PENDING_BYTES)
	}

	fn push_with_limits(&mut self, message: Message, max_messages: usize, max_bytes: usize) -> bool {
		let message_bytes = message.len();
		if max_messages == 0 || message_bytes > max_bytes {
			return false;
		}

		while !self.messages.is_empty() && (self.messages.len() >= max_messages || self.bytes.saturating_add(message_bytes) > max_bytes) {
			if let Some(dropped) = self.messages.pop_front() {
				self.bytes = self.bytes.saturating_sub(dropped.len());
			}
		}

		self.bytes += message_bytes;
		self.messages.push_back(message);
		true
	}
}

struct SocketEntry {
	generation: u64,
	owner_plugin: String,
	outbound: mpsc::Sender<Message>,
	cancel: watch::Sender<bool>,
	reader_done: oneshot::Receiver<()>,
	writer_done: oneshot::Receiver<()>,
	reader_abort: AbortHandle,
	writer_abort: AbortHandle,
}

struct SocketTaskChannels {
	outbound: mpsc::Receiver<Message>,
	reader_cancel: watch::Receiver<bool>,
	writer_cancel: watch::Receiver<bool>,
	reader_done: oneshot::Sender<()>,
	writer_done: oneshot::Sender<()>,
	reader_abort: AbortRegistration,
	writer_abort: AbortRegistration,
}

struct SocketTaskSpec {
	registry: &'static Registry,
	uuid: String,
	generation: u64,
	sink: SocketSink,
	stream: SocketStream,
	initial: VecDeque<Message>,
	channels: SocketTaskChannels,
	inspector_owner: Option<String>,
}

#[derive(Default)]
struct SocketRegistry {
	sockets: HashMap<String, SocketEntry>,
	pending: HashMap<String, PendingQueue>,
}

type Registry = LazyLock<Mutex<SocketRegistry>>;
static NEXT_SOCKET_GENERATION: AtomicU64 = AtomicU64::new(1);
static PLUGINS: Registry = LazyLock::new(|| Mutex::new(SocketRegistry::default()));
static PROPERTY_INSPECTORS: Registry = LazyLock::new(|| Mutex::new(SocketRegistry::default()));

fn new_socket_session(generation: u64, owner_plugin: String) -> (SocketEntry, SocketTaskChannels) {
	let (outbound_tx, outbound_rx) = mpsc::channel(OUTBOUND_CHANNEL_CAPACITY);
	let (cancel_tx, cancel_rx) = watch::channel(false);
	let (reader_done_tx, reader_done_rx) = oneshot::channel();
	let (writer_done_tx, writer_done_rx) = oneshot::channel();
	let (reader_abort_handle, reader_abort_registration) = AbortHandle::new_pair();
	let (writer_abort_handle, writer_abort_registration) = AbortHandle::new_pair();

	(
		SocketEntry {
			generation,
			owner_plugin,
			outbound: outbound_tx,
			cancel: cancel_tx,
			reader_done: reader_done_rx,
			writer_done: writer_done_rx,
			reader_abort: reader_abort_handle,
			writer_abort: writer_abort_handle,
		},
		SocketTaskChannels {
			outbound: outbound_rx,
			reader_cancel: cancel_rx.clone(),
			writer_cancel: cancel_rx,
			reader_done: reader_done_tx,
			writer_done: writer_done_tx,
			reader_abort: reader_abort_registration,
			writer_abort: writer_abort_registration,
		},
	)
}

pub async fn registered_plugins() -> Vec<String> {
	PLUGINS.lock().await.sockets.keys().cloned().collect()
}

async fn stop_socket(entry: SocketEntry) {
	let SocketEntry {
		owner_plugin,
		outbound,
		cancel,
		reader_done,
		writer_done,
		reader_abort,
		writer_abort,
		..
	} = entry;
	let _ = cancel.send(true);
	drop(outbound);
	drop(cancel);

	if tokio::time::timeout(SOCKET_STOP_TIMEOUT, async {
		let _ = tokio::join!(reader_done, writer_done);
	})
	.await
	.is_err()
	{
		log::warn!("Aborting unresponsive WebSocket tasks owned by plugin {owner_plugin}");
		reader_abort.abort();
		writer_abort.abort();
	}
}

pub async fn disconnect_plugin(uuid: &str) {
	let mut disconnected = Vec::new();
	if let Some(entry) = PLUGINS.lock().await.sockets.remove(uuid) {
		disconnected.push(entry);
	}

	{
		let mut inspectors = PROPERTY_INSPECTORS.lock().await;
		let contexts = inspectors
			.sockets
			.iter()
			.filter(|(_, entry)| entry.owner_plugin == uuid)
			.map(|(context, _)| context.clone())
			.collect::<Vec<_>>();
		for context in contexts {
			if let Some(entry) = inspectors.sockets.remove(&context) {
				disconnected.push(entry);
			}
		}
	}

	futures::future::join_all(disconnected.into_iter().map(stop_socket)).await;
}

async fn remove_socket_if_current(registry: &'static Registry, uuid: &str, generation: u64) {
	let removed = {
		let mut registry = registry.lock().await;
		if registry.sockets.get(uuid).is_some_and(|entry| entry.generation == generation) {
			registry.sockets.remove(uuid)
		} else {
			None
		}
	};
	if let Some(entry) = removed {
		let _ = entry.cancel.send(true);
	}
}

async fn lookup_property_inspector_owner(uuid: &str) -> Option<String> {
	let Ok(context) = crate::shared::ActionContext::from_str(uuid) else {
		return None;
	};
	let locks = crate::store::profiles::acquire_locks().await;
	match crate::store::profiles::get_instance(&context, &locks).await {
		Ok(Some(instance)) => Some(instance.action.plugin.clone()),
		_ => None,
	}
}

fn spawn_socket_tasks(spec: SocketTaskSpec) {
	let SocketTaskSpec {
		registry,
		uuid,
		generation,
		mut sink,
		mut stream,
		mut initial,
		channels,
		inspector_owner,
	} = spec;
	let SocketTaskChannels {
		mut outbound,
		mut reader_cancel,
		mut writer_cancel,
		reader_done,
		writer_done,
		reader_abort,
		writer_abort,
	} = channels;

	let writer_uuid = uuid.clone();
	tokio::spawn(Abortable::new(
		async move {
			loop {
				if *writer_cancel.borrow() {
					break;
				}
				let message = match initial.pop_front() {
					Some(message) => Some(message),
					None => {
						tokio::select! {
							biased;
							_ = writer_cancel.changed() => None,
							message = outbound.recv() => message,
						}
					}
				};
				let Some(message) = message else {
					break;
				};

				let sent = tokio::select! {
					biased;
					_ = writer_cancel.changed() => false,
					result = sink.send(message) => result.is_ok(),
				};
				if !sent {
					break;
				}
			}

			let _ = writer_done.send(());
			remove_socket_if_current(registry, &writer_uuid, generation).await;
		},
		writer_abort,
	));

	tokio::spawn(Abortable::new(
		async move {
			loop {
				let event = tokio::select! {
					biased;
					_ = reader_cancel.changed() => None,
					event = stream.next() => event,
				};
				let Some(event) = event else {
					break;
				};
				if *reader_cancel.borrow() {
					break;
				}

				let owner_plugin = inspector_owner.as_deref().unwrap_or(&uuid);
				if plugin_is_deactivating(owner_plugin).await {
					break;
				}
				if let Some(owner_plugin) = &inspector_owner {
					if lookup_property_inspector_owner(&uuid).await.as_deref() != Some(owner_plugin.as_str()) {
						break;
					}
					inbound::process_incoming_message_pi(event, &uuid, owner_plugin).await;
				} else {
					inbound::process_incoming_message(event, &uuid, false).await;
				}
			}

			let _ = reader_done.send(());
			remove_socket_if_current(registry, &uuid, generation).await;
		},
		reader_abort,
	));
}

async fn send_message(registry: &'static Registry, uuid: &str, owner_plugin: &str, message: Message) -> Result<(), anyhow::Error> {
	use tokio::sync::mpsc::error::TrySendError;

	if message.len() > MAX_MESSAGE_BYTES {
		return Err(anyhow::anyhow!("WebSocket message for {uuid} exceeds the size limit"));
	}

	let mut registry = registry.lock().await;
	let owner_matches = registry.sockets.get(uuid).map(|entry| entry.owner_plugin == owner_plugin);
	let message = if owner_matches == Some(true) {
		match registry.sockets.get(uuid).unwrap().outbound.try_send(message) {
			Ok(()) => return Ok(()),
			Err(TrySendError::Full(_)) => return Err(anyhow::anyhow!("outbound WebSocket queue for {uuid} is full")),
			Err(TrySendError::Closed(message)) => message,
		}
	} else {
		message
	};
	if owner_matches.is_some()
		&& let Some(entry) = registry.sockets.remove(uuid)
	{
		let _ = entry.cancel.send(true);
	}

	if !registry.pending.contains_key(uuid) && registry.pending.len() >= MAX_PENDING_IDENTITIES {
		return Err(anyhow::anyhow!("pending WebSocket identity limit reached"));
	}

	let queue = registry.pending.entry(uuid.to_owned()).or_default();
	queue.bind_owner(owner_plugin);
	if !queue.push(message) {
		return Err(anyhow::anyhow!("pending WebSocket message for {uuid} exceeds the queue limit"));
	}
	Ok(())
}

pub(crate) async fn send_plugin_message(uuid: &str, message: Message) -> Result<(), anyhow::Error> {
	send_message(&PLUGINS, uuid, uuid, message).await
}

pub(crate) async fn send_property_inspector_message(uuid: &str, owner_plugin: &str, message: Message) -> Result<(), anyhow::Error> {
	send_message(&PROPERTY_INSPECTORS, uuid, owner_plugin, message).await
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
			let generation = NEXT_SOCKET_GENERATION.fetch_add(1, Ordering::Relaxed);
			let (entry, channels) = new_socket_session(generation, uuid.clone());
			let initial = {
				let deactivating = DEACTIVATING_PLUGINS.lock().await;
				if deactivating.contains_key(&uuid) {
					log::warn!("Rejected WebSocket registration for deactivating plugin {uuid}");
					return;
				}
				let mut registry = PLUGINS.lock().await;
				if registry.sockets.contains_key(&uuid) {
					log::warn!("Rejected duplicate WebSocket registration for plugin {uuid}");
					return;
				}
				if registry.sockets.len() >= MAX_ACTIVE_SOCKETS {
					log::warn!("Rejected plugin registration because the active socket limit was reached");
					return;
				}
				let initial = registry
					.pending
					.remove(&uuid)
					.filter(|queue| queue.owner_plugin.as_deref() == Some(uuid.as_str()))
					.unwrap_or_default()
					.messages;
				registry.sockets.insert(uuid.clone(), entry);
				drop(registry);
				drop(deactivating);
				initial
			};

			spawn_socket_tasks(SocketTaskSpec {
				registry: &PLUGINS,
				uuid: uuid.clone(),
				generation,
				sink,
				stream,
				initial,
				channels,
				inspector_owner: None,
			});
			log::debug!("Registered plugin {uuid}");
		}
		RegisterEvent::RegisterPropertyInspector { uuid, token } => {
			let Some(owner_plugin) = property_inspector_auth::consume(&uuid, &token).await else {
				log::warn!("Rejected WebSocket registration without a valid property inspector capability");
				return;
			};
			let Ok(context) = crate::shared::ActionContext::from_str(&uuid) else {
				return;
			};
			let profile_locks = crate::store::profiles::acquire_locks().await;
			let current_owner = match crate::store::profiles::get_instance(&context, &profile_locks).await {
				Ok(Some(instance)) => instance.action.plugin.as_str(),
				_ => return,
			};
			if current_owner != owner_plugin {
				log::warn!("Rejected stale property inspector registration");
				return;
			}

			let (sink, stream) = stream.split();
			let generation = NEXT_SOCKET_GENERATION.fetch_add(1, Ordering::Relaxed);
			let (entry, channels) = new_socket_session(generation, owner_plugin.clone());
			let (initial, replaced) = {
				let deactivating = DEACTIVATING_PLUGINS.lock().await;
				if deactivating.contains_key(&owner_plugin) {
					log::warn!("Rejected property inspector registration for deactivating plugin {owner_plugin}");
					return;
				}
				let mut registry = PROPERTY_INSPECTORS.lock().await;
				if !registry.sockets.contains_key(&uuid) && registry.sockets.len() >= MAX_ACTIVE_SOCKETS {
					log::warn!("Rejected property inspector registration because the active socket limit was reached");
					return;
				}
				let initial = registry
					.pending
					.remove(&uuid)
					.filter(|queue| queue.owner_plugin.as_deref() == Some(owner_plugin.as_str()))
					.unwrap_or_default()
					.messages;
				let replaced = registry.sockets.insert(uuid.clone(), entry);
				drop(registry);
				drop(deactivating);
				(initial, replaced)
			};
			drop(profile_locks);
			if let Some(replaced) = replaced {
				stop_socket(replaced).await;
			}

			spawn_socket_tasks(SocketTaskSpec {
				registry: &PROPERTY_INSPECTORS,
				uuid,
				generation,
				sink,
				stream,
				initial,
				channels,
				inspector_owner: Some(owner_plugin),
			});
		}
	};
}

#[cfg(test)]
mod tests;
