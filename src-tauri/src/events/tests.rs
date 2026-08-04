use super::PendingQueue;
use tokio_tungstenite::tungstenite::Message;

fn text(value: &str) -> Message {
	Message::Text(value.to_owned().into())
}

#[test]
fn pending_queue_evicts_oldest_message_at_count_limit() {
	let mut queue = PendingQueue::default();
	assert!(queue.push_with_limits(text("one"), 2, 100));
	assert!(queue.push_with_limits(text("two"), 2, 100));
	assert!(queue.push_with_limits(text("three"), 2, 100));
	assert_eq!(queue.messages.pop_front().unwrap().into_text().unwrap(), "two");
	assert_eq!(queue.messages.pop_front().unwrap().into_text().unwrap(), "three");
}

#[test]
fn pending_queue_evicts_oldest_message_at_byte_limit() {
	let mut queue = PendingQueue::default();
	assert!(queue.push_with_limits(text("aaa"), 10, 7));
	assert!(queue.push_with_limits(text("bbbb"), 10, 7));
	assert!(queue.push_with_limits(text("ccccc"), 10, 7));
	assert_eq!(queue.bytes, 5);
	assert_eq!(queue.messages.pop_front().unwrap().into_text().unwrap(), "ccccc");
}

#[test]
fn pending_queue_rejects_oversized_message_without_discarding_existing() {
	let mut queue = PendingQueue::default();
	assert!(queue.push_with_limits(text("kept"), 2, 8));
	assert!(!queue.push_with_limits(text("too large"), 2, 8));
	assert_eq!(queue.bytes, 4);
	assert_eq!(queue.messages.pop_front().unwrap().into_text().unwrap(), "kept");
}

#[test]
fn pending_queue_discards_messages_when_context_owner_changes() {
	let mut queue = PendingQueue::default();
	queue.bind_owner("plugin-a");
	assert!(queue.push_with_limits(text("stale"), 2, 100));
	queue.bind_owner("plugin-b");
	assert!(queue.messages.is_empty());
	assert_eq!(queue.bytes, 0);
	assert_eq!(queue.owner_plugin.as_deref(), Some("plugin-b"));
}
