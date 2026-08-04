use std::collections::HashMap;
use std::sync::LazyLock;
use std::time::{Duration, Instant};

use tokio::sync::Mutex;

use super::{DEACTIVATING_PLUGINS, lookup_property_inspector_owner};

const TOKEN_BYTES: usize = 32;
const TOKEN_TTL: Duration = Duration::from_secs(30);
const MAX_CAPABILITIES: usize = 1024;

struct Capability {
	context: String,
	owner_plugin: String,
	expires_at: Instant,
}

static CAPABILITIES: LazyLock<Mutex<HashMap<String, Capability>>> = LazyLock::new(|| Mutex::new(HashMap::new()));

fn generate_token() -> Result<String, anyhow::Error> {
	let mut bytes = [0_u8; TOKEN_BYTES];
	getrandom::fill(&mut bytes).map_err(|error| anyhow::anyhow!("failed to generate property inspector registration token: {error}"))?;
	const HEX: &[u8; 16] = b"0123456789abcdef";
	let mut token = String::with_capacity(TOKEN_BYTES * 2);
	for byte in bytes {
		token.push(HEX[usize::from(byte >> 4)] as char);
		token.push(HEX[usize::from(byte & 0x0f)] as char);
	}
	Ok(token)
}

fn tokens_match(left: &str, right: &str) -> bool {
	if left.len() != TOKEN_BYTES * 2 || right.len() != TOKEN_BYTES * 2 {
		return false;
	}
	left.as_bytes().iter().zip(right.as_bytes()).fold(0_u8, |difference, (left, right)| difference | (left ^ right)) == 0
}

pub(crate) async fn issue_property_inspector_registration(uuid: &str, expected_owner: &str) -> Result<String, anyhow::Error> {
	let owner_plugin = lookup_property_inspector_owner(uuid).await.ok_or_else(|| anyhow::anyhow!("unknown property inspector context"))?;
	if owner_plugin != expected_owner {
		return Err(anyhow::anyhow!("property inspector context owner changed during load"));
	}
	let token = generate_token()?;
	let now = Instant::now();

	let deactivating = DEACTIVATING_PLUGINS.lock().await;
	if deactivating.contains_key(&owner_plugin) {
		return Err(anyhow::anyhow!("property inspector owner is deactivating"));
	}
	let mut capabilities = CAPABILITIES.lock().await;
	capabilities.retain(|_, capability| capability.expires_at > now);
	while capabilities.len() >= MAX_CAPABILITIES {
		let Some(oldest) = capabilities.iter().min_by_key(|(_, capability)| capability.expires_at).map(|(token, _)| token.clone()) else {
			break;
		};
		capabilities.remove(&oldest);
	}
	capabilities.insert(
		token.clone(),
		Capability {
			context: uuid.to_owned(),
			owner_plugin,
			expires_at: now + TOKEN_TTL,
		},
	);
	drop(capabilities);
	drop(deactivating);

	Ok(token)
}

pub(super) async fn consume(uuid: &str, token: &str) -> Option<String> {
	let now = Instant::now();
	let mut capabilities = CAPABILITIES.lock().await;
	let matching_token = capabilities.keys().find(|candidate| tokens_match(candidate, token)).cloned()?;
	let owner_plugin = capabilities
		.get(&matching_token)
		.filter(|capability| capability.expires_at > now && capability.context == uuid)
		.map(|capability| capability.owner_plugin.clone());
	if owner_plugin.is_some() || capabilities.get(&matching_token).is_some_and(|capability| capability.expires_at <= now) {
		capabilities.remove(&matching_token);
	}
	owner_plugin
}

pub(super) async fn revoke_owner(owner_plugin: &str) {
	CAPABILITIES.lock().await.retain(|_, capability| capability.owner_plugin != owner_plugin);
}

#[cfg(test)]
mod tests {
	use super::{CAPABILITIES, Capability, consume, tokens_match};
	use std::time::{Duration, Instant};

	#[test]
	fn token_comparison_requires_exact_full_token() {
		let token = "a".repeat(64);
		assert!(tokens_match(&token, &token));
		assert!(!tokens_match(&token, &"b".repeat(64)));
		assert!(!tokens_match(&token, "a"));
	}

	#[tokio::test]
	async fn capability_is_single_use_and_wrong_guess_does_not_consume_it() {
		let context = "test-capability-context";
		let owner = "test.owner.sdPlugin";
		let token = "c".repeat(64);
		CAPABILITIES.lock().await.insert(
			token.clone(),
			Capability {
				context: context.to_owned(),
				owner_plugin: owner.to_owned(),
				expires_at: Instant::now() + Duration::from_secs(30),
			},
		);

		assert_eq!(consume(context, &"d".repeat(64)).await, None);
		assert_eq!(consume(context, &token).await.as_deref(), Some(owner));
		assert_eq!(consume(context, &token).await, None);
	}
}
