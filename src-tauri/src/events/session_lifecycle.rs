use std::collections::HashMap;
use std::sync::LazyLock;

use tokio::sync::Mutex;

use super::property_inspector_auth;

pub(super) static DEACTIVATING_PLUGINS: LazyLock<Mutex<HashMap<String, usize>>> = LazyLock::new(|| Mutex::new(HashMap::new()));

pub struct PluginDeactivationGuard {
	uuid: Option<String>,
}

impl PluginDeactivationGuard {
	pub async fn release(mut self) {
		if let Some(uuid) = self.uuid.as_deref() {
			end_plugin_deactivation(uuid).await;
			self.uuid = None;
		}
	}
}

impl Drop for PluginDeactivationGuard {
	fn drop(&mut self) {
		let Some(uuid) = self.uuid.take() else {
			return;
		};
		if let Ok(runtime) = tokio::runtime::Handle::try_current() {
			runtime.spawn(async move { end_plugin_deactivation(&uuid).await });
		}
	}
}

pub async fn begin_plugin_deactivation(uuid: &str) -> PluginDeactivationGuard {
	let mut deactivating = DEACTIVATING_PLUGINS.lock().await;
	*deactivating.entry(uuid.to_owned()).or_default() += 1;
	drop(deactivating);
	let guard = PluginDeactivationGuard { uuid: Some(uuid.to_owned()) };
	property_inspector_auth::revoke_owner(uuid).await;
	guard
}

async fn end_plugin_deactivation(uuid: &str) {
	let mut deactivating = DEACTIVATING_PLUGINS.lock().await;
	if let Some(count) = deactivating.get_mut(uuid) {
		*count = count.saturating_sub(1);
		if *count == 0 {
			deactivating.remove(uuid);
		}
	}
}

pub(super) async fn plugin_is_deactivating(uuid: &str) -> bool {
	DEACTIVATING_PLUGINS.lock().await.contains_key(uuid)
}

#[cfg(test)]
mod tests {
	use super::{begin_plugin_deactivation, plugin_is_deactivating};

	#[tokio::test]
	async fn nested_deactivation_guards_keep_registration_blocked_until_all_release() {
		let plugin = "test.deactivation.sdPlugin";
		let first = begin_plugin_deactivation(plugin).await;
		let second = begin_plugin_deactivation(plugin).await;
		first.release().await;
		assert!(plugin_is_deactivating(plugin).await);
		second.release().await;
		assert!(!plugin_is_deactivating(plugin).await);
	}

	#[tokio::test]
	async fn dropped_deactivation_guard_eventually_releases_registration() {
		let plugin = "test.dropped-deactivation.sdPlugin";
		let guard = begin_plugin_deactivation(plugin).await;
		drop(guard);
		for _ in 0..10 {
			if !plugin_is_deactivating(plugin).await {
				return;
			}
			tokio::task::yield_now().await;
		}
		assert!(!plugin_is_deactivating(plugin).await);
	}
}
