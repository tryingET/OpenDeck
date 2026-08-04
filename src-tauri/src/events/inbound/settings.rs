use crate::events::outbound::settings as outbound;
use crate::shared::ActionContext;
use crate::store::profiles::{acquire_locks, acquire_locks_mut, get_instance, get_instance_mut, mark_profile_stale};

use std::io::Write;
use std::str::FromStr;

pub async fn set_settings(event: super::ContextAndPayloadEvent<serde_json::Value>, from_property_inspector: bool, expected_owner: Option<&str>) -> Result<(), anyhow::Error> {
	let mut locks = acquire_locks_mut().await;

	if let Some(instance) = get_instance_mut(&event.context, &mut locks).await? {
		if expected_owner.is_some_and(|owner| instance.action.plugin != owner) {
			return Ok(());
		}
		instance.settings = event.payload;
		outbound::did_receive_settings(instance, !from_property_inspector).await?;
		mark_profile_stale(&event.context.device, &mut locks).await?;
	}

	Ok(())
}

pub async fn get_settings(event: super::ContextEvent, from_property_inspector: bool, expected_owner: Option<&str>) -> Result<(), anyhow::Error> {
	let locks = acquire_locks().await;

	if let Some(instance) = get_instance(&event.context, &locks).await? {
		if expected_owner.is_some_and(|owner| instance.action.plugin != owner) {
			return Ok(());
		}
		outbound::did_receive_settings(instance, from_property_inspector).await?;
	}

	Ok(())
}

pub async fn set_global_settings(event: super::ContextAndPayloadEvent<serde_json::Value, String>, from_property_inspector: bool, expected_owner: Option<&str>) -> Result<(), anyhow::Error> {
	let uuid = if from_property_inspector {
		if let Some(instance) = get_instance(&ActionContext::from_str(&event.context)?, &acquire_locks().await).await? {
			if expected_owner.is_some_and(|owner| instance.action.plugin != owner) {
				return Ok(());
			}
			instance.action.plugin.clone()
		} else {
			return Ok(());
		}
	} else {
		event.context.clone()
	};
	if !crate::plugins::is_safe_plugin_uuid(&uuid) {
		return Err(anyhow::anyhow!("unsafe plugin id for global settings"));
	}

	{
		let settings_dir = crate::shared::config_dir().join("settings");
		tokio::fs::create_dir_all(&settings_dir).await?;

		let mut file = std::fs::OpenOptions::new().write(true).truncate(true).create(true).open(settings_dir.join(uuid.clone() + ".json"))?;
		file.lock()?;
		file.write_all(event.payload.to_string().as_bytes())?;
		file.sync_data()?;
		file.unlock()?;
	}

	outbound::did_receive_global_settings(&uuid, !from_property_inspector).await?;

	Ok(())
}

pub async fn get_global_settings(event: super::ContextEvent<String>, from_property_inspector: bool, expected_owner: Option<&str>) -> Result<(), anyhow::Error> {
	let uuid = if from_property_inspector {
		if let Some(instance) = get_instance(&ActionContext::from_str(&event.context)?, &acquire_locks().await).await? {
			if expected_owner.is_some_and(|owner| instance.action.plugin != owner) {
				return Ok(());
			}
			instance.action.plugin.clone()
		} else {
			return Ok(());
		}
	} else {
		event.context.clone()
	};
	if !crate::plugins::is_safe_plugin_uuid(&uuid) {
		return Err(anyhow::anyhow!("unsafe plugin id for global settings"));
	}

	outbound::did_receive_global_settings(&uuid, from_property_inspector).await
}
