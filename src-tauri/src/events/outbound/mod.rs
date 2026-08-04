pub mod applications;
pub mod deep_link;
pub mod devices;
pub mod encoder;
pub mod keypad;
pub mod misc;
pub mod property_inspector;
pub mod settings;
pub mod states;
pub mod will_appear;

use serde::Serialize;

#[derive(Serialize)]
struct Coordinates {
	row: u8,
	column: u8,
}

#[derive(Serialize)]
#[allow(non_snake_case)]
struct GenericInstancePayload {
	settings: serde_json::Value,
	coordinates: Coordinates,
	controller: String,
	state: u16,
	isInMultiAction: bool,
}

impl GenericInstancePayload {
	fn new(instance: &crate::shared::ActionInstance) -> Self {
		let coordinates = match &instance.context.controller[..] {
			"Encoder" => Coordinates {
				row: 0,
				column: instance.context.position,
			},
			_ => {
				let columns = crate::shared::DEVICES.get(&instance.context.device).unwrap().columns;
				Coordinates {
					row: instance.context.position / columns,
					column: instance.context.position % columns,
				}
			}
		};

		Self {
			settings: instance.settings.clone(),
			coordinates,
			controller: instance.context.controller.clone(),
			state: instance.current_state,
			isInMultiAction: instance.context.index != 0,
		}
	}
}

async fn send_to_plugin(plugin: &str, data: &impl Serialize) -> Result<(), anyhow::Error> {
	let message = tokio_tungstenite::tungstenite::Message::Text(serde_json::to_string(data)?.into());
	super::send_plugin_message(plugin, message).await
}

async fn send_to_all_plugins(data: &impl Serialize) -> Result<(), anyhow::Error> {
	let mut entries = tokio::fs::read_dir(crate::shared::config_dir().join("plugins")).await?;
	while let Ok(Some(entry)) = entries.next_entry().await {
		let path = match entry.metadata().await?.is_symlink() {
			true => tokio::fs::read_link(entry.path()).await?,
			false => entry.path(),
		};
		let metadata = tokio::fs::metadata(&path).await?;
		if metadata.is_dir() {
			let _ = send_to_plugin(entry.file_name().to_str().unwrap(), data).await;
		}
	}
	Ok(())
}

async fn send_to_property_inspector(context: &crate::shared::ActionContext, owner_plugin: &str, data: &impl Serialize) -> Result<(), anyhow::Error> {
	let message = tokio_tungstenite::tungstenite::Message::Text(serde_json::to_string(data)?.into());
	super::send_property_inspector_message(&context.to_string(), owner_plugin, message).await
}
