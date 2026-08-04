use super::ContextAndPayloadEvent;

pub async fn send_to_plugin(event: ContextAndPayloadEvent<serde_json::Value>, owner_plugin: &str) -> Result<(), anyhow::Error> {
	crate::events::outbound::property_inspector::send_to_plugin(event.context, owner_plugin, event.payload).await?;
	Ok(())
}

pub async fn send_to_property_inspector(event: ContextAndPayloadEvent<serde_json::Value>) -> Result<(), anyhow::Error> {
	crate::events::outbound::property_inspector::send_to_property_inspector(event.context, event.payload).await?;
	Ok(())
}
