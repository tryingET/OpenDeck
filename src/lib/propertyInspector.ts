import type { Action } from "./Action.ts";
import type { Context } from "./Context.ts";

import { get, type Writable, writable } from "svelte/store";

export const inspectedInstance: Writable<string | Context | null> = writable(null);

import { invoke } from "@tauri-apps/api/core";
let old: string | Context | null = null;
inspectedInstance.subscribe(async (value) => {
	await invoke("switch_property_inspector", {
		old: typeof old == "string" ? old : null,
		new: typeof value == "string" ? value : null,
	});
	old = value;
});

export const inspectedParentAction: Writable<Context | null> = writable(null);

export const openContextMenu: Writable<{ context: Context; x: number; y: number } | null> = writable(null);
document.addEventListener("click", () => openContextMenu.set(null));
document.addEventListener("keydown", (event) => {
	if (event.key == "Escape") openContextMenu.set(null);
});
globalThis.addEventListener("blur", () => openContextMenu.set(null));

export type CopiedItem = { type: "instance"; source: Context } | { type: "action"; action: Action };
export const copiedItem: Writable<CopiedItem | null> = writable(null);

// Separate click-to-assign intent from the explicit copy/paste clipboard.
export const selectedAction: Writable<Action | null> = writable(null);

export type SelectedActionIntent = { action: Action; generation: number };
let selectedActionGeneration = 0;

export function setSelectedAction(action: Action | null) {
	selectedActionGeneration += 1;
	selectedAction.set(action);
}

export function takeSelectedAction(): SelectedActionIntent | null {
	const action = get(selectedAction);
	if (!action) return null;
	selectedActionGeneration += 1;
	selectedAction.set(null);
	return { action, generation: selectedActionGeneration };
}

export function restoreSelectedAction(intent: SelectedActionIntent) {
	if (selectedActionGeneration != intent.generation) return false;
	selectedActionGeneration += 1;
	selectedAction.set(intent.action);
	return true;
}
