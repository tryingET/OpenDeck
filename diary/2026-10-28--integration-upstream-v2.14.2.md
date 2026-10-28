# OpenDeck v2.14.2 source integration

## Scope

AK7722: operator authorized resolving the existing hardening branch onto current
upstream, preserving local adoptions and assessing possible contributions.
No installed-binary upgrade, runtime restart, or upstream submission.

## Source lineage

- Original fork tip: `7635b22d437aed94ef26dffc7a54e9debf60e882`.
- Upstream: `3ddecc1ce8952a423916cf066724356267b97410` (v2.14.2).
- Retained original tip: `backup/opendeck-before-upstream-ak7721`.
- Nine non-merge local commits replayed in an isolated worktree; eight replayed
  patch-identically. The first required resolving seven conflicting files.
- Registration conflict: retain local atomic duplicate checks and identity checks,
  preserving upstream's duplicate-registration protection.
- Loopback conflict: both sides bind loopback; retain equivalent tuple-form binds.
- Preserve newer fork dependency minima, adopt upstream's Tauri 2.12 / build 2.7,
  Elgato 0.14 and pinned Enigo revision, and reconcile three lockfiles:
  Deno plus the host and starter-pack Cargo locks.
- SOOMFON layout, click-to-assign and session/capability protections retained.

## Follow-up cleanup

A current-Clippy warning in the pre-existing asset resolver was corrected by
collapsing a nested conditional without changing its order or behavior. The
starter-pack build script was formatted with the repository's Prettier settings.

## Verification

- `cargo fmt --check` for host and starter-pack manifests: passed.
- `cargo metadata --locked --no-deps` for both manifests: passed (not a build).
- `deno install --frozen`: passed after lock reconciliation.
- `deno lint`: passed, 19 files.
- `deno check` for configuration/version scripts and frozen plugin build script:
  passed.
- `deno task check`: 0 errors, 0 warnings.
- `deno task build`: production frontend build passed.
- Prettier check on all changed TypeScript/Svelte/JSON files: passed after cleanup.
- Native Clippy with `--all-targets -- -D warnings`: passed after cleanup.
- `cargo test --locked --manifest-path src-tauri/Cargo.toml --bin opendeck`:
  19 passed, 0 failed, 0 ignored. No application window was launched.
- Native checks ran through the heavy-job runner on data-volume scratch;
  successful run completed 2026-10-28T07:02:20+01:00.
- Independent source inspection found no integration-introduced source blocker;
  it required the regenerated locks to be committed before push.
- No reboot/login, hardware interaction, full release packaging, or GUI behavior
  proof is claimed. Installed package remains separate from this source update.

## Contribution boundary

Assess small changes rather than submitting this whole branch. Locked plugin
builds are a possible first unit; click-to-assign needs focused interaction proof.
Coordinate overlapping socket work with existing upstream proposals. Upstream
rejects hardware-specific support submissions for non-Elgato/non-Tacto devices,
so SOOMFON-specific mapping remains local. Its adopted AI policy also requires
human-written submission explanations and informed human ownership. No upstream
issue, PR, comment or disclosure was made in this task.
