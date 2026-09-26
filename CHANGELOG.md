# Changelog

All notable changes to this project are documented here. Format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [1.1.2] - 2026-09-26

### Changed

- Expanded the README's remote-transfer section with SSH setup steps:
  trusting a host key, adding a default key via ssh-copy-id, wiring a
  custom key file through ~/.ssh/config, and password-only auth via
  SSH_ASKPASS.

## [1.1.1] - 2026-09-26

### Fixed

- Removed `PrivateTmp=true` from the `filetransferd` systemd unit. It put
  the service in its own mount namespace, which remapped root-owned
  files' apparent UID under systemd's user-namespace sandboxing; OpenSSH's
  strict permission check on `/etc/ssh/ssh_config.d/*.conf` then saw a
  "wrong" owner and refused to parse `ssh_config` at all, silently
  breaking every remote transfer before it reached the network. The
  daemon doesn't use `/tmp` (queue state lives under `XDG_STATE_HOME`,
  the socket under `XDG_RUNTIME_DIR`), so this loses no real isolation.

## [1.1.0] - 2026-09-26

### Added

- Remote (SSH) transfers: `ftctl enqueue` now accepts rsync's native
  `[user@]host:/path` syntax for a source or destination, in addition to
  local paths and already-mounted network shares. Authentication and host
  trust remain entirely the system's own SSH's responsibility (agent,
  `~/.ssh/config`, `known_hosts`); the daemon never stores or handles
  credentials. Interactive prompts (password, first-time host-key
  confirmation) surface through the system's `$SSH_ASKPASS` GUI helper,
  since the daemon has no terminal of its own.
- `version.json` at the repo root as the single source of truth for the
  project version, kept in sync with `manifest.json` via
  `scripts/sync-version.sh` (also run automatically by `update.sh`).

## [1.0.0] - 2026-08-29

### Added

- Initial release: `filetransferd` daemon running queued copy/move jobs
  with `rsync`, `ftctl` CLI, and a Quickshell bar panel for Omarchy.
- Pause/resume (via `SIGSTOP`/`SIGCONT`), cancel, reorder, and persisted
  queue state that survives daemon restarts.
- Nautilus/Thunar "send to Transfer Manager" integration.
- Bar icon states themed to the active Omarchy palette (idle, in
  progress, failed).
