# Network (remote SSH) transfers

## Goal

Let `ftctl enqueue` accept rsync's native `[user@]host:/path` syntax for a
source or destination, so the daemon can push/pull files to/from another
machine on the network, in addition to local paths and already-mounted
network shares (which already work today).

## Non-goals

- No credential storage, key management, or host-trust setup in this
  project. Authentication is entirely the system's own SSH (agent,
  `~/.ssh/config`, `known_hosts`).
- No `rsync://` daemon-protocol support (separate port, separate auth
  model). Only SSH-transport remote specs.
- No remote-to-remote (host A to host B) transfers; rsync itself can't do
  this in one hop, so it's rejected at enqueue time.
- No automatic retry/backoff on network drop. A dropped job surfaces as an
  `error` job; the user hits the existing Resume button to retry, and
  rsync's `--partial` means it doesn't restart from scratch.

## Remote-spec detection

A string is a remote spec if it matches `[user@]host:path` *before* its
first `/`, e.g. `nas.local:/mnt/data` or `armiya@192.168.1.50:/srv/backup`.
A string starting with `/`, `.`, or `~`, or with no `:` before its first
`/`, is local. This mirrors rsync's own longstanding heuristic, so
anything rsync itself would treat as remote is treated as remote here too.

New helper in `daemon/ft_common.py`:

```python
import re

_REMOTE_RE = re.compile(r"^(?:[^@/\s]+@)?[^/\s:]+:(?!//).+")

def is_remote_spec(path):
    """True if `path` is an rsync-style [user@]host:path remote spec."""
    return bool(_REMOTE_RE.match(path))
```

(The `(?!//)` guard excludes `rsync://host/path`, which is out of scope.)

## Validation changes (`filetransferd.py`, `_validate_enqueue`)

- Local sources/dest: unchanged (`os.path.lexists`, `os.path.isdir`).
- Remote sources/dest: skip the filesystem existence/type check entirely
  (no network round-trip during enqueue); still reject empty strings and
  NUL bytes.
- If every source is remote **and** dest is remote: reject with
  `"remote-to-remote transfers are not supported: one side must be local"`.
- `_schedule()`'s "does source still exist" recheck before starting a job
  is skipped for remote sources (same reasoning: can't check without a
  network call, and rsync will report it anyway).

## rsync invocation (`_run_job`)

When any source or the dest is a remote spec, add
`-e "ssh -o ConnectTimeout=10"` to the rsync argv.

- No `BatchMode=yes`. `filetransferd`'s subprocess has no controlling
  terminal (`asyncio.create_subprocess_exec`, no PTY), so OpenSSH already
  routes prompts (password, host-key confirmation) to `$SSH_ASKPASS`
  instead of a TTY, provided `DISPLAY`/`WAYLAND_DISPLAY` and `SSH_ASKPASS`
  are in the daemon's environment. This keeps auth fully in the system's
  hands (an askpass helper is a system component, not code this project
  writes).
- `ConnectTimeout=10` bounds a genuinely unreachable host; it does not cut
  off a pending askpass prompt.
- The `-e` string is a fixed constant, never built from user input, so it
  adds no shell-injection surface. Everything is still `argv`-based
  (`asyncio.create_subprocess_exec`, `shell=False` implicitly), consistent
  with the existing rsync invocation.
- `--remove-source-files` (move mode) already works correctly when the
  source is remote or local; rsync's remote side performs the deletion,
  no change needed.

## Environment wiring for interactive auth

- `systemd/filetransferd.service.in` gets
  `PassEnvironment=DISPLAY WAYLAND_DISPLAY SSH_ASKPASS`.
- `install.sh` runs
  `systemctl --user import-environment DISPLAY WAYLAND_DISPLAY SSH_ASKPASS`
  after installing/updating the unit (best-effort; not fatal if it fails,
  e.g. no graphical session yet).
- README gets a "Remote transfers" section documenting: the
  `user@host:/path` syntax, that a graphical askpass helper (e.g.
  `ssh-askpass`, `seahorse`, `x11-ssh-askpass`) must be installed for
  interactive prompts to appear, and that session env can go stale across
  a Hyprland/session restart (re-run `systemctl --user import-environment
  ...` or restart the daemon after logging back in).

## Error handling

Connection failures (unreachable host, refused, non-interactive auth
failure with no askpass available) surface via rsync's stderr into the
job's existing `error` field, exactly like today's local errors. No new
job states.

## Testing

- Unit tests for `is_remote_spec`: local absolute/relative/`~` paths,
  `user@host:path`, `host:path` (no user), a local filename that happens
  to contain `:` (e.g. `./weird:name`), `rsync://host/path` (must NOT
  match).
- Unit test for `_validate_enqueue` rejecting remote-to-remote.
- Manual end-to-end test against a real LAN host over SSH: push (local
  source, remote dest), pull (remote source, local dest), and one
  deliberately-broken case (unreachable host) confirming it errors within
  `ConnectTimeout` instead of hanging.

## Docs / versioning

- README: new "Remote transfers" section as above.
- CHANGELOG: new entry under "Added" for this feature.
- `version.json` (new, repo root): single source of truth for the
  project's version, e.g. `{"version": "1.1.0"}`. `manifest.json`'s
  `version` field and any other version string in the repo get read from
  or kept in sync with this file. A short `sync-version.sh` (or inline
  step in `update.sh`/CI) copies `version.json`'s value into
  `manifest.json` so they never drift.
- A security review pass (per the project's standing checklist: RCE via
  subprocess argv, injection, auth scope, resource handling) runs before
  bumping the version and pushing/tagging a GitHub release.
