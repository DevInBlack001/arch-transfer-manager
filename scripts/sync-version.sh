#!/usr/bin/env bash
# Copies the version in version.json (the single source of truth) into
# manifest.json, so the two never drift. Run after bumping version.json,
# before tagging a release.
set -euo pipefail
REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

command -v python3 >/dev/null 2>&1 || { echo "python3 is required but was not found on PATH" >&2; exit 1; }

python3 - "$REPO_DIR" <<'PY'
import json
import os
import sys

repo_dir = sys.argv[1]

with open(os.path.join(repo_dir, "version.json")) as f:
    version = json.load(f)["version"]

manifest_path = os.path.join(repo_dir, "manifest.json")
with open(manifest_path) as f:
    manifest = json.load(f)

if manifest.get("version") != version:
    manifest["version"] = version
    with open(manifest_path, "w") as f:
        json.dump(manifest, f, indent=2)
        f.write("\n")
    print("manifest.json version set to %s" % version)
else:
    print("manifest.json already at %s" % version)
PY
