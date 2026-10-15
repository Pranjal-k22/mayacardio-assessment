#!/usr/bin/env bash
# Alias to profile_arm64.sh for Raspberry Pi 4 environment
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
exec bash "${SCRIPT_DIR}/profile_arm64.sh" "$@"
