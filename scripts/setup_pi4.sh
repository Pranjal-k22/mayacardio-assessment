#!/usr/bin/env bash
# One-time setup for a fresh Raspberry Pi OS 64-bit (Bookworm) board.
#   sudo REPO_URL=https://github.com/<you>/<repo>.git MOCK_BIN=/opt/biosignal/biosignal-mock ./setup_pi4.sh
# Re-running is safe. It does NOT start the pipeline: fill in .env first.
set -euo pipefail

[ "$(id -u)" -eq 0 ] || { echo "Run with sudo."; exit 1; }

RUN_USER="${SUDO_USER:-pi}"
REPO_URL="${REPO_URL:-}"                                   # leave empty if repo is already on board
REPO_DIR="${REPO_DIR:-/home/$RUN_USER/mayacardio}"
MOCK_BIN="${MOCK_BIN:-/opt/biosignal/biosignal-mock}"      # official ARM64 mock from organisers
MOCK_DURATION="${MOCK_DURATION:-4200}"                     # >1h so 1-hour run never hits session_end
PIPELINE_ARGS="${PIPELINE_ARGS:-start}"                    # MayaCardio daemon start command
NODE_MAJOR="${NODE_MAJOR:-24}"

echo "== Board check"
ARCH="$(uname -m)"
[ "$ARCH" = "aarch64" ] || echo "WARNING: arch is $ARCH, not aarch64 (ARM64). Results will not count as Pi-class evidence."
echo "arch=$ARCH cores=$(nproc) mem_mb=$(awk '/MemTotal/{print int($2/1024)}' /proc/meminfo)"
[ -r /proc/device-tree/model ] && echo "model=$(tr -d '\0' < /proc/device-tree/model)"

echo "== Packages"
apt-get update -y
apt-get install -y git curl xz-utils jq sqlite3 sysstat iptables python3 ca-certificates

echo "== Node.js ${NODE_MAJOR} (arm64)"
need_node=1
if command -v node >/dev/null 2>&1; then
  [ "$(node -p 'process.versions.node.split(".")[0]')" -ge "$NODE_MAJOR" ] && need_node=0
fi
if [ "$need_node" -eq 1 ]; then
  [ "$ARCH" = "aarch64" ] && NODE_ARCH=arm64 || NODE_ARCH=x64
  BASE="https://nodejs.org/dist/latest-v${NODE_MAJOR}.x"
  TMP="$(mktemp -d)"
  curl -fsSL "$BASE/SHASUMS256.txt" -o "$TMP/SHASUMS256.txt"
  FILE="$(grep "linux-${NODE_ARCH}.tar.xz" "$TMP/SHASUMS256.txt" | awk '{print $2}')"
  curl -fsSL "$BASE/$FILE" -o "$TMP/$FILE"
  (cd "$TMP" && grep " $FILE\$" SHASUMS256.txt | sha256sum -c -)
  tar -xJf "$TMP/$FILE" -C /usr/local --strip-components=1
  rm -rf "$TMP"
fi
echo "node $(node -v)"

echo "== Persistent journal (keeps logs across power cuts)"
mkdir -p /var/log/journal
systemctl restart systemd-journald || true

echo "== Repository"
if [ ! -d "$REPO_DIR" ]; then
  if [ -n "$REPO_URL" ]; then
    sudo -u "$RUN_USER" git clone "$REPO_URL" "$REPO_DIR"
  else
    echo "Repo not found at $REPO_DIR and REPO_URL not set. Clone it, then re-run."; exit 1
  fi
fi
if [ ! -f "$REPO_DIR/.env" ]; then
  if [ -f "$REPO_DIR/.env.example" ]; then
    sudo -u "$RUN_USER" cp "$REPO_DIR/.env.example" "$REPO_DIR/.env"
  else
    sudo -u "$RUN_USER" touch "$REPO_DIR/.env"
  fi
  chmod 600 "$REPO_DIR/.env"
  echo "Created $REPO_DIR/.env -> fill in the Worker URL and the rotated token."
fi

# Ensure user owns repo directory and data/logs
chown -R "$RUN_USER:$RUN_USER" "$REPO_DIR"

echo "== systemd units"
if [ -x "$MOCK_BIN" ]; then
cat > /etc/systemd/system/biosignal-mock.service <<EOF
[Unit]
Description=biosignal-mock (organisers' mock)

[Service]
User=$RUN_USER
ExecStart=$MOCK_BIN --mode signal --seed 7 --duration $MOCK_DURATION --port 7100
Restart=always
RestartSec=1

[Install]
WantedBy=multi-user.target
EOF
else
  echo "Mock not found at $MOCK_BIN: skipping its unit (place official mock at $MOCK_BIN or set MOCK_BIN and re-run)."
fi

# No network-online.target dependency: the pipeline starts immediately without network.
cat > /etc/systemd/system/pipeline.service <<EOF
[Unit]
Description=MayaCardio edge pipeline
After=local-fs.target

[Service]
User=$RUN_USER
WorkingDirectory=$REPO_DIR
EnvironmentFile=-$REPO_DIR/.env
ExecStart=/usr/local/bin/node --experimental-strip-types bin/pipeline.ts $PIPELINE_ARGS
Restart=always
RestartSec=1
# Pi-class limits from the assessment
CPUQuota=400%
MemoryMax=2G

[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload
systemctl enable pipeline.service
[ -x "$MOCK_BIN" ] && systemctl enable biosignal-mock.service

cat <<EOF

Done. Next:
  1. Edit $REPO_DIR/.env (Worker URL + rotated CLOUD_AUTH_TOKEN).
  2. sudo systemctl start biosignal-mock pipeline
  3. journalctl -u pipeline -f
  4. $REPO_DIR/pipeline status
EOF
