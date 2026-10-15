#!/usr/bin/env bash
# Real network-cut test: blackholes outbound HTTPS with iptables (packets are silently dropped,
# simulating a dead WiFi / link failure), then restores it, and times how long MayaCardio takes to flip mode.
#   sudo ./scripts/netcut_loop.sh            (defaults: 10 cycles, 30 s offline each)
#   sudo CYCLES=5 HOLD=60 ./scripts/netcut_loop.sh
# Requirements: pipeline + mock running, `pipeline status` prints one JSON line with a "mode" field.
set -uo pipefail

[ "$(id -u)" -eq 0 ] || { echo "Run with sudo."; exit 1; }

REPO_DIR="${REPO_DIR:-/home/${SUDO_USER:-pi}/mayacardio}"
STATUS_CMD="${STATUS_CMD:-$REPO_DIR/pipeline status}"
CYCLES="${CYCLES:-10}"
HOLD="${HOLD:-30}"              # seconds the network stays cut
LIMIT="${LIMIT:-10}"            # requirement: switch within 10 s
POLL="${POLL:-0.2}"
SETTLE_MAX="${SETTLE_MAX:-90}"  # max wait for the pipeline to be online before a cycle
CHAIN="MC_NETCUT"
OUT="${OUT:-netcut_$(date +%Y%m%d_%H%M%S).csv}"

now() { awk '{print $1}' /proc/uptime; }     # monotonic: unaffected by clock changes
mode() { (cd "$REPO_DIR" && $STATUS_CMD 2>/dev/null) | jq -r '.mode // empty' 2>/dev/null; }

cut_net() {
  iptables -N $CHAIN 2>/dev/null || true
  iptables -F $CHAIN
  iptables -A $CHAIN -p tcp --dport 443 -j DROP
  iptables -A $CHAIN -p udp --dport 443 -j DROP
  iptables -C OUTPUT -j $CHAIN 2>/dev/null || iptables -I OUTPUT 1 -j $CHAIN
}
restore_net() {
  iptables -D OUTPUT -j $CHAIN 2>/dev/null || true
  iptables -F $CHAIN 2>/dev/null || true
  iptables -X $CHAIN 2>/dev/null || true
}
trap restore_net EXIT INT TERM
restore_net   # clean start

wait_for() {  # wait_for <mode> <timeout> -> echoes elapsed, returns 1 on timeout
  local want="$1" t0 t
  t0=$(now)
  while :; do
    t=$(awk -v a="$(now)" -v b="$t0" 'BEGIN{printf "%.2f", a-b}')
    [ "$(mode)" = "$want" ] && { echo "$t"; return 0; }
    awk -v t="$t" -v m="$2" 'BEGIN{exit !(t>m)}' && return 1
    sleep "$POLL"
  done
}

echo "cycle,to_offline_s,to_online_s,verdict" | tee "$OUT"
fails=0
for i in $(seq 1 "$CYCLES"); do
  echo "--- Cycle $i / $CYCLES: waiting for online state (max ${SETTLE_MAX}s)..."
  if ! wait_for online "$SETTLE_MAX" >/dev/null; then
    echo "$i,NA,NA,SKIP_not_online_before_cut" | tee -a "$OUT"; fails=$((fails+1)); continue
  fi
  sleep $((RANDOM % 8 + 3))                    # cut at a random moment

  echo "  Injecting network cut (outbound port 443 drop)..."
  cut_net;     t_cut=$(now)
  off=$(wait_for offline $((LIMIT*3)))  || off="TIMEOUT"
  echo "  Cut detected: elapsed=${off}s"
  sleep "$HOLD"

  echo "  Restoring network..."
  restore_net; t_res=$(now)
  on=$(wait_for online $((LIMIT*3)))    || on="TIMEOUT"
  echo "  Restore detected: elapsed=${on}s"

  verdict=PASS
  for v in "$off" "$on"; do
    if [ "$v" = "TIMEOUT" ] || awk -v v="$v" -v l="$LIMIT" 'BEGIN{exit !(v>l)}'; then verdict=FAIL; fi
  done
  [ "$verdict" = FAIL ] && fails=$((fails+1))
  echo "$i,$off,$on,$verdict" | tee -a "$OUT"
done

echo
echo "=========================================================="
echo "Network Cut Summary: Cycles=$CYCLES, Failed/Skipped=$fails"
echo "Evidence Log: $OUT"
echo "=========================================================="
echo "Verification reminder: run export device vs cloud to confirm zero silent loss."
[ "$fails" -eq 0 ]
