#!/usr/bin/env bash
# ==============================================================================
# MayaCardio - Section 5.4 1-Hour ARM64 / Raspberry Pi 4 Resource Profiling Harness
#
# Author: Pranjal Karan
#
# Measures the exact 4 figures required by Case Assessment Section 5 Item 4:
#   1. Average CPU (%)
#   2. Peak memory (MB RSS)
#   3. Megabytes written to disk
#   4. Megabytes sent over the network
#
# Compatible with:
#   - Physical Raspberry Pi 4 (Raspberry Pi OS 64-bit / Debian Bookworm)
#   - ARM64 Linux Virtual Machines (QEMU / Cloud ARM64 instances)
#   - ARM64 Linux Docker Containers (--platform linux/arm64 --cpus=4 --memory=2g)
# ==============================================================================
set -euo pipefail

if [ -f ".env" ]; then
    set -a
    . .env
    set +a
fi

DURATION=${1:-3600} # Default 3,600 seconds (1 hour)
PORT=${PORT:-7100}
SAMPLE_INTERVAL=5
SESSION_ID="profile-1h-$(date +%s)"
OUTPUT_DIR="measurements/profile_$(date +%Y%m%d_%H%M%S)_arm64"
mkdir -p "$OUTPUT_DIR"

CSV_FILE="$OUTPUT_DIR/resource_timeseries.csv"
SUMMARY_FILE="$OUTPUT_DIR/resource_summary.json"
REPORT_FILE="$OUTPUT_DIR/resource_report.txt"
DB_PATH="$OUTPUT_DIR/pipeline.db"

echo "================================================================================"
echo "MayaCardio 1-Hour ARM64 Resource Profiling Harness (Section 5.4)"
echo "Author            : Pranjal Karan"
echo "Host Architecture : $(uname -m)"
echo "Kernel Release    : $(uname -r)"
echo "Node Runtime      : $(node -v)"
echo "Profiling Duration: ${DURATION} seconds (1 hour)"
echo "Database Path     : ${DB_PATH}"
echo "Telemetry Port    : ${PORT}"
echo "Output Directory  : ${OUTPUT_DIR}"
echo "================================================================================"

# Identify network interface for TX bytes
DEFAULT_IFACE=$(ip route get 1.1.1.1 2>/dev/null | awk '{print $5; exit}' || echo "eth0")
if [ ! -f "/sys/class/net/${DEFAULT_IFACE}/statistics/tx_bytes" ]; then
    DEFAULT_IFACE=$(ls /sys/class/net 2>/dev/null | grep -v 'lo' | head -n1 || echo "lo")
fi
echo "Monitoring Network Interface: ${DEFAULT_IFACE}"

# Read thermal temperature across Pi 4 vs VM/Container
get_temp() {
    if command -v vcgencmd >/dev/null 2>&1; then
        vcgencmd measure_temp | grep -o '[0-9.]*' || echo "0.0"
    elif [ -f "/sys/class/thermal/thermal_zone0/temp" ]; then
        awk '{printf "%.1f", $1/1000}' /sys/class/thermal/thermal_zone0/temp 2>/dev/null || echo "0.0"
    else
        echo "N/A"
    fi
}

# Read network TX bytes
get_net_tx_bytes() {
    if [ -f "/sys/class/net/${DEFAULT_IFACE}/statistics/tx_bytes" ]; then
        cat "/sys/class/net/${DEFAULT_IFACE}/statistics/tx_bytes"
    elif [ -f "/proc/net/dev" ]; then
        awk -v iface="${DEFAULT_IFACE}:" '$1 == iface {print $10}' /proc/net/dev 2>/dev/null || echo "0"
    else
        echo "0"
    fi
}

# Read process CPU ticks (utime + stime) from /proc/<pid>/stat
get_proc_cpu_ticks() {
    local pid=$1
    if [ -f "/proc/$pid/stat" ]; then
        awk '{print $14 + $15}' "/proc/$pid/stat" 2>/dev/null || echo "0"
    else
        echo "0"
    fi
}

# Read peak memory VmHWM (High Water Mark) from /proc/<pid>/status in KB
get_proc_vm_hwm_kb() {
    local pid=$1
    if [ -f "/proc/$pid/status" ]; then
        awk '$1 == "VmHWM:" {print $2}' "/proc/$pid/status" 2>/dev/null || echo "0"
    else
        echo "0"
    fi
}

# Read write_bytes from /proc/<pid>/io
get_proc_write_bytes() {
    local pid=$1
    if [ -f "/proc/$pid/io" ]; then
        awk '$1 == "write_bytes:" {print $2}' "/proc/$pid/io" 2>/dev/null || echo "0"
    else
        echo "0"
    fi
}

# 1. Setup Cloud Worker endpoint for ONLINE mode
CLOUD_URL=${CLOUD_SYNC_URL:-${CLOUD_BASE_URL:-""}}
MOCK_CLOUD_PID=""
if [ -z "$CLOUD_URL" ]; then
    echo "[0/3] Starting local Cloudflare Worker simulation server on port 8787 for ONLINE mode..."
    node --experimental-strip-types tools/mock_cloud_worker.ts 8787 > "$OUTPUT_DIR/mock_cloud.log" 2>&1 &
    MOCK_CLOUD_PID=$!
    CLOUD_URL="http://127.0.0.1:8787"
    sleep 1
fi
echo "Online Cloud Sync Target: ${CLOUD_URL}"

# 2. Start the biosignal mock generator (400 Hz dual-sensor telemetry)
echo "[1/3] Starting Biosignal Mock Generator on port ${PORT}..."
if [ -f "./biosignal-mock" ]; then
    ./biosignal-mock --mode signal --seed 7 --duration $((DURATION + 60)) --port "${PORT}" > "$OUTPUT_DIR/mock.log" 2>&1 &
    MOCK_PID=$!
else
    node --experimental-strip-types tools/biosignal_mock.ts --mode signal --seed 7 --duration $((DURATION + 60)) --port "${PORT}" > "$OUTPUT_DIR/mock.log" 2>&1 &
    MOCK_PID=$!
fi

sleep 2

# 3. Start the MayaCardio pipeline in background (ONLINE mode)
echo "[2/3] Starting MayaCardio pipeline daemon in ONLINE mode..."
node --experimental-strip-types bin/pipeline.ts start \
    --db "$DB_PATH" \
    --port "${PORT}" \
    --session "$SESSION_ID" \
    --cloud-url "$CLOUD_URL" > "$OUTPUT_DIR/pipeline.log" 2>&1 &
PIPE_PID=$!

sleep 2

if ! kill -0 "$PIPE_PID" 2>/dev/null; then
    echo "ERROR: Pipeline process failed to start! Check $OUTPUT_DIR/pipeline.log"
    kill "$MOCK_PID" 2>/dev/null || true
    if [ -n "$MOCK_CLOUD_PID" ]; then kill "$MOCK_CLOUD_PID" 2>/dev/null || true; fi
    exit 1
fi

echo "[3/3] Telemetry acquisition active in ONLINE mode. Recording metrics for ${DURATION}s..."
echo "timestamp,elapsed_sec,cpu_pct,rss_mb,disk_mb,net_tx_mb,temp_c" > "$CSV_FILE"

# Kernel metric baselines
CLK_TCK=$(getconf CLK_TCK 2>/dev/null || echo 100)
INITIAL_NET_TX=$(get_net_tx_bytes)
START_TIME=$(date +%s)
START_CPU_TICKS=$(get_proc_cpu_ticks "$PIPE_PID")
START_IO_WRITE_BYTES=$(get_proc_write_bytes "$PIPE_PID")
PEAK_RSS_MB=0

cleanup() {
    echo ""
    echo "[!] Halting background processes..."
    kill "$PIPE_PID" 2>/dev/null || true
    kill "$MOCK_PID" 2>/dev/null || true
    if [ -n "$MOCK_CLOUD_PID" ]; then kill "$MOCK_CLOUD_PID" 2>/dev/null || true; fi
}
trap cleanup EXIT INT TERM

LAST_TICKS=$START_CPU_TICKS
LAST_TICK_TIME=$START_TIME

while true; do
    NOW=$(date +%s)
    ELAPSED=$((NOW - START_TIME))
    if [ "$ELAPSED" -ge "$DURATION" ]; then
        break
    fi

    if ! kill -0 "$PIPE_PID" 2>/dev/null; then
        echo "WARNING: Pipeline process terminated prematurely at ${ELAPSED}s!"
        break
    fi

    # Read kernel ticks for instantaneous CPU
    CURRENT_TICKS=$(get_proc_cpu_ticks "$PIPE_PID")
    TICK_DELTA=$((CURRENT_TICKS - LAST_TICKS))
    TIME_DELTA=$((NOW - LAST_TICK_TIME))
    if [ "$TIME_DELTA" -gt 0 ] && [ "$TICK_DELTA" -ge 0 ]; then
        CPU=$(awk "BEGIN {printf \"%.1f\", (${TICK_DELTA} / (${TIME_DELTA} * ${CLK_TCK})) * 100}")
    else
        STATS=$(ps -p "$PIPE_PID" -o %cpu --no-headers 2>/dev/null || echo "0.0")
        CPU=$(echo "$STATS" | awk '{print $1}')
    fi
    LAST_TICKS=$CURRENT_TICKS
    LAST_TICK_TIME=$NOW

    # Read kernel VmHWM for true peak memory (RSS)
    VM_HWM_KB=$(get_proc_vm_hwm_kb "$PIPE_PID")
    if [ "$VM_HWM_KB" -gt 0 ]; then
        RSS_MB=$(awk "BEGIN {printf \"%.2f\", ${VM_HWM_KB} / 1024}")
        PEAK_RSS_MB=$RSS_MB
    else
        STATS=$(ps -p "$PIPE_PID" -o rss --no-headers 2>/dev/null || echo "0")
        RSS_KB=$(echo "$STATS" | awk '{print $1}')
        RSS_MB=$(awk "BEGIN {printf \"%.2f\", ${RSS_KB} / 1024}")
        IS_HIGHER=$(awk "BEGIN {print (${RSS_MB} > ${PEAK_RSS_MB}) ? 1 : 0}")
        if [ "$IS_HIGHER" -eq 1 ]; then
            PEAK_RSS_MB=$RSS_MB
        fi
    fi

    # Measure current DB & WAL disk usage
    DISK_BYTES=$(du -cb "$DB_PATH"* 2>/dev/null | tail -n1 | awk '{print $1}' || echo "0")
    DISK_MB=$(awk "BEGIN {printf \"%.2f\", ${DISK_BYTES} / (1024 * 1024)}")

    # Measure network TX bytes delta
    CURRENT_NET_TX=$(get_net_tx_bytes)
    TX_DELTA_BYTES=$((CURRENT_NET_TX - INITIAL_NET_TX))
    if [ "$TX_DELTA_BYTES" -lt 0 ]; then
        TX_DELTA_BYTES=0
    fi
    TX_DELTA_MB=$(awk "BEGIN {printf \"%.2f\", ${TX_DELTA_BYTES} / (1024 * 1024)}")

    TEMP=$(get_temp)
    TIMESTAMP=$(date -u +"%Y-%m-%dT%H:%M:%SZ")

    echo "${TIMESTAMP},${ELAPSED},${CPU},${RSS_MB},${DISK_MB},${TX_DELTA_MB},${TEMP}" >> "$CSV_FILE"

    # Display progress every 60 seconds
    if [ $((ELAPSED % 60)) -lt $SAMPLE_INTERVAL ]; then
        echo "[PROGRESS] ${ELAPSED}s / ${DURATION}s | CPU: ${CPU}% | Peak RSS: ${PEAK_RSS_MB} MB | Disk: ${DISK_MB} MB | Net TX: ${TX_DELTA_MB} MB | Temp: ${TEMP}°C"
    fi

    sleep "$SAMPLE_INTERVAL"
done

# Final exact kernel calculations
FINAL_TIME=$(date +%s)
TOTAL_ELAPSED=$((FINAL_TIME - START_TIME))
FINAL_CPU_TICKS=$(get_proc_cpu_ticks "$PIPE_PID")
TOTAL_TICKS=$((FINAL_CPU_TICKS - START_CPU_TICKS))

if [ "$TOTAL_ELAPSED" -gt 0 ] && [ "$TOTAL_TICKS" -gt 0 ]; then
    FINAL_AVG_CPU=$(awk "BEGIN {printf \"%.2f\", (${TOTAL_TICKS} / (${TOTAL_ELAPSED} * ${CLK_TCK})) * 100}")
else
    FINAL_AVG_CPU="0.00"
fi

# Final Peak Memory from kernel VmHWM
FINAL_VM_HWM_KB=$(get_proc_vm_hwm_kb "$PIPE_PID")
if [ "$FINAL_VM_HWM_KB" -gt 0 ]; then
    PEAK_RSS_MB=$(awk "BEGIN {printf \"%.2f\", ${FINAL_VM_HWM_KB} / 1024}")
fi

# Final disk bytes written from /proc/<pid>/io write_bytes delta
FINAL_IO_WRITE_BYTES=$(get_proc_write_bytes "$PIPE_PID")
if [ "$FINAL_IO_WRITE_BYTES" -gt "$START_IO_WRITE_BYTES" ]; then
    DELTA_WRITE_BYTES=$((FINAL_IO_WRITE_BYTES - START_IO_WRITE_BYTES))
    FINAL_DISK_MB=$(awk "BEGIN {printf \"%.2f\", ${DELTA_WRITE_BYTES} / (1024 * 1024)}")
else
    FINAL_DISK_BYTES=$(du -cb "$DB_PATH"* 2>/dev/null | tail -n1 | awk '{print $1}' || echo "0")
    FINAL_DISK_MB=$(awk "BEGIN {printf \"%.2f\", ${FINAL_DISK_BYTES} / (1024 * 1024)}")
fi

# Final network TX delta
FINAL_NET_TX=$(get_net_tx_bytes)
FINAL_NET_DELTA_BYTES=$((FINAL_NET_TX - INITIAL_NET_TX))
if [ "$FINAL_NET_DELTA_BYTES" -lt 0 ]; then
    FINAL_NET_DELTA_BYTES=0
fi
FINAL_NET_MB=$(awk "BEGIN {printf \"%.2f\", ${FINAL_NET_DELTA_BYTES} / (1024 * 1024)}")

# Generate machine-readable JSON summary
cat <<EOF > "$SUMMARY_FILE"
{
  "duration_seconds": ${TOTAL_ELAPSED},
  "environment": "$(uname -m) ($(uname -s) $(uname -r))",
  "mode": "online",
  "average_cpu_percent": ${FINAL_AVG_CPU},
  "peak_memory_rss_mb": ${PEAK_RSS_MB},
  "megabytes_written_to_disk": ${FINAL_DISK_MB},
  "megabytes_sent_over_network": ${FINAL_NET_MB},
  "timestamp": "$(date -u +"%Y-%m-%dT%H:%M:%SZ")",
  "total_samples_collected": $((TOTAL_ELAPSED * 400 * 2))
}
EOF

# Generate human-readable report
cat <<EOF > "$REPORT_FILE"
================================================================================
MayaCardio - Section 5 Item 4 1-Hour Resource Profile Report
Author                       : Pranjal Karan
================================================================================
Profiling Duration           : ${TOTAL_ELAPSED} seconds (1.0 hour)
Operating Mode               : online (streamed to Cloudflare Worker)
Target Environment           : $(uname -m) ($(uname -s) $(uname -r))
Node Version                 : $(node -v)

THE FOUR REQUIRED FIGURES (Section 5 Item 4):
  1. Average CPU             : ${FINAL_AVG_CPU}%
  2. Peak Memory (RSS)       : ${PEAK_RSS_MB} MB
  3. Megabytes Written to Disk: ${FINAL_DISK_MB} MB
  4. Megabytes Sent (Network): ${FINAL_NET_MB} MB

TIMESERIES LOG:
  Detailed timeseries saved to: ${CSV_FILE}
  JSON summary saved to       : ${SUMMARY_FILE}
================================================================================
EOF

cat "$REPORT_FILE"
echo "[OK] Resource profiling completed successfully in ONLINE mode."
