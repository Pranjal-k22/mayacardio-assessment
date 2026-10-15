#!/usr/bin/env python3
"""
1-hour ONLINE resource measurement for MayaCardio on Raspberry Pi 4.

Reports the four figures required by the assessment:
  - Average CPU (%)
  - Peak memory (MB)
  - MB written to disk
  - MB sent over the network

Usage (as root, so /proc/<pid>/io is readable):
  sudo python3 ./scripts/measure_1h.py --unit pipeline.service --iface wlan0 \
       --status-cmd "./pipeline status"
  sudo python3 ./scripts/measure_1h.py --pid 1234 --iface eth0 --duration 60        # quick dry run

It measures the daemon AND all child worker processes/threads. Restarts are handled (per-PID totals are kept).
Network = bytes sent/received on the specified interface (excluding loopback traffic to the mock).
"""
import argparse, json, os, signal, subprocess, sys, time

CLK = os.sysconf("SC_CLK_TCK") if hasattr(os, "sysconf") else 100
stop = False


def on_sig(*_):
    global stop
    stop = True


def main_pid(unit):
    out = subprocess.run(["systemctl", "show", "-p", "MainPID", "--value", unit],
                         capture_output=True, text=True).stdout.strip()
    return int(out) if out.isdigit() else 0


def children_map():
    m = {}
    if not os.path.exists("/proc"):
        return m
    for d in os.listdir("/proc"):
        if not d.isdigit():
            continue
        try:
            with open(f"/proc/{d}/stat") as f:
                s = f.read()
            rest = s[s.rindex(")") + 2:].split()
            m.setdefault(int(rest[1]), []).append(int(d))
        except Exception:
            pass
    return m


def tree(root):
    if root <= 0:
        return []
    cm, out, todo = children_map(), [], [root]
    while todo:
        p = todo.pop()
        out.append(p)
        todo.extend(cm.get(p, []))
    return out


def read_pid(p):
    """returns (cpu_ticks, rss_kb, hwm_kb, write_bytes) or None"""
    try:
        with open(f"/proc/{p}/stat") as f:
            s = f.read()
        rest = s[s.rindex(")") + 2:].split()
        ticks = int(rest[11]) + int(rest[12])          # utime + stime
        rss = hwm = 0
        with open(f"/proc/{p}/status") as f:
            for line in f:
                if line.startswith("VmRSS:"):
                    rss = int(line.split()[1])
                elif line.startswith("VmHWM:"):
                    hwm = int(line.split()[1])
        wb = 0
        try:
            with open(f"/proc/{p}/io") as f:
                for line in f:
                    if line.startswith("write_bytes:"):
                        wb = int(line.split()[1])
        except PermissionError:
            wb = -1
        return ticks, rss, hwm, wb
    except Exception:
        return None


def net_bytes(iface):
    with open("/proc/net/dev") as f:
        for line in f:
            if line.strip().startswith(iface + ":"):
                x = line.split(":")[1].split()
                return int(x[0]), int(x[8])            # rx, tx
    raise SystemExit(f"interface '{iface}' not found in /proc/net/dev")


def mode_of(cmd):
    try:
        r = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=10)
        return json.loads(r.stdout.strip().splitlines()[0]).get("mode")
    except Exception:
        return None


def main():
    ap = argparse.ArgumentParser(description="1-hour physical resource profiler for MayaCardio")
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--unit", help="systemd service unit name (e.g. pipeline.service)")
    g.add_argument("--pid", type=int, help="Target process PID")
    ap.add_argument("--iface", required=True, help="Network interface to monitor (e.g. wlan0, eth0)")
    ap.add_argument("--duration", type=int, default=3600, help="Measurement duration in seconds (default: 3600)")
    ap.add_argument("--interval", type=float, default=2.0, help="Sampling interval in seconds (default: 2.0)")
    ap.add_argument("--status-cmd", default="", help="Command to sample pipeline mode (e.g. ./pipeline status)")
    ap.add_argument("--out", default=f"measure_{time.strftime('%Y%m%d_%H%M%S')}.json", help="JSON output file path")
    a = ap.parse_args()

    signal.signal(signal.SIGINT, on_sig)
    signal.signal(signal.SIGTERM, on_sig)

    cpu, wbytes, hwm = {}, {}, {}          # per-PID totals (dead PIDs keep their totals)
    peak_rss_kb, samples, io_denied = 0, 0, False
    modes = {}
    rx0, tx0 = net_bytes(a.iface)
    t0 = time.monotonic()
    next_mode = t0
    print(f"Measuring for {a.duration}s (interval {a.interval}s). Ctrl-C ends early and still produces report.", flush=True)

    while not stop and time.monotonic() - t0 < a.duration:
        root = main_pid(a.unit) if a.unit else a.pid
        rss_sum = 0
        for p in tree(root):
            r = read_pid(p)
            if not r:
                continue
            ticks, rss, h, wb = r
            cpu[p] = ticks
            hwm[p] = max(hwm.get(p, 0), h)
            if wb < 0:
                io_denied = True
            else:
                wbytes[p] = wb
            rss_sum += rss
        peak_rss_kb = max(peak_rss_kb, rss_sum)
        samples += 1

        if a.status_cmd and time.monotonic() >= next_mode:
            m = mode_of(a.status_cmd)
            modes[str(m)] = modes.get(str(m), 0) + 1
            next_mode += 60
        el = time.monotonic() - t0
        if samples % int(max(1, 300 / a.interval)) == 0:
            print(f"  {el/60:5.1f} min  rss_now={rss_sum/1024:.0f} MB", flush=True)
        time.sleep(a.interval)

    elapsed = time.monotonic() - t0
    rx1, tx1 = net_bytes(a.iface)
    cpu_s = sum(cpu.values()) / CLK
    core_count = os.cpu_count() or 1
    res = {
        "environment": {
            "machine": os.uname().machine if hasattr(os, "uname") else "unknown",
            "model": open("/proc/device-tree/model").read().strip("\0\n") if os.path.exists("/proc/device-tree/model") else "unknown",
            "cores": core_count,
            "mem_total_mb": int(next((l for l in open("/proc/meminfo") if l.startswith("MemTotal")), "MemTotal: 0 kB").split()[1]) // 1024 if os.path.exists("/proc/meminfo") else 0,
            "kernel": os.uname().release if hasattr(os, "uname") else "unknown",
        },
        "elapsed_s": round(elapsed, 1),
        "samples_taken": samples,
        "avg_cpu_pct_of_one_core": round(cpu_s / max(elapsed, 0.001) * 100, 1),
        "avg_cpu_pct_of_all_cores": round(cpu_s / max(elapsed, 0.001) * 100 / core_count, 1),
        "peak_memory_mb_sampled_rss": round(peak_rss_kb / 1024, 1),
        "peak_memory_mb_sum_of_vmhwm_upper_bound": round(sum(hwm.values()) / 1024, 1),
        "disk_written_mb": None if (io_denied and not wbytes) else round(sum(wbytes.values()) / 1e6, 1),
        "network_sent_mb": round((tx1 - tx0) / 1e6, 2),
        "network_received_mb": round((rx1 - rx0) / 1e6, 2),
        "interface": a.iface,
        "mode_samples_once_per_min": modes or "not checked (--status-cmd not given)",
        "notes": [],
    }
    if io_denied:
        res["notes"].append("/proc/<pid>/io not readable: run as root for disk figures")
    if elapsed < a.duration - 5:
        res["notes"].append("run ended early: figures cover elapsed_s only, not a full hour")
    if modes and set(modes) != {"online"}:
        res["notes"].append("pipeline was not online the whole time: this is NOT a clean ONLINE-mode figure")
    res["notes"].append("disk = write_bytes from /proc/<pid>/io (bytes sent to block layer, includes SQLite WAL + checkpoints)")
    res["notes"].append("network includes all traffic on the interface, not just the pipeline")

    with open(a.out, "w") as f:
        json.dump(res, f, indent=2)
    print(json.dumps(res, indent=2))
    print(f"\nSaved to {a.out}")


if __name__ == "__main__":
    main()
