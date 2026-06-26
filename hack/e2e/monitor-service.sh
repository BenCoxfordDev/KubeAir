#!/usr/bin/env bash
#
# Copyright 2026 Ben Coxford.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# monitor-service.sh — Monitor CPU and memory usage of a systemd service over time.
#
# Usage: ./monitor-service.sh <service-name> [interval_seconds] [output_csv]
#
# Examples:
#
# start monitoring:
#   ./hack/e2e/monitor-service.sh kubelet 5 /tmp/kubelet-stats.csv &
#   echo $! > /tmp/monitor.pid
#
# stop it:
#   kill $(cat /tmp/monitor.pid)

set -euo pipefail

SERVICE="${1:-}"
INTERVAL="${2:-5}"
OUTPUT="${3:-}"

if [[ -z "$SERVICE" ]]; then
    echo "Usage: $0 <service-name> [interval_seconds] [output_csv]" >&2
    exit 1
fi

# Strip .service suffix if provided
SERVICE="${SERVICE%.service}"

if ! systemctl is-active --quiet "${SERVICE}.service" 2>/dev/null; then
    echo "Warning: service '${SERVICE}.service' does not appear to be active." >&2
fi

# Get the main PID of the service
get_main_pid() {
    systemctl show -p MainPID --value "${SERVICE}.service" 2>/dev/null || echo "0"
}

# Collect all PIDs belonging to the service cgroup
get_service_pids() {
    local main_pid
    main_pid=$(get_main_pid)
    if [[ "$main_pid" == "0" || -z "$main_pid" ]]; then
        echo ""
        return
    fi
    # Try to get all pids from the cgroup (systemd v232+)
    local cgroup_file="/sys/fs/cgroup/system.slice/${SERVICE}.service/cgroup.procs"
    if [[ -r "$cgroup_file" ]]; then
        tr '\n' ' ' < "$cgroup_file"
    else
        # Fallback: just the main PID
        echo "$main_pid"
    fi
}

# Sum RSS memory (kB) across all PIDs
get_memory_kb() {
    local pids="$1"
    local total=0
    for pid in $pids; do
        if [[ -r "/proc/${pid}/status" ]]; then
            local rss
            rss=$(awk '/^VmRSS:/{print $2}' "/proc/${pid}/status" 2>/dev/null || echo 0)
            total=$(( total + ${rss:-0} ))
        fi
    done
    echo "$total"
}

# Sum CPU time (seconds) across all PIDs using /proc/<pid>/stat
get_cpu_jiffies() {
    local pids="$1"
    local total=0
    for pid in $pids; do
        if [[ -r "/proc/${pid}/stat" ]]; then
            read -r -a fields < "/proc/${pid}/stat" 2>/dev/null || continue
            # fields[13]=utime, fields[14]=stime (0-indexed)
            total=$(( total + ${fields[13]:-0} + ${fields[14]:-0} ))
        fi
    done
    echo "$total"
}

CLK_TCK=$(getconf CLK_TCK 2>/dev/null || echo 100)

HEADER="timestamp,service,pid_count,cpu_percent,mem_rss_mb,mem_rss_kb"

if [[ -n "$OUTPUT" ]]; then
    echo "$HEADER" > "$OUTPUT"
fi

prev_jiffies=0
prev_time=0

while true; do
    pids=$(get_service_pids)
    if [[ -z "$pids" ]]; then
        [[ -n "$OUTPUT" ]] && echo "$(date -Iseconds),${SERVICE},0,0.00,0.000,0" >> "$OUTPUT"
        sleep "$INTERVAL"
        continue
    fi

    pid_count=$(echo "$pids" | wc -w | tr -d ' ')
    now=$(date +%s%3N)  # milliseconds

    cur_jiffies=$(get_cpu_jiffies "$pids")

    cpu_percent="0.00"
    if [[ "$prev_time" -ne 0 ]]; then
        elapsed_ms=$(( now - prev_time ))
        delta_jiffies=$(( cur_jiffies - prev_jiffies ))
        # cpu% = (delta_jiffies / CLK_TCK) / (elapsed_ms / 1000) * 100
        cpu_percent=$(awk "BEGIN { printf \"%.2f\", ($delta_jiffies / $CLK_TCK) / ($elapsed_ms / 1000) * 100 }")
    fi

    prev_jiffies="$cur_jiffies"
    prev_time="$now"

    mem_kb=$(get_memory_kb "$pids")
    mem_mb=$(awk "BEGIN { printf \"%.3f\", $mem_kb / 1024 }")

    ts=$(date -Iseconds)
    line="${ts},${SERVICE},${pid_count},${cpu_percent},${mem_mb},${mem_kb}"
    [[ -n "$OUTPUT" ]] && echo "$line" >> "$OUTPUT"

    sleep "$INTERVAL"
done
