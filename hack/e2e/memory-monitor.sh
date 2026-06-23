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
# memory-monitor.sh — Background RSS sampler for the kube-air kubelet.
#
# Reads the kubelet PID from a file and periodically samples VmRSS from
# /proc/<pid>/status, writing timestamp+rss pairs to a CSV. Run as a
# background process; send SIGTERM or SIGINT to stop.
#
# Usage:
#   bash hack/e2e/memory-monitor.sh [pid-file] [output-csv] [interval-seconds]
#
# Defaults:
#   pid-file         /tmp/kubelet.pid
#   output-csv       /tmp/kubelet-memory.csv
#   interval-seconds 2
set -euo pipefail

PID_FILE="${1:-/tmp/kubelet.pid}"
OUTPUT="${2:-/tmp/kubelet-memory.csv}"
INTERVAL="${3:-2}"

echo "timestamp_s,rss_kb" > "$OUTPUT"

cleanup() { exit 0; }
trap cleanup SIGTERM SIGINT

while true; do
  # Wait for PID file to appear (kubelet may not have started yet)
  if [[ ! -f "$PID_FILE" ]]; then
    sleep "$INTERVAL"
    continue
  fi

  PID=$(cat "$PID_FILE")

  # Stop if the kubelet process has gone away
  if [[ ! -f "/proc/$PID/status" ]]; then
    break
  fi

  RSS=$(grep -m1 '^VmRSS:' "/proc/$PID/status" 2>/dev/null | awk '{print $2}' || true)
  if [[ -n "$RSS" ]]; then
    echo "$(date +%s),$RSS" >> "$OUTPUT"
  fi

  sleep "$INTERVAL"
done
