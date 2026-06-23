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
# memory-report.sh — Compute and report kubelet memory stats from a CSV
#                    produced by memory-monitor.sh.
#
# Emits a Markdown table to stdout AND appends it to $GITHUB_STEP_SUMMARY
# if that env var is set.
#
# Usage:
#   bash hack/e2e/memory-report.sh [input-csv]
#
# Default input-csv: /tmp/kubelet-memory.csv
set -euo pipefail

INPUT="${1:-/tmp/kubelet-memory.csv}"

if [[ ! -f "$INPUT" ]]; then
  echo "No memory data found at $INPUT — skipping report"
  exit 0
fi

REPORT=$(awk -F, '
NR == 1 { next }           # skip header
$2 + 0 <= 0 { next }       # skip zero/empty rows
{
  rss = $2 + 0
  sum += rss
  count++
  if (rss > peak) peak = rss
  if (min == 0 || rss < min) min = rss
}
END {
  if (count == 0) {
    print "No samples collected."
    exit
  }
  avg = sum / count
  printf "### Kubelet Memory Usage\n\n"
  printf "| Metric | Value |\n"
  printf "|--------|-------|\n"
  printf "| Samples | %d |\n", count
  printf "| Min RSS | %.1f MiB |\n", min / 1024
  printf "| Average RSS | %.1f MiB |\n", avg / 1024
  printf "| Peak RSS | %.1f MiB |\n", peak / 1024
}' "$INPUT")

echo "$REPORT"

if [[ -n "${GITHUB_STEP_SUMMARY:-}" ]]; then
  echo "$REPORT" >> "$GITHUB_STEP_SUMMARY"
fi
