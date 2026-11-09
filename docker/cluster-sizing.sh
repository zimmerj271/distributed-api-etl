#!/usr/bin/env bash
# ============================================================
# Suggest WORKERS / WORKER_CORES / WORKER_MEMORY for this machine.
# Run via `make sizing`. The method is explained in docs/cluster-sizing.md;
# keep the constants below in sync with that document.
# ============================================================
set -euo pipefail

# Measured memory (GiB) used by the non-worker services, per profile.
# Includes headroom for the services' peak usage during demo runs.
BASE_CORE=2.5      # postgres, minio, hive-metastore, spark-master, airflow
BASE_JUPYTER=2.5   # jupyter, including a running notebook's local Spark session
BASE_KEYCLOAK=2.0  # keycloak + mock-api
BASE_HISTORY=0.5   # spark-history-server (estimate)

WORKER_OVERHEAD=1  # GiB per worker on top of WORKER_MEMORY (JVMs + Python workers)
RESERVED_CORES=2   # cores left for the non-worker services
CORES_PER_WORKER=3 # preferred task slots per worker
MIN_MEM=2          # GiB; spark-defaults.conf gives executors 2g
MAX_MEM=4          # GiB; more than this rarely helps the demos

# ---------- detect resources ----------
if command -v lscpu >/dev/null 2>&1; then
    physical_cores=$(lscpu -p=CORE,SOCKET | grep -v '^#' | sort -u | wc -l)
else
    physical_cores=$(nproc)
fi
logical_cpus=$(nproc)

if docker info >/dev/null 2>&1; then
    mem_bytes=$(docker info --format '{{.MemTotal}}')
    mem_source="docker info"
else
    mem_bytes=$(awk '/MemTotal/ {print $2 * 1024}' /proc/meminfo)
    mem_source="/proc/meminfo (Docker is not running)"
fi
mem_gib=$(awk -v b="$mem_bytes" 'BEGIN {printf "%.1f", b / 1024^3}')

if grep -qi microsoft /proc/version 2>/dev/null; then
    platform="WSL2"
    headroom=2   # the WSL VM only runs Linux; Windows memory is outside the cap
else
    platform="Linux"
    headroom=4   # OS, desktop, browser
fi

# Optional overrides to preview other machines, e.g.
#   SIZING_CORES=8 SIZING_MEM_GIB=16 SIZING_PLATFORM=Linux make sizing
physical_cores=${SIZING_CORES:-$physical_cores}
if [[ -n "${SIZING_MEM_GIB:-}" ]]; then mem_gib=$SIZING_MEM_GIB; mem_source="SIZING_MEM_GIB override"; fi
if [[ -n "${SIZING_PLATFORM:-}" ]]; then
    platform=$SIZING_PLATFORM
    [[ "$platform" == "WSL2" ]] && headroom=2 || headroom=4
fi

core_budget=$(( physical_cores - RESERVED_CORES ))
(( core_budget < 1 )) && core_budget=1

# ---------- report ----------
echo "Detected resources"
echo "  Platform:        $platform"
echo "  Physical cores:  $physical_cores (logical CPUs: $logical_cpus)"
echo "  Memory:          ${mem_gib} GiB (from $mem_source)"
echo "  Reserved:        ${RESERVED_CORES} cores for services, ${headroom} GiB headroom for the OS"
if [[ "$platform" == "WSL2" ]]; then
    echo "  Note: WSL2 gives Linux 50% of Windows RAM by default; raise it in %UserProfile%\\.wslconfig"
fi
echo ""
printf "%-11s %-10s %-42s %-6s %s\n" "Target" "Baseline" "Suggested parameters" "Slots" "Parallel DAG"
printf "%-11s %-10s %-42s %-6s %s\n" "------" "--------" "--------------------" "-----" "------------"

suggest() {
    local target=$1 base=$2
    awk -v mem="$mem_gib" -v head="$headroom" -v base="$base" \
        -v budget="$core_budget" -v cpw="$CORES_PER_WORKER" -v ovh="$WORKER_OVERHEAD" \
        -v minm="$MIN_MEM" -v maxm="$MAX_MEM" -v target="$target" '
    BEGIN {
        mem_budget = mem - head - base
        cores = (budget < cpw) ? budget : cpw
        by_cores = int(budget / cores)
        by_mem = int(mem_budget / (minm + ovh))
        workers = (by_cores < by_mem) ? by_cores : by_mem
        if (workers < 1) {
            printf "%-11s %-10s %s\n", target, base " GiB", "not enough memory (need ~" base + head + minm + ovh " GiB)"
            exit
        }
        # When memory limits the worker count, give each worker more slots so the
        # cluster still has the 6 slots the parallel DAG needs (if cores allow).
        if (workers * cores < 6) {
            want = int((6 + workers - 1) / workers)
            fit = int(budget / workers)
            cores = (want < fit) ? want : fit
        }
        m = int(mem_budget / workers) - ovh
        if (m > maxm) m = maxm
        if (m < minm) m = minm
        slots = workers * cores
        total_mem = workers * m
        # Parallel DAG: each of 5 jobs needs 1 driver slot + 1 executor slot,
        # 512m each. 6 slots / 3G is the minimum; 10 slots / 5G runs all 5 at once.
        if (target == "up" || target == "up-jupyter") dag = "n/a (needs keycloak)"
        else if (slots >= 10 && total_mem >= 5) dag = "all 5 jobs at once"
        else if (slots >= 6 && total_mem >= 3) dag = "runs, partly queued"
        else dag = "HANGS (needs >= 6 slots)"
        params = sprintf("WORKERS=%d WORKER_CORES=%d WORKER_MEMORY=%dG", workers, cores, m)
        printf "%-11s %-10s %-42s %-6d %s\n", target, base " GiB", params, slots, dag
    }'
}

add() { awk -v a="$1" -v b="$2" 'BEGIN {print a + b}'; }

suggest "up"         "$BASE_CORE"
suggest "up-jupyter" "$(add "$BASE_CORE" "$BASE_JUPYTER")"
suggest "up-demo"    "$(add "$(add "$BASE_CORE" "$BASE_JUPYTER")" "$BASE_KEYCLOAK")"
suggest "up-all"     "$(add "$(add "$(add "$BASE_CORE" "$BASE_JUPYTER")" "$BASE_KEYCLOAK")" "$BASE_HISTORY")"

echo ""
echo "Example: make up-demo WORKERS=2 WORKER_CORES=3 WORKER_MEMORY=2G"
echo "These are starting points. See docs/cluster-sizing.md for the reasoning and trade-offs."
