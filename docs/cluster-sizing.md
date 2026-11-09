# Sizing the Local Spark Cluster

The local Spark cluster is sized with three Makefile parameters. This guide explains what they
mean, how to find out what your machine can support, and how to choose minimum, maximum, and
recommended values.

**Short version:** run `make sizing`. It reads your machine's cores and memory and prints suggested
values for each `make up-*` target. Use the rest of this guide to understand or adjust them.

```bash
make sizing
make up-demo WORKERS=2 WORKER_CORES=3 WORKER_MEMORY=2G
```

## Contents

1. [The three parameters](#1-the-three-parameters)
2. [How Spark uses them](#2-how-spark-uses-them)
3. [Find out what your machine has](#3-find-out-what-your-machine-has)
4. [Memory used by everything else](#4-memory-used-by-everything-else)
5. [Choosing values](#5-choosing-values)
6. [Example configurations](#6-example-configurations)
7. [Checking the result](#7-checking-the-result)
8. [Symptoms and fixes](#8-symptoms-and-fixes)

---

## 1. The three parameters

| Parameter | Default | What it is |
|---|---|---|
| `WORKERS` | `2` | Number of Spark worker containers. Think of each one as a separate machine in the cluster. |
| `WORKER_CORES` | `3` | Number of **task slots** each worker offers to Spark: how many tasks it runs at once. |
| `WORKER_MEMORY` | `2G` | Memory each worker can hand out to the Spark drivers and executors it runs. |

Pass them to any `make up-*` target:

```bash
make up-demo WORKERS=3 WORKER_CORES=2 WORKER_MEMORY=3G
```

Things to know:

- **They can be changed while the stack is running.** Run `make up-*` again with new values and
  Docker Compose recreates only the worker containers. Jobs running on those workers are killed,
  so resize when the cluster is idle.
- **Make does not remember previous values.** Every `make up-*` call that leaves out a parameter
  uses its default. If you started with `WORKER_MEMORY=4G` and later run
  `make up-demo WORKERS=3`, the workers go back to `2G`. Pass every non-default value each time.
- **`make restart` keeps the current values.** It restarts the existing containers in place.
- **The defaults assume a machine with 8 physical cores and 16 GB of memory available to
  Docker.** They give 6 task slots (2 × 3) and 4 GB of worker memory (2 × 2G), which is the
  smallest cluster that runs every demo, including the parallel DAG.

## 2. How Spark uses them

### Cores are task slots, not reserved CPUs

`WORKER_CORES` is a number the worker *advertises* to the Spark master. Docker does not pin
CPUs to containers here: every worker can use every CPU on the host. Two consequences:

- You *can* advertise more slots in total than the machine has cores. Nothing stops you.
- Whether that helps depends on the work. These pipelines spend most of their time waiting for
  HTTP responses, so a moderate amount of oversubscription is fine. CPU-heavy steps (JSON parsing,
  Delta writes) slow down once the total exceeds your physical cores.

### Memory is a budget, not a limit

`WORKER_MEMORY` is how much memory the worker is *allowed to allocate* to Spark drivers and
executors. Docker does not cap the container at that value, and the container uses more than it:

- the worker's own JVM (~0.3 GB),
- JVM overhead on top of each driver/executor heap,
- the Python processes PySpark starts for each running task.

As a rule of thumb, plan for **`WORKER_MEMORY` + 1 GB** of real memory per busy worker. If the
total exceeds what Docker has, the Linux out-of-memory killer stops containers.

### Where the Spark driver runs

Every Spark application has one **driver** (the process that plans the work) and one or more
**executors** (the processes that run tasks). Where the driver runs depends on how the job starts,
and that changes how much of the cluster a job needs:

| How the job starts | Where the driver runs | Uses worker resources for |
|---|---|---|
| Airflow DAG (`make dag-trigger`, Airflow UI) | **On one of the workers** (cluster deploy mode) | Driver (1 slot, 1g; 512m in the parallel DAG) **and** executors |
| `make spark-shell` / `make spark-sql` / `make spark-submit` | In the `spark-master` container | Executors only (2g each, from `docker/spark/spark-defaults.conf`) |
| Jupyter notebooks | In the `jupyter` container | **Nothing** (see note) |

> **Note: Jupyter notebooks do not use the cluster.** The notebooks currently run Spark in local mode
> (`local[*]`) inside the Jupyter container. The Jupyter image (Spark 3.5.0, Python 3.11) does not
> match the cluster (Spark 3.5.1, Python 3.10), so it can't send work to the workers. The
> notebooks run the same pipeline code and write to the same tables, but cluster sizing doesn't
> affect them. Their memory comes from the Jupyter container (up to ~2.5 GB while a notebook
> runs). Use the Airflow DAGs to see the cluster at work.

### Applications hold their cores until they finish

In this standalone cluster, an application keeps its executors (and their slots) until it finishes.
Unless an application sets `spark.cores.max`, it takes **every free slot**. In practice:

- An open `make spark-shell` or `make spark-sql` session holds all free slots, and Airflow jobs
  wait until you exit it.
- The parallel DAG caps each job at 1 executor core (`spark.cores.max=1`) so that 5 jobs can share
  the cluster.

### What the demos need

| Workload | Minimum | Recommended |
|---|---|---|
| One Airflow demo DAG | 2 slots and 2G on one worker (driver 1 slot/1g + executor 1 slot/1g). Tested: `WORKERS=1 WORKER_CORES=2 WORKER_MEMORY=2G`. | Defaults |
| `demo_all_pipelines_parallel` (5 jobs) | **6 slots** and 3G in total. Each job needs 1 slot for its driver plus 1 for its executor; with 5 slots or fewer, the 5 drivers take every slot and the DAG **hangs forever**. | 10 slots and 5G in total to run all 5 jobs at the same time |
| `make spark-shell` / `spark-sql` | 1 free slot and 2G on one worker | Defaults |
| Jupyter notebooks | Not affected by cluster size | — |

Measured on the secondary test machine (see the README), using `make up-demo` and the parallel DAG:

| Cluster | Slots | Result | Wall time | Lowest free memory |
|---|---|---|---|---|
| `WORKERS=2 WORKER_CORES=2 WORKER_MEMORY=2G` | 4 | **Hangs** (all apps WAITING) | — | 7.5 GB |
| `WORKERS=2 WORKER_CORES=3 WORKER_MEMORY=2G` (default) | 6 | Succeeds, up to 2 jobs at once | 106 s | ~5 GB |
| `WORKERS=2 WORKER_CORES=5 WORKER_MEMORY=3G` | 10 | Succeeds, all 5 jobs at once | 90 s | **1.7 GB** (tight on 16 GB) |

## 3. Find out what your machine has

### Physical cores

```bash
lscpu | grep -E '^CPU\(s\)|Thread\(s\) per core|Core\(s\) per socket|Socket\(s\)'
```

Physical cores = `Core(s) per socket` × `Socket(s)`.

Don't use `nproc` or Docker's CPU count for this. They count **logical CPUs**, which is double the
physical core count on CPUs with Hyper-Threading / SMT (`Thread(s) per core: 2`). Two hardware
threads on one core share that core's execution units and together deliver roughly 1.2–1.3× one
core, not 2×. Size the cluster against physical cores; treat the extra logical CPUs as bonus
capacity for I/O-bound work.

**Hybrid CPUs (Intel 12th gen and later).** Many recent Intel CPUs mix fast performance cores
(P-cores) with slower efficient cores (E-cores). Linux schedules containers across all of them, so
some tasks run slower than others. This shows up as uneven task times in the Spark UI, not as
errors. For example, the Intel Core Ultra 7 265H reports 16 physical cores (6 P-cores, 8 E-cores,
2 low-power E-cores) and has no Hyper-Threading; it behaves more like 9–10 P-cores.

### Memory available to Docker

```bash
docker info --format '{{.NCPU}} CPUs, {{.MemTotal}} bytes'
free -h
```

`MemTotal` is the ceiling for the whole stack. On native Linux it's the machine's RAM, shared with
your desktop and browser. On WSL2 it's the WSL virtual machine's limit (see below).

### WSL2 memory limit

By default WSL2 gives Linux **50% of the Windows host's RAM** (for example, 16 GB on a 32 GB
laptop). Within that cap, memory is used on demand and given back to Windows over time. To raise
the cap, create or edit `%UserProfile%\.wslconfig` on Windows:

```ini
[wsl2]
memory=24GB
```

Then run `wsl --shutdown` from PowerShell and reopen your WSL terminal. Leave Windows at least
6–8 GB.

## 4. Memory used by everything else

The non-worker services use a roughly fixed amount of memory that depends on which profiles you
start. Measured with `docker stats`:

| Service group | Idle | Planning figure |
|---|---|---|
| Core (`make up`): postgres, minio, hive-metastore, spark-master, airflow-webserver, airflow-scheduler | ~2.2 GB | **2.5 GB** |
| + Jupyter (`jupyter` profile) | ~0.1 GB idle, **~2.5 GB while a notebook runs** | **2.5 GB** |
| + Keycloak and mock API (`keycloak` profile) | ~1.6 GB | **2 GB** |
| + Spark History Server (`history` profile) | not measured | 0.5 GB |

| Target | Planning baseline |
|---|---|
| `make up` | 2.5 GB |
| `make up-jupyter` | 5 GB |
| `make up-demo` | 7 GB |
| `make up-all` | 7.5 GB |

Also leave headroom for the operating system: about **4 GB** on a Linux desktop, and about
**2 GB** on WSL2 (Windows' own memory is outside the WSL cap).

## 5. Choosing values

### Cores

```
WORKERS × WORKER_CORES ≤ physical cores − 2
```

The 2 cores are left for Airflow, Hive, Postgres, and the Spark master. Going over is allowed (see
[Cores are task slots](#cores-are-task-slots-not-reserved-cpus)) but gives diminishing returns.

- **Minimum:** 2 slots for a single Airflow demo; 6 for the parallel DAG.
- **Maximum:** about 1.5–2× physical cores for this I/O-bound workload. Beyond that you only add
  scheduling overhead.
- **Recommended:** fill the budget above, using 2–4 cores per worker.

### Memory

```
WORKERS × (WORKER_MEMORY + 1G) ≤ Docker memory − OS headroom − baseline for your target
```

- **Minimum:** `WORKER_MEMORY=2G`. The `spark-shell`/`spark-sql` executors ask for 2g, and a
  single Airflow demo needs 1g for its driver plus 1g for its executor.
- **Maximum:** whatever the formula allows. Don't go over it: unlike cores, running out of
  memory causes failures rather than slowdowns.
- **Recommended:** 2–4G per worker. The demos process small batches, so more memory per worker
  rarely helps them.

### Workers vs. cores per worker

The same number of slots can come from many small workers or a few large ones. On a single machine,
**prefer a few workers with 2–4 cores each**:

- Every worker adds a worker JVM, executor JVMs, and Python processes (memory you pay for
  without getting more CPU, since all workers share the same CPUs).
- With 1-core workers, a worker that hosts an Airflow driver has no slot left for an executor.
- More workers *are* useful for seeing how Spark spreads work across machines (for example, in the
  Spark UI). That's a valid reason to run `WORKERS=4 WORKER_CORES=1`, but it isn't faster.

### Worked example: the default machine (8 cores, 16 GB, WSL2), `make up-demo`

- Cores: 8 − 2 = 6 slots → `WORKERS=2 WORKER_CORES=3`.
- Memory: 16 GB − 2 GB (WSL2 headroom) − 7 GB (`up-demo` baseline) = 7 GB →
  2 × (2G + 1G) = 6 GB ✓ → `WORKER_MEMORY=2G`.
- Result: the defaults. 6 slots runs the parallel DAG, partly queued.

On a 16 GB **native Linux desktop**, the 4 GB OS headroom leaves only 5 GB, so `make sizing`
suggests one larger worker (`WORKERS=1 WORKER_CORES=6 WORKER_MEMORY=4G`) instead. The defaults
still run, with less headroom; close memory-heavy applications while running the demos.

## 6. Example configurations

| Machine (available to Docker) | Target | Command | Notes |
|---|---|---|---|
| 8 cores / 16 GB (default) | `up-demo` | `make up-demo` | 6 slots; parallel DAG runs partly queued |
| 8 cores / 16 GB | `up` | `make up WORKER_MEMORY=3G` | No Jupyter or Keycloak, so there's memory to spare |
| 8 cores / 32 GB | `up-demo` | `make up-demo WORKER_MEMORY=4G` | Same slots; more memory headroom |
| 8 cores / 32 GB | `up-demo`, all 5 parallel jobs at once | `make up-demo WORKER_CORES=5 WORKER_MEMORY=3G` | 10 slots oversubscribes 6 cores; fine for this I/O-bound demo |
| 16 cores / 64 GB | `up-demo` | `make up-demo WORKERS=4 WORKER_CORES=3 WORKER_MEMORY=4G` | 12 slots; all 5 parallel jobs at once |
| Any | To see work spread across many workers | `make up-demo WORKERS=4 WORKER_CORES=2 WORKER_MEMORY=2G` | Check the memory formula first |

`make sizing` reproduces this reasoning for your machine. To preview another machine:

```bash
SIZING_CORES=8 SIZING_MEM_GIB=32 SIZING_PLATFORM=Linux make sizing
```

## 7. Checking the result

1. **Spark master UI** at http://localhost:8080. The top of the page shows the number of alive
   workers, total cores, and total memory; the Workers table shows each worker's cores and memory.
   With the defaults, you should see `Alive Workers: 2`, `Cores in use: 6 Total`, and
   `Memory in use: 4.0 GiB Total`.
   While jobs run, *Running Applications* shows how many cores each one holds.
2. **Real memory use:** `docker stats --no-stream`.
3. **Worker containers:** `make ps` lists `distributed-api-etl-spark-worker-1` … `-N`.

## 8. Symptoms and fixes

| Symptom | Likely cause | Fix |
|---|---|---|
| Application shows **WAITING** in the Spark UI; driver log says `Initial job has not accepted any resources` | Not enough free slots or memory for an executor. For the parallel DAG: fewer than 6 slots. | Add slots or memory, or wait for (or exit) the application holding them, such as an open `spark-shell` |
| `demo_all_pipelines_parallel` runs forever, all apps WAITING with 0 cores | ≤ 5 slots: the 5 drivers took every slot | `WORKERS × WORKER_CORES ≥ 6` |
| Containers restart, or `make ps` shows `Exited (137)` | Out of memory | Lower `WORKERS` or `WORKER_MEMORY`, use a smaller target (`make up` instead of `make up-demo`), or raise the WSL2 limit |
| Everything slow, disk busy, desktop sluggish | The host is swapping | Same as above |
| Some tasks much slower than others | Hybrid CPU (P-cores and E-cores), or more slots than physical cores | Expected; reduce slots if it matters |
| Worker shows in the UI with fewer cores/memory than you asked for | A later `make up-*` call without your values reset them to the defaults | Pass every non-default value on each call |
| After resizing during a run, a re-run fails with `DELTA_CREATE_TABLE_WITH_NON_EMPTY_LOCATION` | Resizing killed a job mid-write, leaving files without a table | `make clean-volumes` (deletes all demo tables), then `make up-demo` |
