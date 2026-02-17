# Architecture
This document describes the high-level architecture of the Spark API-Driven ETL Framework. 

## Overview
Performing parallel and concurrent HTTP requests to RESTful API endpoints in Spark is not trivial due to challenges around managing distributed compute architecture. Most implementations use suboptimal approaches:

* **Driver-only requests** — defeats Spark's parallelism and does not scale for large datasets
* **Multithreading with `requests` library** — achieves concurrency but uses blocking I/O, substantially increasing time between requests

This design maximizes API request throughput by layering concurrency at multiple levels:

1) **Cluster-level parallelism**: Spark distributes partitions across worker nodes
2) **Partition-level concurrency**: asyncio.Queue enables concurrent processing of multiple rows within each partition with backpressure control
3) **Request-level concurrency**: aiohttp's non-blocking I/O allows the event loop to handle multiple in-flight HTTP requests simultaneously on each worker

The framework supports common HTTP authentication mechanisms, including OAuth2.0 and mTLS. Request/response processing is handled through a **middleware layer** for payload transformation and a **transport layer** for HTTP execution.

The framework is organized into three architectural layers:

| Layer            | Responsibility                                                                 |
| ---------------- | ------------------------------------------------------------------------------ |
| Pipeline Config  | Declarative pipeline definition via YAML/JSON with Pydantic validation         |
| Driver-side      | Orchestration, batching, driver-side authentication, and resource distribution |
| Executor-side    | Concurrent request execution, middleware processing, worker-side authentication|

### Batch Processing and Idempotency

All pipelines process data in configurable batches (default: 10000 rows). This design choice enforces idempotency:

- **Checkpoint tracking**: Each batch completion is recorded, enabling safe pipeline restarts
- **Partial failure recovery**: Failed batches can be retried without reprocessing successful ones
- **Memory management**: Bounded batch sizes prevent executor memory exhaustion
- **Progress visibility**: Batch-level metrics provide granular execution monitoring

Larger batches reduce overhead but increase memory usage and retry cost. Smaller batches provide finer-grained checkpointing at the cost of more Spark actions.

### Key Design Principles

- **Factory-based composition**: All components (transport, auth, middleware) are instantiated via serializable factories, ensuring they work across Spark's distributed boundaries
- **Extensible design**: Abstract interfaces and dependency injection enable custom authentication, middleware, and transport implementations without modifying framework code
- **Process-scoped resources**: HTTP sessions and connections are reused across partition executions on the same worker for efficiency
- **Separation of concerns**: Authentication, retry logic, logging, and transport are decoupled through middleware layers
- **Idempotency**: Required batch processing with checkpointing ensures pipeline resilience and safe re-execution after failures

### Architecture Diagrams

The following sections provide visual representations of the framework:
- **[Driver-Side Execution](#driver-side-exectuion)** - Driver → Workers → Response collection
- **[Worker-Side Exectuion](#worker-side-execution)** - Row processing, middleware, and transport
- **[Concurrent Request Processing](#concurrent-request-processing)** - Producer/consumer pattern with `asyncio.Queue`
- **[Middleware Pipeline](#middleware-pipeline)** - Injector pattern middleware pipeline

### When to Use This Framework

This framework is ideal for:
- **High-volume API ingestion**: Processing millions of records requiring individual API calls
- **Rate-limited APIs**: Backpressure control prevents overwhelming API endpoints
- **Long-running pipelines**: OAuth2 token refresh and session management for jobs exceeding token lifetimes
- **Complex authentication**: Built-in support for OAuth2, mTLS, and custom auth patterns

**Not recommended for:**
- Systems with native Spark connectors (BigQuery, Snowflake, Kafka) - use the connector instead
- APIs offering bulk export files (CSV/Parquet downloads) - download directly
- Single-request extractions - Python `requests` library is simpler
- APIs with per-second rate limits incompatible with any concurrency

## Pipeline Flow
```mermaid
flowchart TB
    subgraph config["Pipeline Configuration"]
        direction TB
        yaml["YAML/JSON Config"]
        secrets["Secret Preprocessing"]
        validation["Pydantic Validation"]
        
        yaml --> secrets --> validation
    end
    
    subgraph driver["Driver-Side Orchestration"]
        direction TB
        factories["Build Serializable Factories<br/>• Transport<br/>• Auth<br/>• Middleware"]
        auth_mgmt["Authentication Runtime<br/>(e.g., OAuth2 RPC Server)"]
        batching["Required Batching<br/>(Idempotency enforcement)"]
        orchestration["Pipeline Orchestration"]
        
        factories --> auth_mgmt
        auth_mgmt --> batching
        batching --> orchestration
    end
    
    subgraph distribute["Distribution (Per Batch)"]
        direction LR
        source["Source Data<br/>(Batch N)"]
        repartition["Repartition &<br/>Serialize to Workers"]
        
        source --> repartition
    end
    
    subgraph workers["Worker-Side Execution"]
        direction TB
        partitions["Partitions<br/>(Distributed)"]
        executor["ApiPartitionExecutor<br/>(Concurrent Processing)"]
        responses["Response Records"]
        
        partitions --> executor --> responses
    end
    
    subgraph results["Results Collection"]
        direction TB
        collect["Collect Batch Results<br/>Write to Sink"]
        checkpoint["Checkpoint Batch<br/>(Idempotency marker)"]
        
        collect --> checkpoint
    end
    
    config --> driver
    driver --> distribute
    orchestration -.->|"deploy factories"| repartition
    repartition -.->|"distribute"| partitions
    responses --> results
    checkpoint -.->|"next batch"| batching
    
    style config fill:#1168bd,stroke:#0d4884,color:#fff
    style driver fill:#28a745,stroke:#1e7e34,color:#fff
    style distribute fill:#6c757d,stroke:#495057,color:#fff
    style workers fill:#17a2b8,stroke:#117a8b,color:#fff
    style results fill:#dc3545,stroke:#bd2130,color:#fff
    style batching fill:#e67e22,stroke:#d35400,color:#fff
    style checkpoint fill:#e67e22,stroke:#d35400,color:#fff
```

### Driver-Side Execution
```mermaid
flowchart TB
    subgraph driver["Driver-Side Orchestration"]
        direction TB
        
        config_in["Validated Config<br/>(Pydantic Models)"]
        
        subgraph factory_build["Factory Construction"]
            direction TB
            transport_factory["Transport Factory<br/>(Serializable Callable)"]
            endpoint_factory["Endpoint Factory<br/>(RequestContext builder)"]
            middleware_factory["Middleware Factories<br/>(List of Callables)"]
        end
        
        subgraph auth_runtime["Authentication Runtime"]
            direction TB
            check{"Auth Type?"}
            simple["Static/API Key<br/>(No runtime needed)"]
            oauth["OAuth2 RPC Server<br/>(Background token refresh)"]
            mtls["mTLS Setup<br/>(Certificate loading)"]
            
            check -->|"static"| simple
            check -->|"oauth2"| oauth
            check -->|"mtls"| mtls
        end
        
        subgraph batch_mgmt["Batch Processing (Required)"]
            direction TB
            source["Source DataFrame"]
            batch_split["Split into Batches<br/>(batch_size from config)"]
            batch_loop["For each batch"]
            check_complete{"All batches<br/>processed?"}
            
            source --> batch_split --> batch_loop
            batch_loop --> check_complete
            check_complete -->|"No"| batch_loop
            check_complete -->|"Yes"| complete["Pipeline Complete"]
        end
        
        subgraph orchestrate["Batch Orchestration"]
            direction TB
            create_executor["Create ApiPartitionExecutor<br/>(with factories)"]
            map_fn["Build mapPartitions function"]
            execute["df.rdd.mapPartitions(fn)"]
            collect_batch["Collect batch results"]
            checkpoint["Checkpoint batch<br/>(Idempotency)"]
            
            create_executor --> map_fn --> execute
            execute --> collect_batch --> checkpoint
        end
        
        config_in --> factory_build
        factory_build --> auth_runtime
        auth_runtime --> batch_mgmt
        batch_loop -.->|"current batch"| orchestrate
        checkpoint -.->|"next iteration"| batch_loop
        
        execute -.->|"serialized factories"| workers["To Workers"]
    end
    
    style driver fill:#28a745,stroke:#1e7e34,color:#fff
    style factory_build fill:#1168bd,stroke:#0d4884,color:#fff
    style auth_runtime fill:#20c997,stroke:#17a673,color:#fff
    style orchestrate fill:#6c757d,stroke:#495057,color:#fff
    style workers fill:#dc3545,stroke:#bd2130,color:#fff
    style checkpoint fill:#e67e22,stroke:#d35400,color:#fff
    style batch_mgmt fill:#e67e22,stroke:#d35400,color:#fff
```

### Worker-Side Execution

Each Spark partition executes the following flow:

```mermaid
flowchart TB
    subgraph partition["PARTITION EXECUTION"]
        direction TB
        
        rowin["Input Row Data"]
        rowout["Ouput Row Data"]
        
        template["REQUEST TEMPLATE<br/><br/>• Endpoint URL<br/>• Headers (Accept, Content-Type)<br/>• Method (GET, POST, etc.)"]
        
        subgraph middleware1["MIDDLEWARE CHAIN"]
            direction LR
            M11["M1"]
            M21["M2"]
            M31["M3"]
            MN1["Mn"]

            M11 --> M21 --> M31 -.-> MN1
        end
        
        subgraph transport["TRANSPORT"]
            direction TB
            session["aiohttp Session (process-scoped, connection pooled)<br/><br/>• TCP Connection Pool<br/>• TLS Session Reuse<br/>• DNS Caching"]
        end
        
        response["Response"]

        subgraph middleware2["MIDDLEWARE CHAIN"]
            direction LR
            M12["M1"]
            M22["M2"]
            M32["M3"]
            MN2["Mn"]

            MN2 -.-> M32 --> M22 --> M12
        end
        
        rowin --> template --> middleware1 --> transport --> response --> middleware2 --> rowout
    end
    
    style partition fill:#6c757d,stroke:#495057,color:#fff
    style template fill:#1168bd,stroke:#0d4884,color:#fff
    style middleware1 fill:#28a745,stroke:#1e7e34,color:#fff
    style M11 fill:#20c997,stroke:#17a673,color:#fff
    style M21 fill:#20c997,stroke:#17a673,color:#fff
    style M31 fill:#20c997,stroke:#17a673,color:#fff
    style MN1 fill:#20c997,stroke:#17a673,color:#fff
    style middleware2 fill:#28a745,stroke:#1e7e34,color:#fff
    style M12 fill:#20c997,stroke:#17a673,color:#fff
    style M22 fill:#20c997,stroke:#17a673,color:#fff
    style M32 fill:#20c997,stroke:#17a673,color:#fff
    style MN2 fill:#20c997,stroke:#17a673,color:#fff
    style transport fill:#17a2b8,stroke:#117a8b,color:#fff
    style session fill:#138496,stroke:#0c5460,color:#fff
    style response fill:#dc3545,stroke:#bd2130,color:#fff
```

## Concurrent Request Processing

The `ApiPartitionExecutor` uses an **asyncio producer-consumer pattern** with bounded concurrency to process partition rows in parallel, rather than sequentially.

### Row-level concurrency
The following diagram shows the structural components and data flow:
```mermaid
flowchart TB
    rows["Iterable[Row]<br/>(Partition Input)"]
    output["list[Row]"]

    subgraph partition["Partition Execution"]
        direction TB

        subgraph pattern["Producer/Consumer Pattern"]
            direction TB
            
            producer["🔄 Producer Coroutines:<br/>await queue.put(row)Send N sentinels (None)"]
            
            queue["asyncio.Queue<br>• Bounded backpressure<br>• FIFO ordering<br>• Async-safe"]
            
            subgraph consumers["Consumer Pool"]
                direction LR
                c1["Consumer 1"]
                c2["Consumer 2"]
                c3["Consumer ..."]
                c20["Consumer N"]
            end
            
            producer --> queue
            queue --> consumers
        end
        
        subgraph processing["Each Consumer Loop"]
            direction TB
            pull["row = await queue.get()"]
            check{"row is None?<br>(sentinel)"}
            build["Build RequestContext"]
            execute["await executor.send()"]
            collect["Collect (row, response)"]
            
            pull --> check
            check -->|No| build --> execute --> collect
            collect -.->|loop| pull
            check -->|Yes| done["Break & Return Results"]
        end
        
        gather["await asyncio.gather()<br>Collect all consumer results"]
        flatten["Flatten & Build<br>Output Rows"]
        
        
        consumers -.->|"each run"| processing
        processing --> gather
        gather --> flatten
    end

    
    rows --> producer
    flatten --> output
    
    style partition fill:#6c757d,stroke:#495057,color:#fff
    style pattern fill:#1168bd,stroke:#0d4884,color:#fff
    style producer fill:#28a745,stroke:#1e7e34,color:#fff
    style queue fill:#17a2b8,stroke:#117a8b,color:#fff
    style consumers fill:#20c997,stroke:#17a673,color:#fff
    style c1 fill:#138496,stroke:#0c5460,color:#fff
    style c2 fill:#138496,stroke:#0c5460,color:#fff
    style c3 fill:#138496,stroke:#0c5460,color:#fff
    style c20 fill:#138496,stroke:#0c5460,color:#fff
    style processing fill:#28a745,stroke:#1e7e34,color:#fff
    style gather fill:#dc3545,stroke:#bd2130,color:#fff
```
**Key Components:**

- **Producer**: Feeds rows from the partition iterator into the queue, then sends sentinel values (`None`) to signal completion
- **Queue**: Provides backpressure and ensures thread-safe communication between producer and consumers
- **Consumer Pool**: `concurrency_limit` (default 20) concurrent workers that pull rows, execute requests, and collect responses
- **Gather**: Waits for all consumers to complete and combines their results

### Row Processing Execution Timeline

The following sequence diagram shows how these components interact over time:
```mermaid
sequenceDiagram
    participant Spark as Spark Worker
    participant Sync as sync_process_partition
    participant Async as async_process_partition
    participant Prod as Producer
    participant Q as asyncio.Queue
    participant C1 as Consumer 1
    participant C2 as Consumer 2
    participant CN as Consumer N
    participant API as HTTP APIs
    
    Spark->>Sync: mapPartitions(rows)
    Sync->>Async: asyncio.run()
    
    Async->>Prod: create_task(producer())
    Async->>C1: create_task(consumer())
    Async->>C2: create_task(consumer())
    Async->>CN: create_task(consumer())
    
    Note over Prod,CN: All tasks run concurrently
    
    par Producer feeds queue
        loop For each row
            Prod->>Q: put(row)
        end
        loop Send sentinels
            Prod->>Q: put(None) × N
        end
    and Consumer 1 processes
        loop Until sentinel
            C1->>Q: get()
            Q-->>C1: row
            C1->>API: HTTP request
            API-->>C1: response
            C1->>C1: collect result
        end
    and Consumer 2 processes
        loop Until sentinel
            C2->>Q: get()
            Q-->>C2: row
            C2->>API: HTTP request
            API-->>C2: response
            C2->>C2: collect result
        end
    and Consumer N processes
        loop Until sentinel
            CN->>Q: get()
            Q-->>CN: row
            CN->>API: HTTP request
            API-->>CN: response
            CN->>CN: collect result
        end
    end
    
    Async->>Async: gather(all results)
    Async->>Async: flatten & build rows
    Async-->>Sync: list[Row]
    Sync-->>Spark: return results
```

**Execution Flow:**

1. **Initialization**: Spark calls the synchronous wrapper which starts the async event loop
2. **Task Creation**: Producer and N consumer tasks are created and scheduled concurrently
3. **Parallel Execution**: 
   - Producer feeds rows into the queue as fast as consumers can process
   - Consumers pull rows, make HTTP requests, and collect responses independently
   - The queue provides natural backpressure when consumers are slower than the producer
4. **Completion**: Producer sends sentinel values; consumers exit when they receive sentinels
5. **Collection**: All consumer results are gathered, flattened, and returned to Spark

### Performance Benefits

With `concurrency_limit=20`, a partition of 1000 rows can process up to 20 HTTP requests simultaneously rather than sequentially. This can result in **10-20x throughput improvement** for I/O-bound API calls, with the actual speedup depending on:

- API response times
- Network latency
- Rate limits on the target API
- Available system resources

The concurrency limit prevents overwhelming the target API while maximizing throughput within safe bounds.


## Async `aiohttp` vs Multithreaded `request`

In many distributed HTTP request Spark applications, concurrency is achieved on the Spark worker by pairing multithreading with the `requests` library. However, because the `request` library is synchronous, it creates an I/O blocking condition during each request in which subsequent requests are required to wait until the current process has been completed. I/O blocking causes two unintended problems with this implementation: 

1) thread blocking I/O prevents all other threads from staging the request, forcing iterative processing when concurrent processing is intended
2) adds overhead by the OS due to context switching between threads.

A multithreaded `requests` design performs the following operations:
1) Each thread is assigned an available connection from the connection pool. Unassigned threads wait for a connection in the pool to become available.
2) The active thread blocks the I/O while waiting for an HTTP response.
3) Once the thread receives the response, the I/O is unblocked and the OS must contextually switch to the next thread that has an assigned connection.

In contrast, the `aiohttp` library allows for asynchronous, single threaded, non-blocking I/O processes:
1) Coroutines are assigned available connections from the connection pool.
2) a coroutine makes a request and yields while waiting for the HTTP response, it does not block the I/O for other coroutines.
3) The event loop switches between coroutines without OS context switching -- coroutines switching is handled by the python application.

In a standard application, the overhead introduced by multithreading is relatively small so there is no significant benefit to using `aiohttp` over `requests`. Howeer, in the context of a Spark cluster, where the number of Python processes scale with the number of CPUs assigned to the node, the added latency due to context switching between threads can be substantially large.


```mermaid
flowchart TB
  %% Async aiohttp design on ONE Spark worker node
  %% N = concurrent task slots (cores)
  %% M = coroutine concurrency per task (consumers)

  %% ---------- Styling (hex colors for GitHub Mermaid) ----------
  %% Use light fills + strong strokes; generally readable in light/dark themes.
  classDef spark   fill:#EEF2FF,stroke:#4F46E5,stroke-width:2px,color:#111111;
  classDef process fill:#ECFDF5,stroke:#10B981,stroke-width:2px,color:#111111;
  classDef asyncio fill:#EFF6FF,stroke:#2563EB,stroke-width:2px,color:#111111;
  classDef queue   fill:#FFFBEB,stroke:#EAB308,stroke-width:2px,color:#111111;
  classDef coro    fill:#FFF1F2,stroke:#E11D48,stroke-width:2px,color:#111111;
  classDef http    fill:#F5F3FF,stroke:#7C3AED,stroke-width:2px,color:#111111;
  classDef api     fill:#F1F5F9,stroke:#64748B,stroke-width:2px,color:#111111;

  %% ---------- Diagram ----------
  subgraph Node["Spark Worker Node (VM)"]
    direction TB
    Spark["Spark Executor(s) on node\nTotal task slots = N (cores)"]:::spark

    subgraph Slots["Concurrent Task Slots (N)"]
      direction LR

      subgraph Task1["Task Slot 1 → Python Worker Process 1"]
        direction TB
        TaskProc1["Python worker process"]:::process
        Loop1["asyncio Event Loop<br>(1 OS thread)"]:::asyncio
        P1["Producer coroutine<br>(enqueue rows)"]:::coro
        Q1["asyncio.Queue<br>(rows / backpressure)"]:::queue
        C1["M Consumer coroutines<br>(concurrency = M)"]:::coro
        S1["aiohttp.ClientSession<br>(conn pool + non-blocking sockets)"]:::http

        TaskProc1 --> Loop1 --> P1 --> Q1 --> C1 --> S1
      end

      subgraph Task2["Task Slot 2 → Python Worker Process 2"]
        direction TB
        TaskProc2["Python worker process"]:::process
        Loop2["asyncio Event Loop<br>(1 OS thread)"]:::asyncio
        P2["Producer coroutine"]:::coro
        Q2["asyncio.Queue"]:::queue
        C2["M Consumer coroutines"]:::coro
        S2["aiohttp.ClientSession"]:::http

        TaskProc2 --> Loop2 --> P2 --> Q2 --> C2 --> S2
      end

      subgraph TaskN["Task Slot N → Python Worker Process N"]
        direction TB
        TaskProcN["Python worker process"]:::process
        LoopN["asyncio Event Loop<br>(1 OS thread)"]:::asyncio
        PN["Producer coroutine"]:::coro
        QN["asyncio.Queue"]:::queue
        CN["M Consumer coroutines"]:::coro
        SN["aiohttp.ClientSession"]:::http

        TaskProcN --> LoopN --> PN --> QN --> CN --> SN
      end
    end

    Spark --> Task1
    Spark --> Task2
    Spark --> TaskN
  end

  API["Remote HTTP API"]:::api

  S1 --> API
  S2 --> API
  SN --> API

```


## Middleware Pipeline

Middleware is executed in the order it is configured, wrapping each subsequent middleware:

```mermaid
flowchart TB
    request["Request"]
    
    subgraph mw_a["Middleware A"]
        direction TB
        subgraph mw_b["Middleware B"]
            direction TB
            subgraph mw_c["Middleware C"]
                direction TB
                http_req["HTTP Request"]
                http_resp["HTTP Response"]
                
                http_req --> http_resp
            end
        end
    end
    
    response["Response"]
    
    request --> mw_a
    mw_a --> response
    
    style mw_a fill:#28a745,stroke:#1e7e34,color:#fff
    style mw_b fill:#20c997,stroke:#17a673,color:#fff
    style mw_c fill:#17a2b8,stroke:#117a8b,color:#fff
    style http_req fill:#1168bd,stroke:#0d4884,color:#fff
    style http_resp fill:#dc3545,stroke:#bd2130,color:#fff
    style request fill:#6c757d,stroke:#495057,color:#fff
    style response fill:#6c757d,stroke:#495057,color:#fff
```

This allows middleware to run logic **before and/or after** the HTTP request.
