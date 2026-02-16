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

#### When to Use This Framework

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

### Driver-Side Execution

```mermaid
flowchart TB
    subgraph driver1["DRIVER SIDE"]
        direction TB
        config["YAML/JSON<br/>Config"]
        preprocess["Preprocess<br/>Secrets"]
        validation["Pydantic<br/>Validation"]
        factories["Runtime Factories<br/>• Transport<br/>• Auth<br/>• Middleware<br/>• Request Context"]
        source["Source Table<br/>or DataFrame"]
        repartition["Repartition &<br/>Serialize to Workers"]
        
        config --> preprocess --> validation --> factories
        source --> repartition
        factories --> repartition
    end
    
    subgraph workers["WORKER SIDE"]
        direction TB
        subgraph partitions[" "]
            direction LR
            p1["Partition 1"]
            p2["Partition 2"]
            pn["Partition N"]
        end
        
        executor["ApiPartitionExecutor<br/>For each row:<br/>1. Build request from template<br/>2. Apply middleware chain<br/>3. Send HTTP request via Transport<br/>4. Collect response"]
        
        responses["Response Records"]
        p1 -.-> executor
        p2 -.-> executor
        pn -.-> executor
        executor --> responses
    end
    
    subgraph driver2["DRIVER SIDE"]
        direction TB
        collect["Collect Results<br/>Write to Sink Table"]
    end
    
    repartition -.->|distribute| p1
    repartition -.->|distribute| p2
    repartition -.->|distribute| pn
    responses --> collect
    
    style driver1 fill:#1168bd,stroke:#0d4884,color:#fff
    style workers fill:#6c757d,stroke:#495057,color:#fff
    style driver2 fill:#1168bd,stroke:#0d4884,color:#fff
    style partitions fill:none,stroke:none
    style executor fill:#28a745,stroke:#1e7e34,color:#fff
    style responses fill:#17a2b8,stroke:#117a8b,color:#fff
    style repartition fill:#dc3545,stroke:#bd2130,color:#fff
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

### Architecture Overview
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

## Middleware Pipeline and Execution

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

## Design Principles

### 1. Spark Safety

* No SparkContext on workers
* Only serializable factories are shipped

### 2. Idempotency First

* Every row carries a `request_id`
* Designed for merge-friendly downstream tables

### 3. Compile, Then Run

* Config is preprocessed, validated, and *compiled* on the driver
* Workers execute only runtime logic

### 4. Clear Abstractions

* Config ≠ Control ≠ Runtime
* Each layer has a single responsibility

### 5. Minimal Magic

* Explicit wiring
* No hidden global state
* No runtime config mutation

### 6. Middleware-Driven Extensibility

* Request behavior is modified via middleware, not hard-coded logic
* Middleware is composable, reusable, and ordered
* New functionality is added without changing the executor or transport layers
