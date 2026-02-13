# Architecture

This document describes the high-level architecture of the Spark API-Driven ETL Framework.

## Overview

The framework is designed around three distinct layers:

| Layer   | Responsibility                    |
| ------- | --------------------------------- |
| Config  | Declarative pipeline definition   |
| Control | Validation, wiring, orchestration |
| Runtime | Executed on Spark workers         |

## Pipeline Flow

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

## Worker-Side Execution Detail

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

## Middleware Execution Order

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
