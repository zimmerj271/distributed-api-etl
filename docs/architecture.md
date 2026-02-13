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

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                        PARTITION EXECUTION                                  │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  ┌──────────────┐                                                          │
│  │   Row Data   │                                                          │
│  └──────┬───────┘                                                          │
│         │                                                                   │
│         ▼                                                                   │
│  ┌──────────────────────────────────────────────────────────────────────┐  │
│  │                      REQUEST TEMPLATE                                 │  │
│  │   • Base URL + Path                                                   │  │
│  │   • Headers (Accept, Content-Type)                                    │  │
│  │   • Method (GET, POST, etc.)                                          │  │
│  └──────────────────────────────────────────────────────────────────────┘  │
│         │                                                                   │
│         ▼                                                                   │
│  ┌──────────────────────────────────────────────────────────────────────┐  │
│  │                      MIDDLEWARE CHAIN                                 │  │
│  │                                                                       │  │
│  │   ┌────────────┐   ┌────────────┐   ┌────────────┐   ┌────────────┐  │  │
│  │   │   Auth     │──▶│   Retry    │──▶│  Logging   │──▶│   Timing   │  │  │
│  │   │ Injection  │   │   Logic    │   │            │   │            │  │  │
│  │   └────────────┘   └────────────┘   └────────────┘   └────────────┘  │  │
│  │                                                                       │  │
│  └──────────────────────────────────────────────────────────────────────┘  │
│         │                                                                   │
│         ▼                                                                   │
│  ┌──────────────────────────────────────────────────────────────────────┐  │
│  │                         TRANSPORT                                     │  │
│  │                                                                       │  │
│  │   ┌─────────────────────────────────────────────────────────────┐    │  │
│  │   │  aiohttp Session (process-scoped, connection pooled)        │    │  │
│  │   │                                                              │    │  │
│  │   │   • TCP Connection Pool                                      │    │  │
│  │   │   • TLS Session Reuse                                        │    │  │
│  │   │   • DNS Caching                                              │    │  │
│  │   └─────────────────────────────────────────────────────────────┘    │  │
│  │                                                                       │  │
│  └──────────────────────────────────────────────────────────────────────┘  │
│         │                                                                   │
│         ▼                                                                   │
│  ┌──────────────┐                                                          │
│  │   Response   │ ──▶  status_code, headers, body, timing, metadata        │
│  └──────────────┘                                                          │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

## Middleware Execution Order

Middleware is executed in the order it is configured, wrapping each subsequent middleware:

```
                    ┌─────────────────────────────────────┐
  Request ─────────▶│           Middleware A              │
                    │  ┌───────────────────────────────┐  │
                    │  │        Middleware B           │  │
                    │  │  ┌─────────────────────────┐  │  │
                    │  │  │     Middleware C        │  │  │
                    │  │  │  ┌───────────────────┐  │  │  │
                    │  │  │  │   HTTP Request    │  │  │  │
                    │  │  │  │    (Transport)    │  │  │  │
                    │  │  │  └─────────┬─────────┘  │  │  │
                    │  │  │            │            │  │  │
                    │  │  │  ┌─────────▼─────────┐  │  │  │
                    │  │  │  │   HTTP Response   │  │  │  │
                    │  │  │  └───────────────────┘  │  │  │
                    │  │  └─────────────────────────┘  │  │
                    │  └───────────────────────────────┘  │
                    └──────────────────┬──────────────────┘
                                       │
  Response ◀───────────────────────────┘
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
