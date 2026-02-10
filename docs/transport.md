# Transport Layer

The **transport layer** is responsible for executing HTTP requests on Spark **worker nodes**. It is intentionally designed as a **low-level**, **runtime-only abstraction** that focuses solely on *network execution* and *connection management*, not request semantics.

## Design Goals

The transport layer is designed to be:

* **Spark-safe**: Fully serializable, no driver-side state
* **Process-scoped (executor-local)**: One transport instance per Python worker process, reused across multiple partitions
* **Efficient and stable**: Connection pooling, warm-up, reuse across partitions to avoid cold-start effects
* **Extensible**: Multiple HTTP engines can be added and supported via factories

All higher-level concerns—authentication, retries, logging, request mutation, response handling—are **explicitly excluded** from the transport layer and are handled by middleware.

## Execution Model

Spark executes user code in long-lived **Python worker processes** on executor nodes. The transport layer is designed around this execution model.

Each Python worker process follows this lifecycle:

1. Spark deserializes the partition execution function into the worker process
2. A `worker-local resource manager` is lazily initialized
3. A single transport instance is created **once per worker process**
4. The transport is reused across:
   * Multiple rows
   * Multiple partitions
5. When the worker process exits, asynchronous resources are shut down gracefully

This design intentionally avoids:

* Creating a new HTTP session per row
* Creating a new HTTP session per partition
* Repeated DNS lookups, TCP handshakes, and TLS negotiations
* Spark serialization failures caused by leaking runtime state

> **Important:** Transport reuse is scoped to the Python worker process—not the driver or the cluster. Each executor maintains its own independent transport instance.

## Transport vs Middleware Responsibilities

The transport layer is intentionally minimal.

| Concern | Transport | Middleware |
|---------|----------|------------|
| Connection pooling | ✅ | ❌ |
| TCP / TLS / DNS | ✅ | ❌ |
| Warm-up | ✅ | ❌ |
| Authentication | ❌ | ✅ |
| Retries / backoff | ❌ | ✅ |
| Logging / metrics | ❌ | ✅ |
| Request mutation | ❌ | ✅ |
| Response parsing | ❌ | ✅ |

This separation ensures that:

* Transports remain interchangeable
* Request behavior is fully configurable
* Adding new transports does not affect pipeline logic

## Supported Transport Engines

| Engine | Status | Notes |
|--------|--------|-------|
| `aiohttp` | Default | Async, pooled, warm-up enabled |
| `httpx` | Planned | Async + sync supported |

## Configuration

Transport settings are configured in the pipeline YAML:

```yaml
transport:
  base_timeout: 30        # Default request timeout in seconds
  warmup_timeout: 10      # Timeout for connection warm-up
  tcp_connection:
    limit: 10             # Maximum concurrent TCP connections per worker
```

## Connection Pooling

The transport maintains a pool of TCP connections that are reused across requests. This avoids the overhead of:

* DNS resolution for each request
* TCP handshake (SYN, SYN-ACK, ACK)
* TLS negotiation (for HTTPS)

Connection pool settings:

```yaml
transport:
  tcp_connection:
    limit: 10             # Max connections in pool
    ttl_dns_cache: 300    # DNS cache TTL in seconds (optional)
```

## Warm-up

Before processing rows, the transport performs a warm-up phase to establish connections proactively. This reduces latency for the first few requests in each partition.

```yaml
transport:
  warmup_timeout: 10      # Seconds to wait for warm-up
```

## Error Handling

The transport layer handles low-level network errors:

* Connection timeouts
* DNS resolution failures
* TLS handshake errors
* Connection refused

These errors are propagated to the middleware chain, which can then apply retry logic or other handling.

## Adding New Transports

To add a new transport engine:

1. Implement the transport interface
2. Create a factory that produces transport instances
3. Register the factory in the transport registry

The framework will then select the appropriate transport based on configuration.
