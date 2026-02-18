# Transport Layer

The **transport layer** is responsible for executing HTTP requests on Spark 
**worker nodes**. It is intentionally designed as a **low-level**, 
**runtime-only abstraction** that focuses solely on *network execution* and 
*connection management*, not request semantics. The transport layer is: fully 
serializable and safe to distribute across the cluster, scoped to the Python 
worker process lifetime for connection reuse across partitions, warmed up 
proactively to avoid cold-start latency, and extensible via the `TransportEngine` 
interface and factory registry.

All higher-level concerns — authentication, retries, logging, request mutation, 
response handling — are **explicitly excluded** from the transport layer and 
handled entirely by middleware.

## Transport vs Middleware Responsibilities

The transport layer is intentionally minimal and distinguished from middleware by strict separation of concern that is scoped only to handling HTTP requests and responses.

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

## Execution Model

Spark executes user code in long-lived **Python worker processes** on executor nodes. The transport layer is designed around this execution model. Transport objects have a process-scoped lifecycle tied to the Python worker process instead of the partition or individual rows. 

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

## Configuration

Transport settings are configured in the pipeline YAML:

```yaml
transport:
  base_timeout: 30        # Default request timeout in seconds
  warmup_timeout: 10      # Timeout for connection warm-up
  tcp_connection:
    limit: 10             # Maximum concurrent TCP connections per worker
```

**Connection Pooling:**
The transport maintains a pool of TCP connections that are reused across requests. This avoids the overhead of:

* DNS resolution for each request
* TCP handshake (SYN, SYN-ACK, ACK)
* TLS negotiation (for HTTPS)

**Warm-up:**
Before processing rows, the transport performs a warm-up phase to establish 
connections proactively, priming DNS resolution, TCP handshake, and TLS 
negotiation before the first partition row is processed. If warm-up fails, 
the transport continues without it and records the error in response metadata 
via `TransportDiagnosticMiddleware` — cold-start latency may affect the first 
few requests on that worker but processing is not interrupted.

## Error Handling

The transport layer never raises exceptions for network failures. Instead, it 
catches low-level errors, wraps them in a `TransportResponse` with the `error` 
field set, and returns the response up the middleware chain. If `RetryMiddleware` 
is configured, it detects the error and retries the request according to its 
backoff policy. If no retry middleware is present, the error propagates to the 
output row as a failed request.

Transport handles the following network errors:
* Connection timeouts
* DNS resolution failures
* TLS handshake errors
* Connection refused


## Adding New Transport Engines

To add a new transport engine:

1. **Implement the transport interface**
The abstract interface for `TransportEngine` is located in`src/request_execution/transport/base.py`:
```python
class TransportEngine(ABC):
    """
    A structural interface that defines a pluggable HTTP engine abstraction.
    The HTTP Transport engine performs a single HTTP request and returns a
    low-level TransportResponse. Implementations may wrap aiohttp, httpx,
    requests, urllib3, etc. Transport is also the lifecycle manager for an
    HTTP session.
    """

    @abstractmethod
    async def __aenter__(self) -> "TransportEngine": ...

    @abstractmethod
    async def __aexit__(
        self, 
        exc_type: BaseException | None, 
        exc_val: BaseException | None, 
        exc_tb: TracebackType | None,
    ) -> None: ...

    @abstractmethod
    async def send(self, request: TransportRequest) -> TransportResponse: ...
```
As described by the design interface, the implementation must be asynchronous and define the behavior for entering and exiting an asynchronous block as well as the HTTP send logic.

2. Add a factory decorator to register the concrete implementation to `TransportEngineFactory`:
```python
@TransportEngineFactory.register(TransportEngineType.AIOHTTP)
class AiohttpEngine(TransportEngine):
...
```

3. **Wire up the configuration** by following the same pattern as `AiohttpEngine`:
   - Add a new value to the `TransportEngineType` enum in `src/request_execution/transport/base.py`
   - Create a config model subclass in `src/config/models/transport.py` that defines 
     the constructor arguments your engine requires
   - Update `TransportRuntimeFactory` in `src/config/factories.py` to instantiate 
     your engine from the config model

   The `AiohttpEngine` and `AiohttpEngineConfig` implementations serve as the 
   reference pattern for both the config model structure and factory wiring.
