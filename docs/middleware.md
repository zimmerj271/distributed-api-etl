# Middleware

The middleware pipeline is the primary extension point of this framework. It is a composable chain of components that wrap each HTTP request/response cycle on Spark worker nodes, following the interceptor pattern: each middleware receives a `RequestExchange` and a `next_call` handle to the remainder of the chain, and may execute logic before and after the downstream call.
All request behavior — authentication, retries, logging, timing, parameter injection — is implemented as middleware. The core execution engine has no knowledge of any of these concerns.

## Middleware Types

Middleware in this framework falls into one of two categories based on whether it affects control flow:
### Interceptors
Interceptors wrap `await next_call(exchange)` and may execute logic both before and after the HTTP request. They can alter control flow by retrying, short-circuiting, or conditionally calling the next middleware.
Typical use cases:

- Retry with exponential backoff
- OAuth2 token refresh
- JSON response parsing and validation
- Circuit breaking

Interceptors are the most powerful middleware type. Because they control whether and how many times the downstream pipeline executes, they must be implemented carefully.

### Injectors
Injectors always call `await next_call(exchange)` unconditionally. They mutate the outbound request or record observations on the inbound response, but never alter control flow.
Typical use cases:

- Injecting authentication headers or bearer tokens
- Adding query parameters from row data
- Logging request and response details
- Recording timing and executor identity

Injectors should never retry requests, raise exceptions intentionally, or modify success/failure semantics.

## Middleware Contract

All middleware must:

- Accept a `RequestExchange`
- Return a `RequestExchange`
- Be fully async-safe
- Be serializable via factory instantiation
- Avoid blocking I/O
- Avoid direct Spark API usage

Middleware should fail softly by recording structured error information in
`RequestExchange.metadata` rather than raising unhandled exceptions.

## Execution Order

Middleware is executed **in the order it is configured**.

Each middleware wraps the next:

```
Middleware A
  → Middleware B
    → Middleware C
      → HTTP Request
    ← Middleware C
  ← Middleware B
← Middleware A
```

This allows middleware to run logic **before and/or after** the HTTP request.

## Available Middleware

| Middleware | Category | Purpose |
|----------|----------|---------|
| RetryMiddleware | Interceptor | Retry failed requests with exponential backoff |
| JsonResponseMiddleware | Interceptor | Parse and validate JSON responses |
| ParamInjectorMiddleware | Injector | Inject row-level query parameters into the request |
| LoggingMiddleware | Injector | Record request and response details to metadata |
| TimingMiddleware | Injector | Measure and record total request latency |
| WorkerIdentityMiddleware | Injector | Annotate requests with executor hostname, PID, and thread ID |
| TransportDiagnosticMiddleware | Injector | Record transport connection warmup diagnostics |
| BearerTokenMiddleware | Injector | Injects bearer token into request header |
| HeaderAuthMiddleware | Injector | Injects user authentication into header |

## Configuration

Middleware is configured declaratively in the pipeline YAML and executed in the order listed. Interceptors that wrap the full request lifecycle — retry, auth — should generally appear before injectors that only observe.

Each middleware entry defines:
* The middleware type
* Optional configuration parameters

Example:

```yaml
middleware:
  - type: retry
    max_attempts: 3
    retry_status_codes: [429, 500, 502, 503]
    base_delay: 0.2
    max_delay: 2.0
  - type: logging
  - type: timing
  - type: json_body
```

### Retry Middleware

```yaml
- type: retry
  max_attempts: 3           # Maximum retry attempts
  retry_status_codes:       # HTTP status codes to retry
    - 429
    - 500
    - 502
    - 503
  base_delay: 0.2           # Initial delay in seconds
  max_delay: 2.0            # Maximum delay in seconds
```

### Logging Middleware

```yaml
- type: logging
  level: INFO               # Log level (optional)
```

### Timing Middleware

```yaml
- type: timing             # No configuration needed
```

### JSON Body Middleware

```yaml
- type: json_body          # Parses JSON responses automatically
```

## Adding Custom Middleware

Implement the interceptor interface and register via factory injection or configuration:

```python
class CustomHeaderMiddleware:
    def __init__(self, header_name: str, header_value: str):
        self.header_name = header_name
        self.header_value = header_value

    async def __call__(
        self,
        request_exchange: RequestExchange,
        next_call: NEXT_CALL
    ) -> RequestExchange:
        request_exchange.context.headers[self.header_name] = self.header_value
        return await next_call(request_exchange)
```
For an interceptor that executes logic on both sides of the pipeline:
```python
class TimingMiddleware:
    async def __call__(
        self,
        request_exchange: RequestExchange,
        next_call: NEXT_CALL
    ) -> RequestExchange:
        start = time.monotonic()
        result = await next_call(request_exchange)
        result.metadata["duration"] = time.monotonic() - start
        return result
```
### Unit Testing Middleware
Unit testing middleware in isolation is straightforward — pass a mock `next_call` and assert on the returned RequestExchange:
```python
async def test_header_auth_middleware():
    middleware = HeaderAuthMiddleware(username="user", password="pass")
    exchange = RequestExchange(context=RequestContext(method=..., url=...))

    async def mock_next(ex: RequestExchange) -> RequestExchange:
        assert "Authorization" in ex.context.headers
        return ex

    await middleware(exchange, mock_next)
```

## Best Practices

1) **Order matters** — place interceptors (retry, auth) before injectors (logging, timing) so retries apply to the full request lifecycle including auth injection
2) **Keep middleware focused** — each middleware should do exactly one thing
Prefer stateless middleware — if state is needed, scope it to the worker process via the factory pattern
3) **Fail softly** — record errors in RequestExchange.metadata rather than raising exceptions that would terminate partition processing
4) **Never log sensitive data** — avoid writing credentials, tokens, or PII to metadata or logs

## Why Use a Middleware Pattern:
1) **Separation of Concerns**
Each middleware has a single job:
- Auth injector: add credentials
- Retry middleware: handle transient failures
- Logging middleware: record requests/responses
- Timing middleware: measure latency

This makes each component simple, testable, and reusable.

2) **Composability**
Middleware can be stacked in any order via configuration:
The framework builds the pipeline at runtime by wrapping each middleware around the next.

3) **Extensibility**
New middleware can be added without modifying existing code:
Just add it to the configuration and the framework automatically integrates it.

4. **Testability**
Each middleware can be unit tested in isolation:

