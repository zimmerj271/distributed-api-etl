# Middleware

This framework uses a **middleware pipeline** to customize request behavior without modifying core execution logic. Middleware is executed **on Spark worker nodes** and wraps each API request using a chain-of-responsibility pattern.

## What Middleware Can Do

Middleware allows you to:

* Inject authentication or headers
* Retry failed requests with backoff
* Record timing and diagnostics
* Enrich or inspect request / response metadata
* Mutate outbound requests on a per-row basis

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

## Middleware Categories

Middleware is intentionally divided into **three conceptual categories**:

### 1. Interceptors (Control-Flow Middleware)

Interceptors **modify request or response behavior** and may influence execution flow.

Examples:
* Retry logic
* Authentication injection
* Query parameter injection
* JSON parsing
* Request/response mutation

These middleware **must run before the HTTP request**.

### 2. Listeners (Observability Middleware)

Listeners **observe and record metadata** but do not alter request/response semantics.

Examples:
* Logging
* Timing
* Worker / executor identity
* Diagnostics

Listeners are safe to run anywhere in the pipeline and never affect request success or retries.

### 3. Common (Mutate Requests Only)

Standard middleware that does not follow the interceptor pattern. These middleware objects only mutate requests.

## Available Middleware

| Middleware | Category | Purpose |
|----------|----------|---------|
| RetryMiddleware | Interceptor | Retry failed requests with exponential backoff |
| JsonResponseMiddleware | Interceptor | Parse and validate JSON responses |
| AuthMiddleware | Interceptor | Inject authentication headers |
| ParamInjectorMiddleware | Interceptor | Inject row-level query parameters |
| LoggingMiddleware | Listener | Record request/response logs |
| TimingMiddleware | Listener | Measure request latency |
| WorkerIdentityMiddleware | Listener | Record executor identity |
| TransportDiagnosticMiddleware | Listener | Record transport diagnostics |
| BearerTokenMiddleware | Common | Injects bearer token into request header |
| HeaderAuthMiddleware | Common | Injects user authentication into header |

## Configuration

Middleware is configured declaratively and executed in the order listed.

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

To add custom middleware, implement the following interface:

```python
async def __call__(
    self,
    request_exchange: RequestExchange,
    next_call: NEXT_CALL
) -> RequestExchange:
    # Pre-request logic
    ...

    # Call next middleware in chain
    result = await next_call(request_exchange)

    # Post-request logic
    ...

    return result
```

Then register it via configuration, factory injection, or late binding in `ApiPartitionExecutor`.

### Example: Custom Header Middleware

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
        # Add custom header before request
        request_exchange.request.headers[self.header_name] = self.header_value

        # Continue chain
        return await next_call(request_exchange)
```

## Best Practices

1. **Order matters**: Place interceptors (retry, auth) before listeners (logging, timing)
2. **Keep middleware focused**: Each middleware should do one thing well
3. **Avoid state**: Middleware should be stateless or use request-scoped state
4. **Handle errors gracefully**: Middleware should not swallow exceptions unless intentional
5. **Log strategically**: Avoid logging sensitive data (credentials, tokens)
