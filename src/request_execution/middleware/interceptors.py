# Wrap the downstream call and inspect/modifies the request and response
# timing, bearer token, json response, retry
import asyncio
import aiohttp
import random
import json
from typing import Iterable

from request_execution.models import RequestExchange
from request_execution.middleware.pipeline import (
    NEXT_CALL, 
    Middleware,
    MiddlewareFactory,
    MiddlewareType,
)


@MiddlewareFactory.register(MiddlewareType.RETRY)
class RetryMiddleware(Middleware):
    """
    Retry on failure using exponential backoff and jitter
    Sets success/fail on RequestExchange if maximum number of retries is reached.
    """

    def __init__(
        self, 
        max_attempts: int = 10, 
        retry_status_codes: Iterable[int] = (500, 502, 503, 504, 429),
        base_delay: float = 0.1,
        max_delay: float = 2.0
    ) -> None:
        self.max_attempts = max_attempts 
        self.retry_status_codes = set(retry_status_codes)
        self.base_delay = base_delay
        self.max_delay = max_delay

    def __is_retryable_exception(self, exc: Exception) -> bool:
        # Expand as needed
        if isinstance(
            exc, 
            (
                aiohttp.ClientConnectionError, 
                aiohttp.ClientPayloadError,
                aiohttp.ServerTimeoutError,
                asyncio.TimeoutError
            )
        ):
            return True
        return False
    
    def __is_retryable_status(self, request_exchange: RequestExchange) -> bool:
        return (
            request_exchange.status_code is not None
            and request_exchange.status_code in self.retry_status_codes
        )

    async def __exponential_backoff(self, attempt: int) -> None:
        """Low latency exponential backoff with bounded random jitter delay."""
        exponential_delay = self.base_delay * (2 ** attempt)
        delay = random.uniform(0.0, min(self.max_delay, exponential_delay))
        await asyncio.sleep(delay)

    async def __call__(self, request_exchange: RequestExchange, next_call: NEXT_CALL) -> RequestExchange:
        logs = request_exchange.metadata.setdefault("logs", [])

        for attempt in range(1, self.max_attempts + 1):
            request_exchange.attempts = attempt
            logs.append(
                f"[RetryMiddleware] Attempt {attempt}/{self.max_attempts} -> "
                f"{request_exchange.context.method.name} {request_exchange.context.url}"
            )

            try:
                request_exchange = await next_call(request_exchange)

                # Retry on selected HTTP status codes 
                if self.__is_retryable_status(request_exchange):
                    status = request_exchange.status_code
                    logs.append(
                        f"[RetryMiddleware] Got retryable HTTP {status} on attempt {attempt}"
                    )

                    if attempt < self.max_attempts:
                        await self.__exponential_backoff(attempt)
                        continue

                    # Exhausted retries on retryable status 
                    request_exchange.success = False
                    request_exchange.error_message = (
                        f"Retry attempts exhausted (HTTP {status}) "
                        f"after {attempt} attempts"
                    )
                    request_exchange.metadata["retry_attempts"] = attempt
                    return request_exchange

                # Non-retryable status or success -- let downstream semantics manage
                return request_exchange
            except Exception as exc:
                # Retryable transport/timeout error?
                if self.__is_retryable_exception(exc) and attempt < self.max_attempts:
                    logs.append(
                        f"[RetryMiddleware] Retryable exception on attempt {attempt}: "
                        f"{type(exc).__name__}: {exc}"
                    )
                    await self.__exponential_backoff(attempt)
                    continue

                # Either non-retryable or max attempts reached
                reason = (
                    f"Retry attempts exhausted: {type(exc).__name__}: {exc}"
                    if self.__is_retryable_exception(exc)
                    else f"Non-retryable exception: {type(exc).__name__}: {exc}"
                )
                logs.append(f"[RetryMiddleware] {reason}")
                request_exchange.success = False
                request_exchange.error_message = reason
                request_exchange.metadata["retry_attempts"] = attempt
                request_exchange.attempts = attempt
                return request_exchange

        # Defensive fallback -- normally unreachable
        request_exchange.success = False
        request_exchange.error_message = "RetryMiddleware: unexpected fallback reached"
        request_exchange.metadata["retry_attempts"] = self.max_attempts
        return request_exchange


@MiddlewareFactory.register(MiddlewareType.JSON_BODY)
class JsonResponseMiddleware(Middleware):
    """
    Reads the response JSON (if any), stores it in RequestExchange.json.
    Marks the RequestExchange success=False with error_message if parsing fails.
    """

    async def __call__(self, request_exchange: RequestExchange, next_call: NEXT_CALL) -> RequestExchange:
        result = await next_call(request_exchange)

        if result.body is None:
            return result

        try:
            text = result.body.decode("utf-8")
            result.body_text = text

            try:
                json.loads(text)
                json_output = {"valid": True, "error": None}
                result.metadata["json"] = json_output 
            except json.JSONDecodeError as je:
                json_output = {"valid": False, "error": str(je)}
                result.metadata["json"] = json_output

            if result.status_code is not None and result.status_code < 400:
                result.success = True

        except Exception as e:
            result.success = False
            result.error_message = f"Body binary to string conversion error: {str(e)}"


        return result


class ParamInjectorMiddleware(Middleware):
    """
    Injects row-level query params to the RequestContext.
    """

    def __init__(self, param_bindings: dict[str, str]) -> None:
        """
        param bindings: {query_param_name: input_column_name}
        """
        self.param_bindings = param_bindings

    async def __call__(self, request_exchange: RequestExchange, next_call: NEXT_CALL) -> RequestExchange:
        ctx = request_exchange.context
        row = ctx._row

        if row is None:
            return await next_call(request_exchange)

        if ctx.params is not None:
            ctx.params.clear()

        if ctx.params is None:
            ctx.params = {}

        for param, col in self.param_bindings.items():
            ctx.params[param] = row[col]

        return await next_call(request_exchange)
