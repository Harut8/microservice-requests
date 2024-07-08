import asyncio
import json
from functools import wraps
from typing import Any, Callable

import circuitbreaker
import httpx
from circuitbreaker import circuit
from circuitbreaker import CircuitBreaker
from retry_async import retry

from http_exceptions import RequestError, ServiceUnavailableException

DEFAULT_URL = "http://127.0.0.1:8000/api/v1/"


def handle_circuit_breaker_exception(func):
    async def wrapper(*args, **kwargs):
        try:
            return await func(*args, **kwargs)
        except circuitbreaker.CircuitBreakerError as e:
            raise ServiceUnavailableException(message='Service Unavailable due to Circuit Breaker') from e

    return wrapper


class CustomCircuitBreaker(CircuitBreaker):
    FAILURE_THRESHOLD = 1
    RECOVERY_TIMEOUT = 60
    EXPECTED_EXCEPTION = RequestError


def request_exception_handler(func: Callable[..., Any]) -> Callable[..., Any]:
    @CustomCircuitBreaker()
    @retry(
        exceptions=(
                ServiceUnavailableException,
                RequestError,
        ),
        tries=2,
        delay=3,
        is_async=True,
    )
    async def wrapper(*args, **kwargs):
        try:
            return await func(*args, **kwargs)
        except httpx.HTTPStatusError as e:
            raise RequestError(
                status_code=e.response.status_code,
                message="Something went wrong. Problem likes network issue or server error.",
            ) from e
        except httpx.ConnectError as e:
            raise ServiceUnavailableException() from e

    return wrapper


def run_request(func):
    @wraps(func)
    async def wrapper(*args, **kwargs):
        global DEFAULT_URL
        async with httpx.AsyncClient() as client:
            kwargs["client"] = client
            kwargs["timeout"] = 5 * 60
            base_url = kwargs.get("base_url")
            DEFAULT_URL = DEFAULT_URL if not base_url else base_url
            print(f"Try to call with url: {DEFAULT_URL}")
            response = await func(*args, **kwargs)
            response.raise_for_status()
            try:
                return response.json()
            except json.JSONDecodeError:
                pass
            return response.status_code in [200, 201, 204]

    return wrapper


@handle_circuit_breaker_exception
@request_exception_handler
@run_request
async def create_get_request(url, client, default_url=DEFAULT_URL, **kwargs):
    return await client.get(f"{default_url}{url}", **kwargs)


@handle_circuit_breaker_exception
@request_exception_handler
@run_request
async def create_post_request(url, client, data=None, default_url=DEFAULT_URL, **kwargs):
    return (
        await client.post(f"{default_url}{url}", data=json.dumps(data), **kwargs)
        if data
        else await client.post(f"{default_url}{url}", **kwargs)
    )


@handle_circuit_breaker_exception
@request_exception_handler
@run_request
async def create_delete_request(url, client, default_url=DEFAULT_URL, **kwargs):
    return await client.delete(f"{default_url}{url}", **kwargs)
