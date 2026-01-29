"""Core fixtures for testing"""
from .coroutine import simple_background
from .runtime import DummyAsyncResource

__all__ = [
    'simple_background',
    'DummyAsyncResource',
]
