from transport.engine import TransportEngineFactory
from transport.base import TransportEngineType


def test_transport_engine_factory_registration():
    keys = TransportEngineFactory.list_keys()
    assert TransportEngineType.AIOHTTP in keys

