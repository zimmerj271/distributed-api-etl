import pytest
from auth.rpc.service import RpcService
from tests.fixtures.spark import FakeSparkSession


@pytest.mark.parametrize("port", [0, 22, 80, 443, 4040, 7077])
def test_invalid_ports(port):
    svc = RpcService.__new__(RpcService)
    assert svc.valid_port(port) is False


@pytest.mark.parametrize("port", [1025, 9001, 25000])
def test_valid_ports(port):
    svc = RpcService.__new__(RpcService)
    assert svc.valid_port(port) is True


def test_dynamic_port_select_returns_valid_port():
    port = RpcService.dynamic_port_select()
    assert 0 < port <= 65535


def test_rpc_service_url():
    class DummyRpc(RpcService):
        def build_app(self):
            raise NotImplementedError

    spark = FakeSparkSession()
    svc = DummyRpc(spark=spark, port=9999)

    assert svc.url == "http://localhost:9999"

