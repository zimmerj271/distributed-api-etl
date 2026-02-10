import pytest
from unittest.mock import patch, MagicMock

from auth.rpc.bootstrap import RpcBootstrapper
from auth.token.token_provider import TokenProvider
from tests.fixtures.spark import FakeSparkSession


class FakeTokenProvider(TokenProvider):
    async def get_token(self):
        return MagicMock()

    def token_telemetry(self):
        return {}


@patch("auth.rpc.bootstrap.AsyncBackgroundService")
@patch("auth.rpc.bootstrap.TokenRpcService")
@patch("auth.rpc.bootstrap.DriverTokenManager")
def test_rpc_bootstrapper_start(
    mock_tm,
    mock_rpc,
    mock_bg,
):
    spark = FakeSparkSession()
    token_provider = FakeTokenProvider()

    bootstrap = RpcBootstrapper(
        spark=spark,
        token_provider=token_provider,
    )

    bootstrap.start()

    mock_tm.assert_called_once_with(token_provider, refresh_margin=60)
    mock_rpc.assert_called_once()
    assert mock_bg.call_count == 2


def test_rpc_bootstrapper_url_before_start_raises():
    bootstrap = RpcBootstrapper(
        spark=FakeSparkSession(),
        token_provider=FakeTokenProvider(),
    )

    with pytest.raises(ValueError):
        _ = bootstrap.url


@patch("auth.rpc.bootstrap.AsyncBackgroundService")
def test_rpc_bootstrapper_stop(mock_bg):
    spark = FakeSparkSession()
    bootstrap = RpcBootstrapper(
        spark=spark,
        token_provider=FakeTokenProvider(),
    )

    bootstrap._token_runtime = MagicMock()
    bootstrap._rpc_runtime = MagicMock()

    bootstrap.stop()

    bootstrap._token_runtime.stop.assert_called_once()
    bootstrap._rpc_runtime.stop.assert_called_once()
