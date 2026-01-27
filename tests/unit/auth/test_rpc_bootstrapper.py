import pytest
from unittest.mock import patch, MagicMock

from auth.rpc.bootstrap import RpcBootstrapper
from tests.fixtures.spark import FakeSparkSession


@patch("auth.rpc.bootstrap.AsyncBackgroundService")
@patch("auth.rpc.bootstrap.TokenRpcService")
@patch("auth.rpc.bootstrap.DriverTokenManager")
@patch("auth.rpc.bootstrap.PasswordGrantTokenProvider")
def test_rpc_bootstrapper_start(
    mock_provider,
    mock_tm,
    mock_rpc,
    mock_bg,
):
    spark = FakeSparkSession()
    credentials = {
        "client_id": "id",
        "client_secret": "secret",
        "username": "user",
        "password": "pass",
    }

    bootstrap = RpcBootstrapper(
        spark=spark,
        token_url="http://token",
        credentials=credentials,
    )

    bootstrap.start()

    mock_provider.assert_called_once()
    mock_tm.assert_called_once()
    mock_rpc.assert_called_once()
    assert mock_bg.call_count == 2


def test_rpc_bootstrapper_url_before_start_raises():
    bootstrap = RpcBootstrapper(
        spark=FakeSparkSession(),
        token_url="http://token",
        credentials={},
    )

    with pytest.raises(ValueError):
        _ = bootstrap.url


@patch("auth.rpc.bootstrap.AsyncBackgroundService")
def test_rpc_bootstrapper_stop(mock_bg):
    spark = FakeSparkSession()
    bootstrap = RpcBootstrapper(
        spark=spark,
        token_url="http://token",
        credentials={},
    )

    bootstrap._token_runtime = MagicMock()
    bootstrap._rpc_runtime = MagicMock()

    bootstrap.stop()

    bootstrap._token_runtime.stop.assert_called_once()
    bootstrap._rpc_runtime.stop.assert_called_once()

