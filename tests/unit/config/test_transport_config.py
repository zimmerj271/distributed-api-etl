def test_aiohttp_runtime_args(aiohttp_config):
    args = aiohttp_config.to_runtime_args()
    assert args["base_timeout"] == 30
    assert args["connector_config"].limit == 50
