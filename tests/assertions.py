"""Custom assertions for better test readability"""
from typing import Any
from pyspark.sql import DataFrame


def assert_dataframe_equal(df1: DataFrame, df2: DataFrame, check_order: bool = False):
    """Assert two DataFrames are equal"""
    if not check_order:
        df1 = df1.orderBy(*df1.columns)
        df2 = df2.orderBy(*df2.columns)
    
    assert df1.schema == df2.schema, "Schemas don't match"
    assert df1.collect() == df2.collect(), "Data doesn't match"


def assert_valid_token(token: Any):
    """Assert object is a valid token"""
    assert hasattr(token, 'token_value'), "Token missing token_value"
    assert hasattr(token, 'expires_at'), "Token missing expires_at"
    assert token.token_value, "Token value is empty"


def assert_config_valid(config: Any, config_type: type):
    """Assert config is valid instance of expected type"""
    assert isinstance(config, config_type), f"Expected {config_type}, got {type(config)}"
    # Pydantic models have model_validate
    assert hasattr(config, 'model_dump'), "Not a valid Pydantic model"
