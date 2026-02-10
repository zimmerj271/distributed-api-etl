import pytest
from config.models.data_contract import (
    ColumnSchema,
    TableSchema,
    DataType,
)


@pytest.fixture
def simple_table_schema():
    return TableSchema(
        columns=[
            ColumnSchema(name="tracking_id", type=DataType.STRING),
            ColumnSchema(name="value", type=DataType.INTEGER),
        ]
    )

