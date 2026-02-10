import pytest
from pyspark.sql.types import (
    StringType, IntegerType, LongType, FloatType, DoubleType,
    BooleanType, TimestampType, DateType, BinaryType, ArrayType, MapType,
)
from config.models.data_contract import TableSchema, ColumnSchema, DataType


class TestColumnValidation:
    """Group related column validation tests"""
    
    @pytest.mark.unit
    @pytest.mark.config
    def test_column_name_validation_rejects_invalid_characters(self):
        with pytest.raises(ValueError, match="alphanumeric"):
            ColumnSchema(name="bad-name!", type=DataType.STRING)
    
    @pytest.mark.unit
    @pytest.mark.config
    def test_column_name_validation_accepts_underscores(self):
        col = ColumnSchema(name="valid_name", type=DataType.STRING)
        assert col.name == "valid_name"


class TestTableSchema:
    """Group table schema tests"""
    
    @pytest.mark.unit
    @pytest.mark.config
    def test_duplicate_columns_raises_error(self):
        with pytest.raises(ValueError, match="Duplicate column names"):
            TableSchema(
                columns=[
                    ColumnSchema(name="id", type=DataType.STRING),
                    ColumnSchema(name="id", type=DataType.STRING),
                ]
            )
    
    @pytest.mark.unit
    @pytest.mark.config
    def test_get_column_returns_matching_column(self, simple_table_schema):
        col = simple_table_schema.get_column("tracking_id")
        assert col is not None
        assert col.name == "tracking_id"
    
    @pytest.mark.unit
    @pytest.mark.config
    def test_get_column_returns_none_for_missing(self, simple_table_schema):
        col = simple_table_schema.get_column("nonexistent")
        assert col is None


class TestSparkSchemaConversion:
    """Group Spark schema conversion tests"""
    
    @pytest.mark.parametrize("dtype,expected_type", [
        (DataType.STRING, StringType),
        (DataType.INTEGER, IntegerType),
        (DataType.LONG, LongType),
        (DataType.FLOAT, FloatType),
        (DataType.DOUBLE, DoubleType),
        (DataType.BOOLEAN, BooleanType),
        (DataType.TIMESTAMP, TimestampType),
        (DataType.DATE, DateType),
        (DataType.BINARY, BinaryType),
    ])
    @pytest.mark.unit
    @pytest.mark.config
    def test_scalar_type_mapping(self, dtype, expected_type):
        schema = TableSchema(columns=[ColumnSchema(name="col", type=dtype)])
        spark_schema = schema.to_spark_schema()
        field = spark_schema.fields[0]
        assert isinstance(field.dataType, expected_type)
    
    @pytest.mark.unit
    @pytest.mark.config
    def test_array_type_with_element_type(self):
        schema = TableSchema(
            columns=[
                ColumnSchema(
                    name="tags",
                    type=DataType.ARRAY,
                    element_type=DataType.STRING,
                )
            ]
        )
        spark_schema = schema.to_spark_schema()
        field = spark_schema.fields[0]
        
        assert isinstance(field.dataType, ArrayType)
        assert isinstance(field.dataType.elementType, StringType)
        assert field.dataType.containsNull is True
    
    @pytest.mark.unit
    @pytest.mark.config
    def test_array_type_missing_element_type_raises(self):
        schema = TableSchema(
            columns=[ColumnSchema(name="tags", type=DataType.ARRAY)]
        )
        with pytest.raises(ValueError, match="must specify element_type"):
            schema.to_spark_schema()
    
    @pytest.mark.unit
    @pytest.mark.config
    def test_map_type_with_key_and_value(self):
        schema = TableSchema(
            columns=[
                ColumnSchema(
                    name="attributes",
                    type=DataType.MAP,
                    key_type=DataType.STRING,
                    value_type=DataType.STRING,
                )
            ]
        )
        spark_schema = schema.to_spark_schema()
        field = spark_schema.fields[0]
        
        assert isinstance(field.dataType, MapType)
        assert isinstance(field.dataType.keyType, StringType)
        assert isinstance(field.dataType.valueType, StringType)
    
    @pytest.mark.unit
    @pytest.mark.config
    def test_map_type_missing_types_raises(self):
        schema = TableSchema(
            columns=[
                ColumnSchema(
                    name="attributes",
                    type=DataType.MAP,
                    key_type=DataType.STRING,
                )
            ]
        )
        with pytest.raises(ValueError, match="must specify key_type and value_type"):
            schema.to_spark_schema()
    
    @pytest.mark.unit
    @pytest.mark.config
    def test_field_order_is_preserved(self):
        schema = TableSchema(
            columns=[
                ColumnSchema(name="first", type=DataType.STRING),
                ColumnSchema(name="second", type=DataType.INTEGER),
                ColumnSchema(name="third", type=DataType.BOOLEAN),
            ]
        )
        spark_schema = schema.to_spark_schema()
        names = [f.name for f in spark_schema.fields]
        assert names == ["first", "second", "third"]
