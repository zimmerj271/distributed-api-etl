import pytest
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    LongType,
    FloatType,
    DoubleType,
    BooleanType,
    TimestampType,
    DateType,
    BinaryType,
    ArrayType,
    MapType,
)
from config.models.data_contract import (
    TableSchema,
    ColumnSchema,
    DataType,
)


def test_column_name_validation():
    with pytest.raises(ValueError):
        ColumnSchema(name="bad-name!", type=DataType.STRING)


def test_table_schema_duplicate_columns():
    with pytest.raises(ValueError):
        TableSchema(
            columns=[
                ColumnSchema(name="id", type=DataType.STRING),
                ColumnSchema(name="id", type=DataType.STRING),
            ]
        )


def test_get_column(simple_table_schema):
    col = simple_table_schema.get_column("tracking_id")
    assert col is not None
    assert col.name == "tracking_id"


def test_to_spark_schema_simple_types():
    schema = TableSchema(
        columns=[
            ColumnSchema(name="id", type=DataType.STRING, nullable=False),
            ColumnSchema(name="value", type=DataType.INTEGER),
        ]
    )

    spark_schema = schema.to_spark_schema()

    assert isinstance(spark_schema, StructType)
    assert len(spark_schema.fields) == 2

    f0, f1 = spark_schema.fields

    assert f0.name == "id"
    assert isinstance(f0.dataType, StringType)
    assert f0.nullable is False

    assert f1.name == "value"
    assert isinstance(f1.dataType, IntegerType)
    assert f1.nullable is True


def test_to_spark_schema_scalar_types():
    schema = TableSchema(
        columns=[
            ColumnSchema(name="id", type=DataType.STRING, nullable=False),
            ColumnSchema(name="count", type=DataType.INTEGER),
            ColumnSchema(name="active", type=DataType.BOOLEAN),
        ]
    )

    spark_schema = schema.to_spark_schema()

    assert isinstance(spark_schema, StructType)
    assert len(spark_schema.fields) == 3

    f0, f1, f2 = spark_schema.fields

    # Field 0
    assert isinstance(f0, StructField)
    assert f0.name == "id"
    assert isinstance(f0.dataType, StringType)
    assert f0.nullable is False

    # Field 1
    assert f1.name == "count"
    assert isinstance(f1.dataType, IntegerType)
    assert f1.nullable is True

    # Field 2
    assert f2.name == "active"
    assert isinstance(f2.dataType, BooleanType)


@pytest.mark.parametrize(
    "dtype, expected",
    [
        (DataType.STRING, StringType),
        (DataType.INTEGER, IntegerType),
        (DataType.LONG, LongType),
        (DataType.FLOAT, FloatType),
        (DataType.DOUBLE, DoubleType),
        (DataType.BOOLEAN, BooleanType),
        (DataType.TIMESTAMP, TimestampType),
        (DataType.DATE, DateType),
        (DataType.BINARY, BinaryType),
    ],
)
def test_scalar_type_mapping(dtype, expected):
    schema = TableSchema(
        columns=[ColumnSchema(name="col", type=dtype)]
    )

    spark_schema = schema.to_spark_schema()
    field = spark_schema.fields[0]

    assert isinstance(field.dataType, expected)


def test_array_type_with_element_type():
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


def test_array_type_missing_element_type_raises():
    schema = TableSchema(
        columns=[
            ColumnSchema(name="tags", type=DataType.ARRAY)
        ]
    )

    with pytest.raises(ValueError, match="must specify element_type"):
        schema.to_spark_schema()


def test_map_type_with_key_and_value():
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
    assert field.dataType.valueContainsNull is True


def test_map_type_missing_key_or_value_raises():
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


def test_field_order_is_preserved():
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


def test_mixed_complex_and_scalar_schema():
    schema = TableSchema(
        columns=[
            ColumnSchema(name="id", type=DataType.STRING),
            ColumnSchema(
                name="tags",
                type=DataType.ARRAY,
                element_type=DataType.STRING,
            ),
            ColumnSchema(
                name="metadata",
                type=DataType.MAP,
                key_type=DataType.STRING,
                value_type=DataType.STRING,
            ),
        ]
    )

    spark_schema = schema.to_spark_schema()

    assert isinstance(spark_schema.fields[0].dataType, StringType)
    assert isinstance(spark_schema.fields[1].dataType, ArrayType)
    assert isinstance(spark_schema.fields[2].dataType, MapType)

