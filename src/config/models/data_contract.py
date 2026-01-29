"""
ETL Source and Sink Table Configuration

Desgin Decisions:
1. Source schema: OPTIONAL but RECOMMENDED
    - Validates input data structure
    - Documents expectations
    - Eanbles early failure detection
    - Helps with column mapping validation

2. Sink Schema: REQUIRED
    - Defines output data contract
    - Ensures type safety
    - Required for DataFrame schema creation
    - Documents what downstream consumers can expect

3. Column mapping: Links source columns to endpoint parameters and sink columns
"""
from pydantic import BaseModel, Field, field_validator, model_validator
from typing import Literal, Self 
from enum import Enum

from pyspark.sql import DataFrame
from pyspark.sql.types import StructType

from request_execution.models import RequestMapping
from core.bronze import BronzeSchema


class DataType(str, Enum):
    """Supported data types for schemas."""
    STRING = "string"
    INTEGER = "integer"
    LONG = "long"
    FLOAT = "float"
    DOUBLE = "double"
    BOOLEAN = "boolean"
    TIMESTAMP = "timestamp"
    DATE = "date"
    BINARY = "binary"
    ARRAY = "array"
    MAP = "map"
    STRUCT = "struct"


class ColumnSchema(BaseModel):
    """
    Schema definition for a single column.
    Represents both source and sink column schemas.
    """

    name: str = Field(..., description="Column name")
    type: DataType = Field(..., description="Data type")
    nullable: bool = Field(default=True, description="Whether column can be null")
    description: str | None = Field(default=None, description="Column description")

    # For complex data types
    element_type: DataType | None = Field(
        default=None,
        description="Element type for arrays"
    )
    key_type: DataType | None = Field(
        default=None,
        description="Key type for maps"
    )
    value_type: DataType | None = Field(
        default=None,
        description="Value type for maps"
    )

    @field_validator("name")
    @classmethod
    def validate_name(cls, v: str) -> str:
        """Ensure column names are valid identifiers"""
        if not v.replace("_", "").isalnum():
            raise ValueError(f"Column name must be alphanumeric with underscores: {v}")

        return v


class TableSchema(BaseModel):
    """
    Complete schema definition for a table.
    Can represent source or sink table schemas.
    """
    columns: list[ColumnSchema] = Field(..., description="List of column schemas")

    @field_validator("columns")
    @classmethod
    def validate_unique_columns(cls, v: list[ColumnSchema]) -> list[ColumnSchema]:
        """Ensure column names are unique."""
        names = [col.name for col in v]
        if len(names) != len(set(names)):
            duplicates = [name for name in names if names.count(name) > 1]
            raise ValueError(f"Duplicate column names: {set(duplicates)}")
        return v

    def get_column(self, name: str) -> ColumnSchema | None:
        """Get column schema by name."""
        for col in self.columns:
            if col.name == name:
                return col
        return None

    def column_names(self) -> list[str]:
        """Get list of all column names"""
        return [col.name for col in self.columns]

    def to_spark_schema(self) -> StructType:
        """
        Convert to PySpark StructType.
        Returns the Spark schema for DataFrame creation.
        """
        from pyspark.sql.types import (
            StructType, StructField, StringType, IntegerType, LongType,
            FloatType, DoubleType, BooleanType, TimestampType, DateType,
            BinaryType, ArrayType, MapType
        )

        type_mapping = {
            DataType.STRING: StringType(),
            DataType.INTEGER: IntegerType(),
            DataType.LONG: LongType(),
            DataType.FLOAT: FloatType(),
            DataType.DOUBLE: DoubleType(),
            DataType.BOOLEAN: BooleanType(),
            DataType.TIMESTAMP: TimestampType(),
            DataType.DATE: DateType(),
            DataType.BINARY: BinaryType()
        }

        fields = []
        for col in self.columns:
            if col.type == DataType.ARRAY:
                if col.element_type is None:
                    raise ValueError(f"Array column {col.name} must specify element_type")
                element = type_mapping[col.element_type]
                spark_type = ArrayType(element, containsNull=True)
            elif col.type == DataType.MAP:
                if col.key_type is None or col.value_type is None:
                    raise ValueError(f"Map column {col.name} must specify key_type and value_type")
                key, value = type_mapping[col.key_type], type_mapping[col.value_type]
                spark_type = MapType(key, value, valueContainsNull=True)
            else:
                spark_type = type_mapping.get(col.type)
                if spark_type is None:
                    raise ValueError(f"Unsupported type for column {col.name}: {col.type}")

            fields.append(StructField(col.name, spark_type, nullable=col.nullable))

        return StructType(fields)


class ColumnMapping(BaseModel):
    """
    Defines how source columns map to endpoint parameters.
    """
    source_column: str = Field(
        ...,
        description="Column name in source DataFrame"
    )
    endpoint_param: str | None = Field(
        default=None,
        description="Paramter name in endpoint (if used for API request)"
    )
    transform: str | None = Field(
        default=None,
        description="Optional transformation to apply (e.g. 'upper', 'lower', 'strip')"
    )


class SourceTableConfig(BaseModel):
    """
    Configuration for the source table (input to ETL pipeline).
    
    Schema is OPTIONAL but RECOMMENDED:
    - When provided: validates input DataFrame against expected structure
    - When omitted: accepts any DataFrame structure (more flexible but riskier)
    """
    name: str = Field(..., description="Source table/DataFrame name")
    namespace: str = Field(..., description="Namespace location identifier 'catalog.schema'")

    table_schema: TableSchema | None = Field(
        default=None,
        description="Expected schema (optional but recommended for validation)"
    )
    id_column: str = Field(
        default="tracking_id",
        description="Column containing unique identifier for record and API request tracking"
    )
    required_columns: list[str] = Field(
        default_factory=list,
        description="Columns that must be present (even if schema is not provided)"
    )    

    @property
    def identifier(self) -> str:
        return f"{self.namespace}.{self.name}"

    @model_validator(mode="after")
    def validate_id_column(self) -> Self:
        """Ensure id_column exists in schema if schema is provided."""
        if self.table_schema is not None:
            if self.table_schema.get_column(self.id_column) is None:
                raise ValueError(
                    f"id_column '{self.id_column}' not found in schema columns"
                )

        if self.id_column not in self.required_columns:
            self.required_columns.append(self.id_column)

        return self

    def validate_dataframe(self, df: DataFrame) -> tuple[bool, list[str]]:
        """
        Validate a DataFrame against the source config.
        
        Returns:
            (is_valid, error_message)
        """
        errors = []
        warnings = []
        df_columns = set(df.columns)

        for col in self.required_columns:
            if col not in df_columns:
                errors.append(f"Required column missing: {col}")

        if self.table_schema is not None:
            schema_columns = set(self.table_schema.column_names())

            missing = schema_columns - df_columns
            if missing:
                errors.append(f"Schema columns missing from DataFrame: {missing}")

            extra = df_columns - schema_columns
            if extra:
                warnings.append(f"Extra column found in DataFrame: {extra}")

        return (len(errors) == 0, errors)


class BronzeSinkConfig(BaseModel):
    """
    Configuration for bronze layer sink table.
    
    Schema is NOT configurable - it's always the standard bronze schema.
    This config only specifies WHERE to write and HOW (partitioning, properties).
    """
    name: str = Field(
        ...,
        description="Fully qualified table name (e.g., 'catalog.schema.bronze_api_data')"
    )
    namespace: str = Field(..., description="Namespace location identifier 'catalog.schema'")
    mode: Literal["append", "overwrite"] = Field(
        default="append",
        description="Write mode for sink table"
    )
    partition_by: list[str] | None = Field(
        default_factory=list,
        description="Columns to partition by (must be bronze schema columns)"
    )
    location: str | None = Field(
        default=None,
        description="External location for table data"
    )
    table_properties: dict[str, str] | None = Field(
        default_factory=dict,
        description="Delta table properties"
    )
    
    @field_validator("partition_by")
    @classmethod
    def validate_partition_columns(cls, v: list[str]) -> list[str]:
        """Ensure partition columns exist in bronze schema."""

        if not v:
            return v
        
        bronze_cols = set(col for col in BronzeSchema.get_columns())
        for part_col in v:
            if part_col not in bronze_cols:
                raise ValueError(
                    f"Partition column '{part_col}' not in bronze schema. "
                    f"Available columns: {sorted(bronze_cols)}"
                )
        return v

    @property
    def identifier(self) -> str:
        return f"{self.namespace}.{self.name}"

    @property
    def get_schema(self) -> StructType:
        return BronzeSchema.get_schema()


class EtlTableConfig(BaseModel):
    """
    Complete configuration for ETL source and sink tables.
    Defines the full data contract for the pipeline.
    """
    source: SourceTableConfig | None = Field(
        default=None, 
        description="Optional source table configuration"
    )
    sink: BronzeSinkConfig = Field(..., description="Sink table configuration")
    column_mappings: list[ColumnMapping] = Field(
        default_factory=list,
        description="How source columns map to endpoint params and sink columns"
    )
    
    @model_validator(mode="after")
    def validate_mappings(self) -> "EtlTableConfig":
        """Validate column mappings against source and sink schemas."""

        # Get available columns
        if self.source is not None:
            source_cols = (
                set(self.source.table_schema.column_names()) 
                if self.source.table_schema is not None 
                else None
            )
            
            for mapping in self.column_mappings:
                # Validate source column exists (if source schema provided)
                if source_cols is not None:
                    if mapping.source_column not in source_cols:
                        raise ValueError(
                            f"Mapping references unknown source column: {mapping.source_column}"
                        )
            
        return self

    def get_request_mapping(self) -> RequestMapping:
        param_map: dict[str, str] = {}

        for mapping in self.column_mappings:
            if mapping.endpoint_param:
                param_map[mapping.endpoint_param] = mapping.source_column

        return RequestMapping(param=param_map)
