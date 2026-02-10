import logging

from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, DataType
from core.bronze import BronzeSchema
from config.models.data_contract import BronzeSinkConfig, TableSchema


class TableManager:
    """
    Responsible only for DDL lifecycle of Delta tables:
      - create table if not exists
      - validate schema compatibility
      - perform schema evolution if permitted
    """

    def __init__(self, spark: SparkSession) -> None:
        self.spark = spark
        self.schema = BronzeSchema.get_schema()
        self._logger = logging.getLogger(f"[{__class__.__name__}]")

    def table_exists(self, table_name: str) -> bool:
        return self.spark.catalog.tableExists(table_name)

    def _spark_type_str(self, dtype: DataType) -> str:
        return dtype.simpleString()

    def structtype_to_ddl(self, schema: StructType) -> str:
        column_lines = []

        for field in schema.fields:
            col_name = field.name
            col_type = self._spark_type_str(field.dataType)

            nullable = "" if field.nullable else "NOT NULL"

            comment = field.metadata.get("comment")
            comment_sql = f"COMMENT '{comment}'" if comment else ""

            # compose single column line
            parts = [col_name, col_type, nullable]
            if comment_sql:
                parts.append(comment_sql)

            column_lines.append(" ".join(parts))

        return ",\n  ".join(column_lines)

    def create_database(self, cfg: BronzeSinkConfig) -> None:
        """
        Create database in metastore.

        Args:
            cfg: Pydantic configuration defined by BronzeSinkConfig
        """
        database_name = cfg.namespace.partition('.')[2] or cfg.namespace

        self._logger.info(f"Creating database {database_name}")
        self.spark.sql(f"CREATE DATABASE IF NOT EXISTS {database_name}")

    def create_table(
        self,
        cfg: BronzeSinkConfig,
    ) -> None:
        """
        Create Delta table if it does not exist.

        Args:
            cfg: Pydantic configuration defined by BronzeSinkConfig
        """

        self.create_database(cfg)

        table_name = cfg.identifier
        table_exists = self.table_exists(table_name)
        if table_exists and cfg.mode != "overwrite":
            self._logger.info(f"Table already exists: {table_name}")
            return
        elif table_exists and (cfg.mode == "overwrite"):
            self._logger.warning(
                f"Sink table OVERWRITE enabled. Table {cfg.name} will be replaced."
            )
            create_ddl = "CREATE OR REPLACE TABLE"
        else:
            create_ddl = "CREATE TABLE IF NOT EXISTS"

        ddl_schema = self.structtype_to_ddl(self.schema)

        location_clause = f"LOCATION '{cfg.location}'" if cfg.location else ""

        partition_clause = ""
        if cfg.partition_by:
            partition_cols = ", ".join(cfg.partition_by)
            partition_clause = f"PARTITIONED by ({partition_cols})"

        properties_clause = ""
        if cfg.table_properties:
            properties = ", ".join(
                f"'{k}' = '{v}'" for k, v in cfg.table_properties.items()
            )
            properties_clause = f"TBLPROPERTIES ({properties})"

        self.spark.sql(
            f"""
            {create_ddl} {table_name} (
                {ddl_schema}
            )
            USING DELTA
            {partition_clause}
            {location_clause}
            {properties_clause}
            """
        )
        self._logger.info(f"Created Delta table: {table_name}")

    def _enrich_schema_with_comments(
        self, spark_schema: StructType, config_schema: TableSchema
    ) -> StructType:
        """Add column descriptions as comments to Spark schema metadata"""
        enriched_fields = []

        for spark_field in spark_schema.fields:
            config_col = config_schema.get_column(spark_field.name)

            if config_col and config_col.description:
                metadata = spark_field.metadata.copy()
                metadata["comment"] = config_col.description

                enriched_field = StructField(
                    name=spark_field.name,
                    dataType=spark_field.dataType,
                    nullable=spark_field.nullable,
                    metadata=metadata,
                )
                enriched_fields.append(enriched_field)
            else:
                enriched_fields.append(spark_field)

        return StructType(enriched_fields)

    def validate_schema(self, table_name: str, expected: StructType) -> None:
        """
        Validate existing table schema against expected schema.
        Returns validation result with errors and warnings.
        """
        if not self.table_exists(table_name):
            ve = ValueError(f"Table '{table_name}' does not exist")
            self._logger.error(str(ve))
            raise ve

        existing = self.spark.table(table_name).schema
        errors = []

        expected_fields = {f.name: f for f in expected.fields}
        existing_fields = {f.name: f for f in existing.fields}

        # Check for missing columns
        missing_cols = set(expected_fields.keys()) - set(existing_fields.keys())
        if missing_cols:
            errors.append(f"Missing required columns: {missing_cols}")

        # Check for extra columns (in existing but not expected)
        extra_cols = set(existing_fields.keys()) - set(expected_fields.keys())
        if extra_cols:
            self._logger.warning(f"Extra columns in table {table_name}: {extra_cols}")

        # Check column compatibility
        for col_name in set(existing_fields.keys()) & set(expected_fields.keys()):
            existing_field = existing_fields[col_name]
            expected_field = expected_fields[col_name]

            # Check data type
            if existing_field.dataType != expected_field.dataType:
                errors.append(
                    f"Type mismatch for '{col_name}': "
                    f"existing={existing_field.dataType.simpleString()}, "
                    f"expected={expected_field.dataType.simpleString()}"
                )

            # Check nullability
            if existing_field.nullable != expected_field.nullable:
                if existing_field.nullable and not expected_field.nullable:
                    # Existing is nullable, expected is NOT NULL
                    errors.append(f"Cannot make nullable column '{col_name}' NOT NULL")
                else:
                    # Existing is NOT NULL, expected is nullable
                    self._logger.warning(
                        f"Column '{col_name}' is NOT NULL in table but nullable in config. "
                        f"This is backwards compatible but may indicate config drift."
                    )

        if errors:
            error_msg = "\n".join(errors)
            self._logger.error(error_msg)
            raise ValueError(f"Schema incompatible for {table_name}:\n{error_msg}\n\n")
