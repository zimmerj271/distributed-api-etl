from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    TimestampType,
)


class BronzeSchema:

    @staticmethod
    def get_schema() -> StructType:
        return StructType([
            StructField(
                "request_id", 
                StringType(), 
                nullable=False,
                metadata={
                    "primary_key": True,
                    "comment": "API request unique identifier"
                }
            ),
            StructField(
                "row_hash", 
                StringType(), 
                nullable=True,
                metadata={
                    "comment": "Row hash to identify record has been loaded to the table."
                }
            ),
            StructField(
                "url", 
                StringType(), 
                nullable=False,
                metadata={
                    "comment": "Vendor endpoint URL"
                }
            ),
            StructField(
                "method", 
                StringType(), 
                nullable=False,
                metadata={
                    "comment": "Method for HTTP request"
                }
            ),
            StructField(
                "request_headers", 
                StringType(),
                metadata={
                    "comment": "Request headers"
                }
            ),
            StructField(
                "request_params", 
                StringType(),
                metadata={
                    "comment": "Request params"
                }
            ),
            StructField(
                "request_metadata", 
                StringType(),
                metadata={
                    "comment": "Request metadata"
                }
            ),
            StructField(
                "status_code", 
                StringType(),
                metadata={
                    "comment": "Request HTTP status code"
                }
            ),
            StructField(
                "response_headers", 
                StringType(),
                metadata={
                    "comment": "Response headers"
                }
            ),
            StructField(
                "body_text", 
                StringType(),
                metadata={
                    "comment": "Body text payload from response"
                }
            ),
            StructField(
                "success", 
                StringType(),
                metadata={
                    "comment": "HTTP request success flag"
                }
            ),
            StructField(
                "error_message", 
                StringType(),
                metadata={
                    "comment": "Error message from failed request"
                }
            ),
            StructField(
                "attempts", 
                StringType(),
                metadata={
                    "comment": "Number attempts made for request"
                }
            ),
            StructField(
                "response_metadata", 
                StringType(),
                metadata={
                    "comment": "Response metadata"
                }
            ),
            StructField(
                "_request_time", 
                TimestampType(),
                nullable=False,
                metadata={
                    "comment": "Time request was completed"
                }
            ),
        ])

    @staticmethod
    def get_columns() -> list[str]:
        return BronzeSchema.get_schema().fieldNames()


