#!/usr/bin/env python3
"""
Reusable script for running demo pipelines via spark-submit.

This script is designed to be submitted to Spark and can run any of the
demo pipeline configurations by specifying the config name.

Usage:
    spark-submit run_demo_pipeline.py --config noauth_demo
    spark-submit run_demo_pipeline.py --config basic_auth_demo
    spark-submit run_demo_pipeline.py --config bearer_token_demo
    spark-submit run_demo_pipeline.py --config oauth2_user_credentials_demo
    spark-submit run_demo_pipeline.py --config oauth2_client_credentials_demo

Optional arguments:
    --num-records: Number of records to generate (default: 100)
    --num-partitions: Number of DataFrame partitions (default: 4)
"""

import argparse
import sys
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import sha2, expr


def parse_args() -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Run demo ETL pipeline with specified configuration"
    )
    parser.add_argument(
        "--config",
        type=str,
        required=True,
        help="Name of the demo configuration (without .yml extension)",
    )
    parser.add_argument(
        "--num-records",
        type=int,
        default=100,
        help="Number of records to generate (default: 100)",
    )
    parser.add_argument(
        "--num-partitions",
        type=int,
        default=4,
        help="Number of DataFrame partitions (default: 4)",
    )
    parser.add_argument(
        "--config-dir",
        type=str,
        default="/opt/spark/app/configs/examples",
        help="Directory containing config files",
    )
    return parser.parse_args()


def create_source_dataframe(
    spark: SparkSession, num_records: int, num_partitions: int
):
    """Create a source DataFrame with unique tracking IDs."""
    return (
        spark.range(num_records)
        .repartition(num_partitions)
        .select(sha2(expr("uuid()"), 256).alias("tracking_id"))
    )


def main() -> int:
    """Main entry point for the demo pipeline runner."""
    args = parse_args()

    # Construct config path
    config_path = Path(args.config_dir) / f"{args.config}.yml"

    if not config_path.exists():
        print(f"Error: Configuration file not found: {config_path}")
        return 1

    # Create Spark session
    spark = SparkSession.builder.appName(f"{args.config}_pipeline").getOrCreate()

    try:
        # Import pipeline orchestrator
        from pipeline.orchestrator import run_pipeline

        # Create source DataFrame
        print(f"Creating source DataFrame with {args.num_records} records...")
        source_df = create_source_dataframe(
            spark, args.num_records, args.num_partitions
        )

        # Run the pipeline
        print(f"Running pipeline with config: {config_path}")
        run_pipeline(
            spark=spark,
            config_path=config_path,
            source_df=source_df,
            source_id="tracking_id",
        )

        print("Pipeline completed successfully")
        return 0

    except Exception as e:
        print(f"Pipeline failed with error: {e}")
        raise

    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
