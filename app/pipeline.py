from pyspark.sql import SparkSession
import pyspark.sql.functions as F

from app.cleaner import clean_text_udf
from app.parser import parse_partition
from app.schemas import parser_schema, validation_schema
from app.validator import validate_partition

import os
INPUT_PATH = os.getenv("INPUT_PATH", "data/input")
OUTPUT_PATH = os.getenv("INPUT_PATH", "data/output")


def main():
    """Run the distributed PDF ingestion pipeline."""

    spark = (
        SparkSession.builder
        .appName("AIDataEngineering-ProductionPipeline")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .getOrCreate()
    )

    # 1. Load PDF files as binary data.
    raw_df = (
        spark.read
        .format("binaryFile")
        .load(INPUT_PATH)
    )

    # 2. Parse PDFs into raw text on Spark executors.
    parsed_df = (
        raw_df
        .select("path", "content")
        .mapInPandas(
            parse_partition,
            schema=parser_schema,
        )
    )

    # 3. Clean extracted text using an Arrow-based Pandas UDF.
    cleaned_df = parsed_df.withColumn(
        "cleaned_text",
        clean_text_udf(F.col("raw_text")),
    )

    # 4. Validate cleaned records on Spark executors.
    validated_df = (
        cleaned_df
        .select("file_path", "cleaned_text")
        .mapInPandas(
            validate_partition,
            schema=validation_schema,
        )
    )

    # Persist because the validated dataset is used by two write actions.
    validated_df.persist()

    # 5. Write valid records to the processed-data storage.
    (
        validated_df
        .filter(F.col("is_valid"))
        .select("file_path", "cleaned_text")
        .write
        .mode("append")
        .parquet(f"{OUTPUT_PATH}/valid_chunks")
    )

    # 6. Write rejected records to the dead-letter queue.
    (
        validated_df
        .filter(~F.col("is_valid"))
        .select("file_path", "cleaned_text", "dlq_reason")
        .write
        .mode("append")
        .parquet(f"{OUTPUT_PATH}/dlq_failed/")
    )

    validated_df.unpersist()
    spark.stop()


if __name__ == "__main__":
    main()