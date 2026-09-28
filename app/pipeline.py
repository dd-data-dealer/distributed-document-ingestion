from pyspark.sql import SparkSession
import pyspark.sql.functions as F

from app.cleaner import clean_text_udf
from app.parser import parse_partition
from app.schemas import parser_schema, validation_schema, CHUNK_SCHEMA
from app.validator import validate_partition, validate_ingestion, validate_parsed_output
from app.chunker import chunk_document, chunk_partition

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

    # CHECK 2: Validate ingestion structure.

    validate_ingestion(raw_df)

    # 2. Parse PDFs into raw text on Spark executors.
    parsed_df = (
        raw_df
        .select("path", "content")
        .mapInPandas(
            parse_partition,
            schema=parser_schema,
        )
    )

    # CHECK 4: Validate parser output contract.
    validate_parsed_output(parsed_df)

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
    # temp for tests s
    valid_df = spark.read.parquet("/app/data/output/valid_chunks/")
    valid_df.select("file_path").show(truncate=False)
    #
    # dlq_df = spark.read.parquet("/app/data/output/dlq_failed/")
    # dlq_df.select("file_path", "dlq_reason").show(truncate=False)
    # temp for tests e
    # NEXT TESTS STAGE: CHUNKING
    chunks_df = valid_df.mapInPandas(
        chunk_partition,
        schema=CHUNK_SCHEMA
    )
    print(f"Verify that chunk_id increments (0, 1, 2...), "
          f"chunks from the same PDF share the same document_id, "
          f"text looks sensible, and there are no empty chunks.")
    
    chunks_df.select(
        "document_id",
        "file_path",
        "chunk_id",
        "text"
    ).show(10, truncate=100)

    print(f"Chunk count: {chunks_df.count()}")

    validated_df.unpersist()
    spark.stop()

if __name__ == "__main__":
    main()