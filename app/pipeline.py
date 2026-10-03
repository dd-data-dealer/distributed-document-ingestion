from pyspark.sql import SparkSession
import pyspark.sql.functions as F

from .cleaner import clean_text_udf
from .parser import parse_partition
from .schemas import parser_schema, validation_schema, CHUNK_SCHEMA, EMBEDDING_SCHEMA
from .validator import validate_partition, validate_ingestion, validate_parsed_output
from .chunker import chunk_document, chunk_partition
from .embedder import embed_partition

import os
from pathlib import Path

# Points to the 'pdf-pipeline-w1-t1' folder
PROJECT_ROOT = Path(__file__).resolve().parent.parent

# Default to relative paths anchored to the project folder
INPUT_PATH = os.getenv("INPUT_PATH", str(PROJECT_ROOT / "data" / "input"))
OUTPUT_PATH = os.getenv("OUTPUT_PATH", str(PROJECT_ROOT / "data" / "output"))


def main():
    """Run the distributed PDF ingestion pipeline."""

    spark = (
        SparkSession.builder
        .appName("AIDataEngineering-ProductionPipeline")
        .master("local[1]")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        # .config("spark.sql.execution.arrow.pyspark.enabled", "true")
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
        .mode("overwrite")
        .parquet(f"{OUTPUT_PATH}/valid_chunks")
    )

    # 6. Write rejected records to the dead-letter queue.
    (
        validated_df
        .filter(~F.col("is_valid"))
        .select("file_path", "cleaned_text", "dlq_reason")
        .write
        .mode("overwrite")
        .parquet(f"{OUTPUT_PATH}/dlq_failed/")
    )
    # temp for tests s
    valid_df = spark.read.parquet(f"{OUTPUT_PATH}/valid_chunks/")
    valid_df.select("file_path").show(truncate=False)
    #
    # dlq_df = spark.read.parquet(f"{OUTPUT_PATH}/dlq_failed/")
    # dlq_df.select("file_path", "dlq_reason").show(truncate=False)
    # temp for tests e
    # NEXT TESTS STAGE: CHUNKING
    chunks_df = valid_df.mapInPandas(
        chunk_partition,
        schema=CHUNK_SCHEMA
    )
    # 1.10 KZ here temp turned ON

    print(f"Verify that chunk_id increments (0, 1, 2...), "
          f"chunks from the same PDF share the same document_id, "
          f"text looks sensible, and there are no empty chunks.")

    # chunks_df.select(
    #     "document_id",
    #     "file_path",
    #     "chunk_id",
    #     "text"
    # ).show(10, truncate=True)
    # TRUNCATE 100

    # # 1.10 KZ here temp turned off

    embedded_df = chunks_df.mapInPandas(
        embed_partition,
        schema=EMBEDDING_SCHEMA
    )

    embedded_df.write \
        .mode("overwrite") \
        .parquet(f"{OUTPUT_PATH}/embeddings/")
    #  29 09 2026 embdedding check

    # # 1. Check number of embedded chunks
    # print(f"Embedded chunk count: {embedded_df.count()}")
    #
    # # not need as we already created embedded_df
    # # embeddings_check = spark.read.parquet(EMBEDDINGS_OUTPUT_PATH)
    #
    # 2. Inspect a few results
    embedded_df.select(
        "document_id",
        "chunk_id",
        "text",
        "embedding"
    ).show(3, truncate=False)
    #
    # # 3. Check embedding dimensions
    # embedded_df.selectExpr(
    #     "chunk_id",
    #     "size(embedding) AS embedding_dimension"
    # ).show(5)
    #
    # # 4. Check for missing embeddings
    # missing_embeddings = embedded_df.filter(
    #     "embedding IS NULL OR size(embedding) = 0"
    # ).count()
    #
    # print(f"Missing embeddings: {missing_embeddings}")
    # # end embedding text
    #
    # print(f"Chunk count: {chunks_df.count()}")

    validated_df.unpersist()
    spark.stop()

if __name__ == "__main__":
    main()