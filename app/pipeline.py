from typing import Iterator
import re
import pandas as pd
from pydantic import BaseModel, Field, ValidationError
import pypdf
import io


from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, BooleanType
import pyspark.sql.functions as F


# --- 1. KONTRAKT PYDANTIC (Weryfikowany wewnątrz workerów) ---
class ValidatedChunk(BaseModel):
    file_path: str = Field(..., min_length=3)
    cleaned_text: str = Field(..., min_length=20)


# --- 2. UDF DO CZYSZCZENIA (Arrow / pandas_udf) ---
@F.pandas_udf(StringType())
def clean_text_udf(texts: pd.Series) -> pd.Series:
    def strip_artifacts(val: str) -> str:
        if not val:
            return ""
        val = re.sub(r"<[^>]+>", " ", val)
        val = re.sub(r"\s+", " ", val)
        return val.strip()

    return texts.fillna("").apply(strip_artifacts)


# --- 3. PARSOWANIE PARTYCJI (mapInPandas) ---
parser_schema = StructType([
    StructField("file_path", StringType(), False),
    StructField("raw_text", StringType(), False)
])

def parse_partition(iterator: Iterator[pd.DataFrame]) -> Iterator[pd.DataFrame]:
    for batch_df in iterator:
        results = []
        for _, row in batch_df.iterrows():
            path = row["path"]
            raw_bytes = row["content"]
            try:
                # decoded = raw_bytes.decode("utf-8", errors="ignore").strip()
                with io.BytesIO(raw_bytes) as pdf_stream:
                    reader = pypdf.PdfReader(pdf_stream)
                    pages = [page.extract_text() or "" for page in reader.pages]
                    full_text = "\n".join(pages).strip()

                results.append({"file_path": path, "raw_text": full_text})
            except Exception as e:
                results.append({"file_path": path, "raw_text": f"PARSE_ERROR: {str(e)}"})
        yield pd.DataFrame(results)


# --- 4. ROZPROSZONA WALIDACJA PYDANTIC (mapInPandas) ---
validation_schema = StructType([
    StructField("file_path", StringType(), False),
    StructField("cleaned_text", StringType(), False),
    StructField("is_valid", BooleanType(), False),
    StructField("dlq_reason", StringType(), True)
])

def validate_partition(iterator: Iterator[pd.DataFrame]) -> Iterator[pd.DataFrame]:
    for batch_df in iterator:
        validated_batch = []
        for _, row in batch_df.iterrows():
            payload = {
                "file_path": row["file_path"],
                "cleaned_text": row["cleaned_text"]
            }
            try:
                # Walidacja Pydantic V2 na poziomie executora
                ValidatedChunk.model_validate(payload)
                validated_batch.append({
                    "file_path": payload["file_path"],
                    "cleaned_text": payload["cleaned_text"],
                    "is_valid": True,
                    "dlq_reason": None
                })
            except ValidationError as err:
                validated_batch.append({
                    "file_path": payload["file_path"],
                    "cleaned_text": payload["cleaned_text"],
                    "is_valid": False,
                    "dlq_reason": str(err).replace("\n", " ")
                })
        yield pd.DataFrame(validated_batch)


def main():
    spark = SparkSession.builder \
        .appName("AIDataEngineering-ProductionPipeline") \
        .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
        .getOrCreate()

    # 1. Wczytanie plików jako strumienie bajtów (Lazy Plan)
    # W warunkach chmurowych podajesz ścieżkę: "s3a://twoj-bucket/raw-files/*"
    df_raw = spark.read.format("binaryFile").load("/app/data/input/")

    # 2. Parsowanie wsadowe na executorach
    parsed_df = df_raw.select("path", "content").mapInPandas(
        parse_partition,
        schema=parser_schema
    )

    # 3. Czyszczenie tekstu przez Arrow UDF
    cleaned_df = parsed_df.withColumn("cleaned_text", clean_text_udf(F.col("raw_text")))

    # 4. Rozproszona walidacja Pydantic na workerach
    validated_df = cleaned_df.select("file_path", "cleaned_text").mapInPandas(
        validate_partition,
        schema=validation_schema
    )

    # Cache pośredni, bo będziemy zapisywać do dwóch osobnych ścieżek
    validated_df.persist()

    # 5. Prawdziwa akcja rozproszona A: Zapis poprawnych danych (Data Lake / S3)
    (
        validated_df
        .filter(F.col("is_valid") == True)
        .select("file_path", "cleaned_text")
        .write
        .mode("append")
        .parquet("/app/data/output/valid_chunks/")
    )

    # 6. Prawdziwa akcja rozproszona B: Zapis odrzutów (Dead Letter Queue)
    (
        validated_df
        .filter(F.col("is_valid") == False)
        .select("file_path", "cleaned_text", "dlq_reason")
        .write
        .mode("append")
        .parquet("/app/data/output/dlq_failed/")
    )

    validated_df.unpersist()
    spark.stop()


if __name__ == "__main__":
    main()