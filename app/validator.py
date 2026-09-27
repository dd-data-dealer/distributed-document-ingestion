from typing import Iterator

import pandas as pd
from pydantic import ValidationError

from pyspark.sql import DataFrame
import pyspark.sql.functions as F

from app.schemas import ValidatedDocument

# validator.py
def validate_ingestion(raw_df):
    required_columns = {"path", "content"}
    missing_columns = required_columns - set(raw_df.columns)

    if missing_columns:
        raise ValueError(f"Missing required columns: {missing_columns}")


def validate_partition(
    iterator: Iterator[pd.DataFrame],
) -> Iterator[pd.DataFrame]:
    """Validate cleaned PDF content using the Pydantic data contract."""

    for batch_df in iterator:
        validated_batch = []

        for _, row in batch_df.iterrows():
            payload = {
                "file_path": row["file_path"],
                "cleaned_text": row["cleaned_text"],
            }

            try:
                # Validate each record on the Spark executor.
                ValidatedDocument.model_validate(payload)

                validated_batch.append({
                    **payload,
                    "is_valid": True,
                    "dlq_reason": None,
                })

            except ValidationError as err:
                validated_batch.append({
                    **payload,
                    "is_valid": False,
                    "dlq_reason": str(err).replace("\n", " "),
                })

        yield pd.DataFrame(validated_batch)

# CHECK 4
#   → Does parser output have the expected structure?

def validate_required_columns(df: DataFrame) -> None:
    """Verify that parser output contains the required columns."""
    required_columns = {
        "file_path",
        "raw_text",
        "parse_success",
        "parse_error",
    }

    missing_columns = required_columns - set(df.columns)

    if missing_columns:
        raise ValueError(
            f"Missing parser output columns: {sorted(missing_columns)}"
        )

# CHECK 4
#    → parse_success=True ⇒ raw_text exists, parse_error=None

def validate_successful_records(df: DataFrame) -> None:
    """Verify that successful parsing produced valid text."""

    invalid_records = df.filter(
        (F.col("parse_success") == True)
        & (
            F.col("raw_text").isNull()
            | (F.trim(F.col("raw_text")) == "")
            | F.col("parse_error").isNotNull()
        )
    )

    if invalid_records.limit(1).count() > 0:
        raise ValueError(
            "Successful parser record has invalid output."
        )


# CHECK 4
# → parse_success=False ⇒ parse_error exists

def validate_failed_records(df: DataFrame) -> None:
    """Verify that failed parsing contains an error reason."""

    invalid_records = df.filter(
        (F.col("parse_success") == False)
        & (
            F.col("parse_error").isNull()
            | (F.trim(F.col("parse_error")) == "")
        )
    )

    if invalid_records.limit(1).count() > 0:
        raise ValueError(
            "Failed parser record has no parse_error."
        )

# CHECK 4 all steps
def validate_parsed_output(df: DataFrame) -> None:
    """CHECK 4: Validate the complete parser output contract."""

    validate_required_columns(df)
    validate_successful_records(df)
    validate_failed_records(df)