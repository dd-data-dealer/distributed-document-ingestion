from typing import Iterator

import pandas as pd
from pydantic import ValidationError

from app.schemas import ValidatedChunk


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
                ValidatedChunk.model_validate(payload)

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