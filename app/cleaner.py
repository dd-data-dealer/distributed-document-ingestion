import re

import pandas as pd
import pyspark.sql.functions as F
from pyspark.sql.types import StringType


@F.pandas_udf(StringType())
def clean_text_udf(texts: pd.Series) -> pd.Series:
    """Remove basic text artifacts and normalize whitespace."""

    def strip_artifacts(value: str) -> str:
        if not value:
            return ""

        value = re.sub(r"<[^>]+>", " ", value)
        value = re.sub(r"\s+", " ", value)

        return value.strip()

    return texts.fillna("").apply(strip_artifacts)