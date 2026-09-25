from pydantic import BaseModel, Field
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    BooleanType,
)


class ValidatedChunk(BaseModel):
    """Data contract for successfully processed PDF content."""

    file_path: str = Field(..., min_length=3)
    cleaned_text: str = Field(..., min_length=20)


parser_schema = StructType([
    StructField("file_path", StringType(), False),
    StructField("raw_text", StringType(), True),
    StructField("parse_success", BooleanType(), False),
    StructField("parse_error", StringType(), True),
])


validation_schema = StructType([
    StructField("file_path", StringType(), False),
    StructField("cleaned_text", StringType(), False),
    StructField("is_valid", BooleanType(), False),
    StructField("dlq_reason", StringType(), True),
])