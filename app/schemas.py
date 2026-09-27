from pydantic import BaseModel, Field
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    BooleanType,
)


class ValidatedDocument(BaseModel):
    """Data contract for successfully processed PDF content."""

    file_path: str = Field(..., min_length=3)
    cleaned_text: str = Field(..., min_length=20)

#   kz 2026/27/09 ValidatedChunk added

class ValidatedChunk(BaseModel):
    """Data contract for a validated text chunk."""

    document_id: str = Field(..., min_length=1)
    file_path: str = Field(..., min_length=3)
    chunk_id: int = Field(..., ge=0)
    text: str = Field(..., min_length=20)

#   kz 2026/27/09 embedding tba later
# class EmbeddedChunk(ValidatedChunk):
#     embedding: list[float]


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