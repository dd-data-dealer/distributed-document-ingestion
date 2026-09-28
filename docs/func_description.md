### 1. Converting Pydantic Objects to Dictionaries

`chunk_document()` returns a list of `ValidatedChunk` Pydantic objects:

```python
chunks = [
    ValidatedChunk(
        document_id="abc",
        file_path="diet.pdf",
        chunk_id=0,
        text="chicken rice..."
    ),
    ValidatedChunk(
        document_id="abc",
        file_path="diet.pdf",
        chunk_id=1,
        text="thai curry..."
    )
]
```

Before creating a Pandas DataFrame, each Pydantic object is converted into a Python dictionary using `model_dump()`:

```python
for chunk in chunks:
    chunk_dict = chunk.model_dump()
    results.append(chunk_dict)
```

`model_dump()` converts:

```python
ValidatedChunk(
    document_id="abc",
    file_path="diet.pdf",
    chunk_id=0,
    text="chicken rice..."
)
```

into:

```python
{
    "document_id": "abc",
    "file_path": "diet.pdf",
    "chunk_id": 0,
    "text": "chicken rice..."
}
```

This conversion is required because `chunk_document()` uses Pydantic models to validate the chunk data contract, while `pd.DataFrame(results)` works naturally with dictionaries representing rows.

```text
ValidatedChunk
      |
      v
model_dump()
      |
      v
Python dictionary
      |
      v
results.append()
      |
      v
pd.DataFrame(results)
      |
      v
Spark DataFrame via mapInPandas
```