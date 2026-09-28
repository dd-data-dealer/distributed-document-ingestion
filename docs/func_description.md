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

### 1. Embeddings

1. Adding data contract

```python

class EmbeddedChunk(ValidatedChunk):
    embedding: list[float]
```
getting
```text

ValidatedChunk
{
    document_id
    file_path
    chunk_id
    text
}
        ↓
embedding
        ↓
EmbeddedChunk
{
    document_id
    file_path
    chunk_id
    text
    embedding
}
```

2. Model selection
```text

BAAI/bge-small-en-v1.5

```
It produces 384-dimensional embeddings, is small enough for your local setup, and you've already encountered this model before.

3. embedder.py
```python

from sentence_transformers import SentenceTransformer

MODEL_NAME = "BAAI/bge-small-en-v1.5"

def embed_texts(texts: list[str]):
```
embedding in batch:
```text

[
 chunk1,
 chunk2,
 chunk3,
 ...
]
       ↓
     model
       ↓
[
 embedding1,
 embedding2,
 embedding3,
 ...
]
```
4. spark aapter

```text
parser.py
parse_partition()

chunker.py
chunk_text()
chunk_document()
chunk_partition()

embedder.py
embed_texts()
embed_partition()
```