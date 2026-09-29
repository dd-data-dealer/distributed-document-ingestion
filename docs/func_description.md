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

### 2. Embeddings

#### 2.0. Adding the data contract

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
The equivalent Spark schema is:

```python
EMBEDDING_SCHEMA = StructType([
    StructField("document_id", StringType(), False),
    StructField("file_path", StringType(), False),
    StructField("chunk_id", IntegerType(), False),
    StructField("text", StringType(), False),
    StructField("embedding", ArrayType(FloatType()), False),
])
```

`ArrayType(FloatType())` is used because one embedding is an array of floating-point numbers:

```text
[0.023, -0.117, 0.442, ...]
```

The Pydantic and Spark definitions therefore describe the same data:

```text
Pydantic                         Spark

list[float]        <-->          ArrayType(FloatType())
```

The Pydantic model validates Python objects inside the application, while the Spark schema defines the structure of the distributed DataFrame.

---

#### 2.1. Model selection
```text

BAAI/bge-small-en-v1.5

```
It produces 384-dimensional embeddings, is small enough for your local setup, and you've already encountered this model before.

#### 2.2. `embed_texts()` — batch inference

The embedding model is:

```text
BAAI/bge-small-en-v1.5
```

It produces 384-dimensional vectors.

`embed_texts()` receives multiple texts and embeds them in batches:

```python
def embed_texts(texts: list[str]) -> list[list[float]]:
    embeddings = model.encode(
        texts,
        batch_size=32,
        normalize_embeddings=True,
        show_progress_bar=False,
    )

    return embeddings.tolist()
```

```text
[chunk1, chunk2, chunk3]
          |
          v
     model.encode()
     batch_size=32
          |
          v
[embedding1, embedding2, embedding3]
```

Batch inference is more efficient than calling the model separately for every chunk.

`.tolist()` converts the NumPy output into regular Python lists compatible with `list[float]` and Spark's `ArrayType(FloatType())`.

---

#### 2.3. `embed_partition()` — Spark adapter

`chunks_df` is the input to the embedding stage:

```python
embedded_df = chunks_df.mapInPandas(
    embed_partition,
    schema=EMBEDDING_SCHEMA
)
```

Inside `embed_partition()`, all texts from the Pandas batch are extracted at once:

```python
texts = batch_df["text"].tolist()
embeddings = embed_texts(texts)
```

`.tolist()` converts the Pandas `Series` into:

```python
["chunk 1", "chunk 2", "chunk 3"]
```

The returned embeddings have the same order as the input texts.

`zip()` therefore pairs every original row with its embedding:

```python
for (_, row), embedding in zip(batch_df.iterrows(), embeddings):
```

```text
row 0  <->  embedding 0
row 1  <->  embedding 1
row 2  <->  embedding 2
```

The loop does not perform embedding again. It only combines each generated vector with its original chunk metadata to create an `EmbeddedChunk`.

The complete stage is:

```text
chunks_df
    |
    v
mapInPandas(embed_partition)
    |
    v
Pandas batch
    |
    v
batch_df["text"].tolist()
    |
    v
embed_texts()
    |
    v
batch embeddings
    |
    v
zip(rows, embeddings)
    |
    v
EmbeddedChunk
    |
    v
model_dump()
    |
    v
embedded_df
```

Embedding is kept separate from chunking so each stage has one responsibility:

```text
chunk_partition()  -> document -> chunks
embed_partition()  -> chunks -> chunks + embeddings