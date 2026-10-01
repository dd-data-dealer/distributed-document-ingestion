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

***ONLY ENGLISH SUPPORTED***
```text

BAAI/bge-small-en-v1.5

```

It produces 384-dimensional embeddings, is small enough for your local setup, and you've already encountered this model before.

#### 2.2. `embed_texts()` — batch inference

`embed_texts()` receives multiple texts and embeds them in batches:


***!!!! IT WAS WRONG !!!!***

old:
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
end old

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
```

The embedding model is not initialized directly when `embedder.py` is imported. Instead, it is loaded lazily using `get_model()`:

```python
MODEL_NAME = "intfloat/multilingual-e5-small"
_model = None


def get_model():
    global _model

    if _model is None:
        _model = SentenceTransformer(
            MODEL_NAME,
            device="cpu"
        )

    return _model
```

Spark Python workers run as separate processes and do not share Python memory. Therefore, one model instance created by the driver cannot simply be shared by all workers.

Using `_model = None` delays model initialization until a worker actually needs to generate embeddings.

The first call to `get_model()` creates the model:

```text
_model = None
      |
      v
get_model()
      |
      v
_model is None
      |
      v
SentenceTransformer(...)
      |
      v
model stored in _model
```

Later calls from the same Python worker reuse the already initialized model instead of loading it again:

```text
Spark Python Worker
      |
      v
get_model()
      |
      v
Load model once
      |
      +----> embed batch 1
      +----> embed batch 2
      +----> embed batch 3
```

The model uses `device="cpu"` explicitly because Apple Metal/MPS caused crashes when the model was initialized inside Spark Python workers on macOS.

For Polish documents, `intfloat/multilingual-e5-small` is used because it is a multilingual embedding model suitable for semantic retrieval across languages, including Polish.




The `embed_texts()` function converts a list of text chunks into embedding vectors:

```python
def embed_texts(texts: list[str]):
    model = get_model()

    embeddings = model.encode(
        texts,
        batch_size=32,
        normalize_embeddings=True,
        show_progress_bar=False,
    )

    return embeddings.tolist()
```

Instead of encoding every chunk separately, the function sends multiple texts to the model as a batch:

```text
["chunk 1", "chunk 2", "chunk 3"]
              |
              v
        model.encode()
              |
              v
[embedding 1, embedding 2, embedding 3]
```

`batch_size=32` allows the model to process several chunks together, which is more efficient than calling the model separately for every row.

`normalize_embeddings=True` normalizes each vector to length 1, making the embeddings suitable for similarity calculations such as cosine similarity.

`embeddings.tolist()` converts the NumPy output returned by SentenceTransformer into standard Python lists that can be stored in Spark using `ArrayType(FloatType())`.




The `embed_partition()` function connects Spark processing with the embedding model:

```python
def embed_partition(iterator: Iterator[pd.DataFrame]) -> Iterator[pd.DataFrame]:
    for batch_df in iterator:
        results = []

        txt_for_embedding = batch_df["text"].tolist()
        embeddings = embed_texts(txt_for_embedding)

        for (, row), embedding in zip(batch_df.iterrows(), embeddings):
            embedded_chunk = EmbeddedChunk(
                document_id=row["document_id"],
                file_path=row["file_path"],
                chunk_id=row["chunk_id"],
                text=row["text"],
                embedding=embedding,
            )

            results.append(embedded_chunk.model_dump())

        yield pd.DataFrame(results)
```

Spark passes batches of chunk rows to `embed_partition()` through `mapInPandas`.

First, the text column is extracted from the Pandas batch:

```python
txt_for_embedding = batch_df["text"].tolist()
```

All texts in the batch are embedded together:

```python
embeddings = embed_texts(txt_for_embedding)
```

`zip()` then reconnects each original chunk with its corresponding embedding:

```python
for (, row), embedding in zip(batch_df.iterrows(), embeddings):
```

Each result is validated using the `EmbeddedChunk` Pydantic model and converted to a dictionary using `model_dump()` before creating the output Pandas DataFrame.

The complete flow is:

```text
Spark chunks DataFrame
        |
        v
mapInPandas(embed_partition)
        |
        v
Pandas batch
        |
        v
extract text column
        |
        v
embed_texts()
        |
        v
get_model()
        |
        v
SentenceTransformer
        |
        v
embedding vectors
        |
        v
EmbeddedChunk validation
        |
        v
model_dump()
        |
        v
pd.DataFrame(results)
        |
        v
Spark embeddings DataFrame
```
