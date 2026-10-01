from sentence_transformers import SentenceTransformer
from .schemas import EmbeddedChunk
from typing import Iterator
import pandas as pd


MODEL_NAME = "intfloat/multilingual-e5-small"
# model = SentenceTransformer(MODEL_NAME)
# KZ 1.10 lazy initialization WAS inside the worker process, not it gives us once per Python worker:
_model = None

def get_model():
    global _model

    if _model is None:
        _model = SentenceTransformer(
            MODEL_NAME,
            device="cpu")

    return _model

def embed_texts(texts: list[str]):
    model = get_model()
    embeddings = model.encode(
        texts,
        batch_size=32,
        normalize_embeddings=True,
        show_progress_bar=False,
    )

    return embeddings.tolist()


def embed_partition(
    iterator: Iterator[pd.DataFrame],
) -> Iterator[pd.DataFrame]:
    """embed text chunks from Spark partitions into vectors."""

    for batch_df in iterator:
        results = []
        #  process whole embeddings list not separate embedding!
        txt_for_embedding=batch_df["text"].tolist()
        embeddings=embed_texts(txt_for_embedding)


        for (_, row), embedding in zip(batch_df.iterrows(), embeddings):
            embedded_chunk = EmbeddedChunk(
                document_id=row["document_id"],
                file_path=row["file_path"],
                chunk_id=row["chunk_id"],
                text=row["text"],
                embedding=embedding,
            )

            results.append(embedded_chunk.model_dump())


        # Return one DataFrame per processed batch.
        yield pd.DataFrame(results)