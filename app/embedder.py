from sentence_transformers import SentenceTransformer


MODEL_NAME = "BAAI/bge-small-en-v1.5"


def embed_texts(texts: list[str]):
    ...

class EmbeddedChunk(ValidatedChunk):
    embedding: list[float]