# responsible for three things: embedding the query, c
# alculating cosine similarity between the query and stored embeddings,
# and returning the top-k chunks.

from .embedder import embed_texts


def cosine_similarity(vector_a, vector_b):
    return sum(
        a * b
        for a, b in zip(vector_a, vector_b)
    )


def retrieve(query, chunks, top_k=3):
    query_embedding = embed_texts([query])[0]

    scored_chunks = []

    for chunk in chunks:
        score = cosine_similarity(
            query_embedding,
            chunk["embedding"]
        )

        scored_chunks.append({
            "document_id": chunk["document_id"],
            "file_path": chunk["file_path"],
            "chunk_id": chunk["chunk_id"],
            "text": chunk["text"],
            "score": score,
        })

    scored_chunks.sort(
        key=lambda x: x["score"],
        reverse=True
    )

    return scored_chunks[:top_k]