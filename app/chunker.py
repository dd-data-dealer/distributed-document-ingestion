from app.schemas import ValidatedChunk

def chunk_text(text, chunk_size=600, overlap=75) -> list[str]:

    # 1. text must actually be usable
    if not text or not text.strip():
        return []

    # 2. chunk_size must be positive
    if chunk_size <= 0:
        raise ValueError("chunk_size must be greater than 0")

    # 3. overlap cannot be negative
    if overlap < 0:
        raise ValueError("overlap cannot be negative")

    # 4. overlap must be smaller than chunk_size
    if overlap >= chunk_size:
        raise ValueError("overlap must be smaller than chunk_size")
    words = text.split()
    s_idx=0
    n=len(words)
    results=[]

    while s_idx<n:
        e_idx=s_idx+chunk_size
        chunk=" ".join(words[s_idx:e_idx])
        results.append(chunk)
        s_idx+=chunk_size-overlap
    return results


def chunk_document(document_id, file_path, text):
    chunks = chunk_text(text)
    results = []

    for chunk_id, chunk in enumerate(chunks):
        validated_chunk = ValidatedChunk(
            document_id=document_id,
            file_path=file_path,
            chunk_id=chunk_id,
            text=chunk
        )

        results.append(validated_chunk)

    return results
