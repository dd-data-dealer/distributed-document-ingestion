from app.chunker import chunk_text


def test_chunk_text_creates_multiple_chunks():
    text = " ".join(["word"] * 1000)

    chunks = chunk_text(
        text,
        chunk_size=600,
        overlap=75
    )

    assert len(chunks) == 2