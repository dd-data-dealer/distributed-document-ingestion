from app.chunker import chunk_text


def test_chunk_text_creates_multiple_chunks():
    text = " ".join(["word"] * 1000)

    chunks = chunk_text(
        text,
        chunk_size=600,
        overlap=75
    )

    assert len(chunks) == 2

def test_chunk_does_not_exceed_chunk_size():
    text = " ".join(["word"] * 1000)

    chunks = chunk_text(
        text,
        chunk_size=600,
        overlap=75
    )

    for chunk in chunks:
        assert len(chunk.split()) <= 600


def test_chunk_overlap():
    text = " ".join([f"word{i}" for i in range(1000)])

    chunks = chunk_text(
        text,
        chunk_size=600,
        overlap=75
    )

    first_chunk = chunks[0].split()
    second_chunk = chunks[1].split()

    assert first_chunk[-75:] == second_chunk[:75]


def test_empty_text_returns_no_chunks():
    chunks = chunk_text(
        "",
        chunk_size=600,
        overlap=75
    )

    assert chunks == []