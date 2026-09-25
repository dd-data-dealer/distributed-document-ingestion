from typing import Iterator
import io

import pandas as pd
import pypdf


def parse_partition(
    iterator: Iterator[pd.DataFrame],
) -> Iterator[pd.DataFrame]:
    """Parse PDF files from Spark partitions into raw text."""

    for batch_df in iterator:
        results = []

        for _, row in batch_df.iterrows():
            path = row["path"]
            raw_bytes = row["content"]

            # Reject empty files before attempting PDF parsing.
            if not raw_bytes:
                results.append({
                    "file_path": path,
                    "raw_text": None,
                    "parse_success": False,
                    "parse_error": "EMPTY_FILE",
                })
                continue

            # Verify that the file starts with a PDF signature.
            if not bytes(raw_bytes).startswith(b"%PDF-"):
                results.append({
                    "file_path": path,
                    "raw_text": None,
                    "parse_success": False,
                    "parse_error": "INVALID_PDF",
                })
                continue

            try:
                # Convert PDF bytes into a readable stream and extract text.
                with io.BytesIO(raw_bytes) as pdf_stream:
                    reader = pypdf.PdfReader(pdf_stream)
                    pages = [
                        page.extract_text() or ""
                        for page in reader.pages
                    ]
                    full_text = "\n".join(pages).strip()

                # Reject PDFs from which no text could be extracted.
                if not full_text:
                    results.append({
                        "file_path": path,
                        "raw_text": None,
                        "parse_success": False,
                        "parse_error": "NO_TEXT",
                    })
                    continue

                results.append({
                    "file_path": path,
                    "raw_text": full_text,
                    "parse_success": True,
                    "parse_error": None,
                })

            except Exception as exc:
                results.append({
                    "file_path": path,
                    "raw_text": None,
                    "parse_success": False,
                    "parse_error": f"PARSE_ERROR: {str(exc)}",
                })

        # Return one DataFrame per processed batch.
        yield pd.DataFrame(results)