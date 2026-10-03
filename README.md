This project implements a Dockerized distributed PDF ingestion pipeline using PySpark. PDF files are loaded as binary data using Spark's binaryFile source, processed across Spark workers with mapInPandas, and parsed in memory using pypdf.

Extracted text is cleaned and validated before being routed to either valid Parquet output or a Dead Letter Queue (DLQ).

The pipeline distinguishes between two types of failures:

**-> Record-level validation failures**- problems affecting an individual file, such as an empty file, invalid PDF, parsing failure, or missing extracted text. These records are routed to the DLQ while the rest of the batch continues processing.

**-> Pipeline-contract failures** - structural problems that make further processing unsafe or impossible, such as missing required Spark DataFrame columns (path or content). These errors intentionally fail the pipeline.

The current version verifies the complete PDF → Spark → Python worker → text extraction → validation → Parquet workflow. Production hardening such as improved observability, chunking, idempotency, metadata management, automated testing, and downstream embedding generation will be added incrementally.

**CHECK 1**  — File validation

Detect empty files.

Verify the PDF signature.

Reject invalid input before parsing.

**CHECK 2**  — Ingestion validation

Verify that the Spark input contains the required path and content columns.

Fail the pipeline when the ingestion contract is broken.

**CHECK 3** — Parsing validation

Verify that pypdf can parse the document.

Verify that pages can be read.

Verify that text was extracted.

Isolate parsing failures without stopping processing of other files.

**CHECK 4**  — Parsed output validation

Verify that required parser output columns are present.

Verify that successful records contain extracted text and no parsing error.

Verify that failed records contain an identifiable parsing error.

Fail the pipeline when the parser produces internally inconsistent output.

```text
PDF
 ↓
Docker
 ↓
Spark binaryFile
 ↓
Pipeline-contract validation
 ↓

mapInPandas
 ↓
pypdf
 ↓
Record-level input validation
 ↓
pypdf
 ↓
CHECK 3 — Parsing validation
 ↓
REAL readable text
 ↓
CHECK 4 — Parsed output validation
 ↓ 
Text cleaning
   |
   v
Record-level validation
   |
   +-----------------------------+
   |                             |
   v                             v
Valid documents              Invalid documents
   |                             |
   v                             v
Chunking  ----> Parguet         DLQ
   |
   v
Embedding                       DLQ
   |
   v
   v
Parquet

```

****RETRIEVAL IS SEPARATED****

```text

Failure strategy: individual bad documents should not stop the batch, while violations of the pipeline's structural contract should fail fast.
Retrieval is executed separately:

User query
   ↓
`query.py`
   ↓
Load stored embeddings
   ↓
`retrieval.py`
   ↓
Embed query
   ↓
Cosine similarity
   ↓
Top-K relevant chunks
   ↓
LLM + retrieved context
   ↓
clean, grounded answer

`pipeline.py` is responsible for preparing and storing the document data.

`retrieval.py` contains the retrieval logic, including cosine similarity and Top-K selection.

`query.py` is the retrieval entry point. It loads the previously generated embeddings and calls the retrieval functions without rerunning the ingestion pipeline.
```
# distributed-document-ingestion
