This project implements a Dockerized distributed PDF ingestion pipeline using PySpark. PDF files are loaded as binary data using Spark's binaryFile source, processed across Spark workers with mapInPandas, and parsed in memory using pypdf. Extracted text is cleaned and validated before being routed to either valid Parquet output or a Dead Letter Queue (DLQ). The current version verifies the complete PDF → Spark → Python worker → text extraction → validation → Parquet workflow; production hardening such as improved error handling, chunking, idempotency, metadata management, and downstream embedding generation will be added incrementally.
current pipeline:

PDF
 ↓
Docker
 ↓
Spark binaryFile
 ↓
mapInPandas
 ↓
pypdf
 ↓
REAL readable text
 ↓
validation
 ↓
Parquet
# distributed-document-ingestion
