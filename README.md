current decode() is intentional: this version validates:
binaryFile → mapInPandas → validation → Parquet execution, 
rather than performing real PDF extraction.

current pipeline:

Mac PDF
   ↓ volume mount ✓
Docker container
   ↓
Spark binaryFile ✓
   ↓
binary bytes ✓
   ↓
mapInPandas ✓
   ↓
Arrow/Pandas ✓
   ↓
validation ✓
   ↓
valid / DLQ branching ✓
   ↓
Parquet ✓
   ↓
mounted back to Mac ✓# distributed-document-ingestion
