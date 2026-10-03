from pyspark.sql import SparkSession

from .retrieval import retrieve
from .rag import generate_recipe

import os
from pathlib import Path

# Points to the 'pdf-pipeline-w1-t1' folder
PROJECT_ROOT = Path(__file__).resolve().parent.parent

# Default to relative paths anchored to the project folder
OUTPUT_PATH = os.getenv("OUTPUT_PATH", str(PROJECT_ROOT / "data" / "output"))

EMBEDDINGS_PATH = os.path.join(OUTPUT_PATH, "embeddings")


spark = SparkSession.builder \
    .appName("DocumentRetrieval") \
    .master("local[*]")\
    .config("spark.driver.bindAddress", "127.0.0.1")\
    .config("spark.driver.host", "127.0.0.1")\
    .getOrCreate()

embeddings_df = spark.read.parquet(EMBEDDINGS_PATH)

chunks = [
    row.asDict()
    for row in embeddings_df.collect()
]


query = "Jaki posiłek zawiera kurczaka?"

results = retrieve(
    query=query,
    chunks=chunks,
    top_k=3
)

answer = generate_recipe(
    query=query,
    retrieved_chunks=results)

print(answer)

# just for chunk testing
# for result in results:
#     print("\nScore:", result["score"])
#     print("File:", result["file_path"])
#     print("Chunk:", result["chunk_id"])
#     print("Text:", result["text"])