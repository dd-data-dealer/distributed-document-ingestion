# app/rag.py

from openai import OpenAI
from dotenv import load_dotenv


# The SDK reads OPENAI_API_KEY from the environment.!
load_dotenv()

client = OpenAI()


def generate_recipe(query: str, retrieved_chunks: list[dict]) -> str:

    context = "\n\n".join(
        chunk["text"] for chunk in retrieved_chunks
    )

    prompt = f"""
You are a recipe extraction assistant.

The retrieved context may contain multiple recipes, nutritional
information, calculations, recommendations, and unrelated text.

Find the 3 recipes in the context that best answers the user's request.

Return ONLY those recipes.
If you find less then 3 recipes, provide maximum number of available recipes following below instruction.

Preserve information from the source recipe and include:
- recipe name
- ingredients
- preparation instructions
- macro in format P: F: C: Total Kcal:

Ignore:
- unrelated recipes
- nutritional theory
- glycemic index calculations
- tables
- recommendations appearing before or after the recipe

Do not invent missing ingredients or instructions.
If no matching recipe exists in the context, say that no matching
recipe was found.

Answer in the same language as the user's request.
{query}

RETRIEVED CONTEXT:
{context}
"""

    response = client.responses.create(
        model="gpt-4o-mini",
        input=prompt
    )

    return response.output_text