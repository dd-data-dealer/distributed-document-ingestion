# app/rag.py

from openai import OpenAI

client = OpenAI()


def generate_recipe(query: str, retrieved_chunks: list[dict]) -> str:

    context = "\n\n".join(
        chunk["text"] for chunk in retrieved_chunks
    )

    prompt = f"""
You are a recipe assistant.

Answer the user's request using only the information provided
in the retrieved context.

Return ONE recipe that best matches the user's request.

Include:
- Recipe name
- Ingredients
- Instructions

Do not include unrelated recipes.
Do not invent information that is not present in the context.

USER REQUEST:
{query}

RETRIEVED CONTEXT:
{context}
"""

    response = client.responses.create(
        model="gpt-4o-mini",
        input=prompt
    )

    return response.output_text