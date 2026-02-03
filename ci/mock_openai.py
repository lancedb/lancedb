# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors
"""A mock OpenAI embeddings API endpoint for testing purposes."""
import argparse
import random

from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.routing import Route
import uvicorn

MODEL_DIMENSIONS = {
    "text-embedding-3-small": 512,
    "text-embedding-3-large": 3072,
}
DEFAULT_DIMENSIONS = 1536


async def embeddings(request: Request) -> JSONResponse:
    post_data = await request.json()

    if isinstance(post_data["input"], str):
        num_inputs = 1
    else:
        num_inputs = len(post_data["input"])

    model = post_data.get("model", "text-embedding-ada-002")
    dimensions = MODEL_DIMENSIONS.get(model, DEFAULT_DIMENSIONS)

    data = []
    for i in range(num_inputs):
        data.append({
            "object": "embedding",
            "embedding": [random.uniform(-1.0, 1.0) for _ in range(dimensions)],
            "index": i,
        })

    return JSONResponse({
        "object": "list",
        "data": data,
        "model": model,
        "usage": {
            "prompt_tokens": 0,
            "total_tokens": 0,
        },
    })


app = Starlette(routes=[
    Route("/", embeddings, methods=["POST"]),
])


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Mock OpenAI embeddings API endpoint")
    parser.add_argument("--port", type=int, default=8000, help="Port to listen on")
    args = parser.parse_args()
    port = args.port

    print(f"server started on port {port}. Press Ctrl-C to stop.")
    print(f"To use, set OPENAI_BASE_URL=http://localhost:{port} in your environment.")

    uvicorn.run(app, host="0.0.0.0", port=port)
