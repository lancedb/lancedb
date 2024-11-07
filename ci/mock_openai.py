# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors
"""A zero-dependency mock OpenAI embeddings API endpoint for testing purposes."""
import argparse
import json
import http.server


class MockOpenAIRequestHandler(http.server.BaseHTTPRequestHandler):
    def do_POST(self):
        content_length = int(self.headers["Content-Length"])
        post_data = self.rfile.read(content_length)
        post_data = json.loads(post_data.decode("utf-8"))
        # See: https://platform.openai.com/docs/api-reference/embeddings/create

        if isinstance(post_data["input"], str):
            num_inputs = 1
        else:
            num_inputs = len(post_data["input"])

        model = post_data.get("model", "text-embedding-ada-002")

        data = []
        for i in range(num_inputs):
            data.append({
                "object": "embedding",
                "embedding": [0.1] * 1536,
                "index": i,
            })

        response = {
            "object": "list",
            "data": data,
            "model": model,
            "usage": {
                "prompt_tokens": 0,
                "total_tokens": 0,
            }
        }

        self.send_response(200)
        self.send_header("Content-type", "application/json")
        self.end_headers()
        self.wfile.write(json.dumps(response).encode("utf-8"))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Mock OpenAI embeddings API endpoint")
    parser.add_argument("--port", type=int, default=8000, help="Port to listen on")
    args = parser.parse_args()
    port = args.port

    print(f"server started on port {port}. Press Ctrl-C to stop.")
    print(f"To use, set OPENAI_BASE_URL=http://localhost:{port} in your environment.")

    with http.server.HTTPServer(("localhost", port), MockOpenAIRequestHandler) as server:
        server.serve_forever()
