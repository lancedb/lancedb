# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

"""Measure fetch_blobs RSS in a fresh client process against real HTTP.

Run with the uv environment's Python. The server lives in a separate process so
payload generation and Arrow serialization are excluded from the client's RSS.
Each row contains 1 MiB; requests over the server's 64 MiB limit are split by the
SDK. Example: python benchmarks/bench_fetch_blobs_rss.py --rows 48 144
"""

import argparse
import json
import resource
import subprocess
import sys
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


def serve(batch_rows, row_mib):
    import pyarrow as pa

    payload = b"x" * (row_mib << 20)

    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, *args):
            pass

        def respond(self, status, data, content_type="application/json"):
            self.send_response(status)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(data)))
            self.send_header("phalanx-version", "0.5.0")
            self.send_header("x-lancedb-version", "1")
            self.end_headers()
            for offset in range(0, len(data), 8192):
                self.wfile.write(data[offset : offset + 8192])

        def do_POST(self):
            length = int(self.headers.get("Content-Length", 0))
            request_body = self.rfile.read(length)
            if self.path.endswith("/describe/"):
                self.do_GET()
                return
            body = json.loads(request_body)
            count = len(body["row_ids"])
            if count * row_mib > 64:
                self.respond(
                    400,
                    b"fetch_blobs accepts at most 67108864 total blob bytes",
                )
                return
            schema = pa.schema([pa.field("image", pa.large_binary())])
            sink = pa.BufferOutputStream()
            with pa.ipc.new_stream(sink, schema) as writer:
                for start in range(0, count, batch_rows):
                    array = pa.array(
                        [payload] * min(batch_rows, count - start),
                        type=pa.large_binary(),
                    )
                    writer.write_batch(pa.record_batch([array], schema=schema))
            self.respond(200, sink.getvalue(), "application/vnd.apache.arrow.stream")

        def do_GET(self):
            if "/blob/image/" in self.path:
                start, end = map(int, self.headers["Range"][6:].split("-"))
                body = memoryview(payload)[start : end + 1]
                self.send_response(206)
                self.send_header("Content-Length", str(len(body)))
                self.send_header("Content-Range", f"bytes {start}-{end}/{len(payload)}")
                self.send_header("x-lancedb-version", "1")
                self.end_headers()
                for offset in range(0, len(body), 8192):
                    self.wfile.write(body[offset : offset + 8192])
                return
            self.respond(
                200,
                json.dumps(
                    {
                        "table": "test",
                        "version": 1,
                        "schema": {"fields": []},
                    }
                ).encode(),
            )

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    print(server.server_port, flush=True)
    server.serve_forever()


def client(port, rows, row_mib):
    import lancedb

    db = lancedb.connect(
        "db://rss-test", api_key="test", host_override=f"http://127.0.0.1:{port}"
    )
    table = db.open_table("test")
    # Reset the Linux high-water mark after imports and connection setup.
    with open("/proc/self/clear_refs", "w") as reset:
        reset.write("5")

    def memory_kib(field):
        with open("/proc/self/status") as status:
            return next(
                int(line.split()[1]) for line in status if line.startswith(field + ":")
            )

    before = memory_kib("VmRSS")
    values = table.fetch_blobs("image", list(range(rows)))
    peak = memory_kib("VmHWM")
    assert len(values) == rows
    assert values.buffers()[2].size == rows * (row_mib << 20)
    # Inspect slices without copying the result to Python bytes.
    for i in (0, rows - 1):
        assert values.slice(i, 1).buffers()[2][i * (row_mib << 20)] == ord("x")
    print(
        json.dumps(
            {
                "rows": rows,
                "payload_mib": rows * row_mib,
                "baseline_rss_kib": before,
                "peak_rss_kib": peak,
                "peak_delta_kib": peak - before,
                "peak_delta_per_payload": (peak - before) / (rows * row_mib * 1024),
                "rusage_maxrss_kib": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
            }
        ),
        flush=True,
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rows", nargs="+", type=int, default=[48, 144])
    parser.add_argument("--batch-rows", type=int, default=48)
    parser.add_argument("--row-mib", type=int, default=1)
    parser.add_argument("--serve", action="store_true")
    parser.add_argument("--client", type=int)
    args = parser.parse_args()
    if args.serve:
        serve(args.batch_rows, args.row_mib)
    elif args.client is not None:
        client(args.client, args.rows[0], args.row_mib)
    else:
        server = subprocess.Popen(
            [
                sys.executable,
                __file__,
                "--serve",
                "--batch-rows",
                str(args.batch_rows),
                "--row-mib",
                str(args.row_mib),
            ],
            stdout=subprocess.PIPE,
            text=True,
        )
        try:
            port = server.stdout.readline().strip()
            for rows in args.rows:
                subprocess.run(
                    [
                        sys.executable,
                        __file__,
                        "--client",
                        port,
                        "--rows",
                        str(rows),
                        "--row-mib",
                        str(args.row_mib),
                    ],
                    check=True,
                )
        finally:
            server.terminate()
            server.wait()


if __name__ == "__main__":
    main()
