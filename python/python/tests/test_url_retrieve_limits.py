import pytest
import http.server
import threading
from lancedb.embeddings.utils import url_retrieve

class MockImageHandler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/valid.png":
            self.send_response(200)
            self.send_header("Content-Type", "image/png")
            self.send_header("Content-Length", "10")
            self.end_headers()
            self.wfile.write(b"x" * 10)
        elif self.path == "/huge.png":
            self.send_response(200)
            self.send_header("Content-Type", "image/png")
            self.send_header("Content-Length", str(100 * 1024 * 1024))
            self.end_headers()
            self.wfile.write(b"x" * 1024)
        elif self.path == "/chunked_huge.png":
            self.send_response(200)
            self.send_header("Content-Type", "image/png")
            self.end_headers()
            for _ in range(20):
                self.wfile.write(b"x" * (1024 * 1024))

def test_url_retrieve_scheme_safety():
    with pytest.raises(ValueError, match="Unsupported URL scheme"):
        url_retrieve("file:///etc/passwd")

def test_url_retrieve_size_limit():
    server = http.server.HTTPServer(("127.0.0.1", 0), MockImageHandler)
    port = server.server_port
    t = threading.Thread(target=server.serve_forever)
    t.daemon = True
    t.start()
    
    # 1. Valid fetch works
    res = url_retrieve(f"http://127.0.0.1:{port}/valid.png", timeout=5, max_bytes=100)
    assert len(res) == 10
    
    # 2. Rejects Content-Length header exceeding limit
    with pytest.raises(ValueError, match="exceeds maximum allowed size"):
        url_retrieve(f"http://127.0.0.1:{port}/huge.png", timeout=5, max_bytes=1000)
        
    # 3. Rejects streaming chunks exceeding limit
    with pytest.raises(ValueError, match="exceeded maximum allowed size"):
        url_retrieve(f"http://127.0.0.1:{port}/chunked_huge.png", timeout=5, max_bytes=5 * 1024 * 1024)
        
    server.shutdown()
