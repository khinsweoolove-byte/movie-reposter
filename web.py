"""Small HTTP server that keeps the hourly movie worker alive on Render."""
import asyncio
import json
import os
import threading
import time
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import movie_reposter

WORKER_STATUS = {"state": "starting", "error": ""}


def run_worker():
    try:
        WORKER_STATUS["state"] = "running"
        asyncio.run(movie_reposter.main())
        WORKER_STATUS["state"] = "finished"
    except Exception as exc:
        WORKER_STATUS["state"] = "error"
        WORKER_STATUS["error"] = type(exc).__name__ + ": " + str(exc)[:300]
        print("WORKER_ERR", WORKER_STATUS["error"], flush=True)


def self_ping():
    """Keep the free web service awake between hourly batches."""
    while True:
        time.sleep(600)
        url = os.getenv("RENDER_EXTERNAL_URL", "").rstrip("/")
        if not url:
            url = "https://movie-reposter.onrender.com"
        try:
            urllib.request.urlopen(url + "/health", timeout=20).read()
        except Exception as exc:
            print("SELF_PING_ERR", type(exc).__name__, flush=True)


threading.Thread(target=run_worker, daemon=True, name="movie-worker").start()
threading.Thread(target=self_ping, daemon=True, name="self-ping").start()


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path.startswith("/status"):
            body = json.dumps(WORKER_STATUS).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
        elif self.path.startswith("/health"):
            body = b"ok"
            self.send_response(200)
            self.send_header("Content-Type", "text/plain; charset=utf-8")
        else:
            self.send_response(404)
            self.end_headers()
            return
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args):
        pass


port = int(os.getenv("PORT", "10000"))
ThreadingHTTPServer(("0.0.0.0", port), Handler).serve_forever()
