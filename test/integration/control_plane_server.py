#!/usr/bin/env python3
"""
PostHog DuckDB Extension - Mock Control Plane Server

A simple HTTP server that mocks the PostHog control plane API for integration testing.
Returns a configured Flight SQL server endpoint in response to session creation requests.

Usage:
    python control_plane_server.py [--host HOST] [--port PORT] [--flight-endpoint ENDPOINT]

Requirements:
    Python 3.8+ (no external dependencies)
"""

import argparse
import json
import os
from http.server import HTTPServer, BaseHTTPRequestHandler
from datetime import datetime, timedelta


class ControlPlaneHandler(BaseHTTPRequestHandler):
    """HTTP request handler for mock control plane API."""

    # Set by main() based on command line args
    flight_endpoint = "grpc://127.0.0.1:8815"
    valid_tokens = {"demo", "test-token", "valid-api-key"}

    def do_POST(self):
        if self.path == "/v1/session":
            self._handle_create_session()
        else:
            self._send_error(404, f"Endpoint not found: {self.path}")

    def _handle_create_session(self):
        """Handle POST /v1/session - create a new session."""
        # Check Authorization header
        auth_header = self.headers.get("Authorization", "")
        if not auth_header.startswith("Bearer "):
            self._send_error(401, "Missing or invalid Authorization header")
            return

        token = auth_header[7:]  # Remove "Bearer " prefix
        if token not in self.valid_tokens:
            self._send_error(403, "Invalid API token")
            return

        # Parse request body
        content_length = int(self.headers.get("Content-Length", 0))
        if content_length == 0:
            self._send_error(400, "Missing request body")
            return

        try:
            body = self.rfile.read(content_length)
            request = json.loads(body.decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError) as e:
            self._send_error(400, f"Invalid JSON: {e}")
            return

        database = request.get("database", "")
        if not database:
            self._send_error(400, "Missing 'database' field in request body")
            return

        # Simulate database not found for specific test case
        if database == "nonexistent_db":
            self._send_error(404, f"Database '{database}' not found")
            return

        # Build response
        response = {
            "flight_endpoint": self.flight_endpoint,
            "session_token": f"session-{token}-{database}",
            "expires_at": (datetime.utcnow() + timedelta(hours=1)).isoformat() + "Z",
        }

        self._send_json(200, response)
        print(f"[ControlPlane] Created session for database '{database}' -> {self.flight_endpoint}")

    def _send_json(self, status_code: int, data: dict):
        """Send a JSON response."""
        body = json.dumps(data).encode("utf-8")
        self.send_response(status_code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _send_error(self, status_code: int, message: str):
        """Send an error response."""
        self._send_json(status_code, {"error": message})
        print(f"[ControlPlane] Error {status_code}: {message}")

    def log_message(self, format, *args):
        """Override to prefix log messages."""
        print(f"[ControlPlane] {args[0]}")


def main():
    parser = argparse.ArgumentParser(description="Mock Control Plane Server")
    parser.add_argument("--host", default="127.0.0.1", help="Host to bind to")
    parser.add_argument("--port", type=int, default=8080, help="Port to bind to")
    parser.add_argument(
        "--flight-endpoint",
        default=os.environ.get("FLIGHT_ENDPOINT", "grpc://127.0.0.1:8815"),
        help="Flight SQL server endpoint to return",
    )
    args = parser.parse_args()

    # Configure the handler
    ControlPlaneHandler.flight_endpoint = args.flight_endpoint

    server = HTTPServer((args.host, args.port), ControlPlaneHandler)
    print(f"[ControlPlane] Mock server started on http://{args.host}:{args.port}")
    print(f"[ControlPlane] Flight endpoint: {args.flight_endpoint}")
    print("[ControlPlane] Press Ctrl+C to stop")

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n[ControlPlane] Shutting down...")
        server.shutdown()


if __name__ == "__main__":
    main()
