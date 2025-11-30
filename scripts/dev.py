#!/usr/bin/env python3
import argparse
import os
import socket
import subprocess
import sys


def find_free_port(start: int = 8511, end: int = 8599) -> int:
    for port in range(start, end + 1):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            try:
                s.bind(("127.0.0.1", port))
                return port
            except OSError:
                continue
    raise RuntimeError(f"No free port found in range {start}-{end}")


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description="Run Streamlit app in dev mode with auto-reload.")
    parser.add_argument("--port", type=int, default=None, help="Port to use (auto if omitted)")
    parser.add_argument("--headless", action="store_true", help="Run headless (default: false)")
    parser.add_argument("--watcher", default="auto", choices=["auto", "watchdog", "poll"], help="File watcher type")
    parser.add_argument("--log-level", default="debug", choices=["debug", "info", "warning", "error"])
    args = parser.parse_args(argv)

    port = args.port or find_free_port()

    env = os.environ.copy()
    # Encourage fast reloads in dev
    env.setdefault("STREAMLIT_SERVER_RUN_ON_SAVE", "true")

    cmd = [
        sys.executable,
        "-m",
        "streamlit",
        "run",
        "app.py",
        f"--server.headless={'true' if args.headless else 'false'}",
        f"--server.port={port}",
        f"--server.fileWatcherType={args.watcher}",
        f"--logger.level={args.log_level}",
    ]

    print(f"Starting Streamlit dev server on http://localhost:{port} (headless={args.headless})")
    try:
        return subprocess.call(cmd, env=env)
    except KeyboardInterrupt:
        return 130


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))

