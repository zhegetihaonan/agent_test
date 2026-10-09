#!/bin/bash
set -e

BASE_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$BASE_DIR"

if [ -f "$BASE_DIR/.env" ]; then
    set -a
    . "$BASE_DIR/.env"
    set +a
fi

exec "$BASE_DIR/venv/bin/python3" "$BASE_DIR/main.py"
