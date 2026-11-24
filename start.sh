#!/bin/bash
# Запускает Node-шлюз WhatsApp и Python-ядро в одном контейнере.
set -u
export INTERNAL_KEY="${INTERNAL_KEY:-$(head -c 32 /dev/urandom | base64 | tr -dc 'A-Za-z0-9' | head -c 32)}"
export GATEWAY_PORT="${GATEWAY_PORT:-3001}"
export PORT="${PORT:-10000}"

node gateway/index.js & NODE_PID=$!
python3 wa_core.py & PY_PID=$!

term() { kill -TERM "$NODE_PID" "$PY_PID" 2>/dev/null; wait "$NODE_PID" "$PY_PID" 2>/dev/null; exit 0; }
trap term TERM INT

wait -n "$NODE_PID" "$PY_PID"
code=$?
kill -TERM "$NODE_PID" "$PY_PID" 2>/dev/null
wait
exit "$code"
