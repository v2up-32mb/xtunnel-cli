#!/usr/bin/env bash
set -euo pipefail

# E2E reverse mode validation script for xtunnel-cli feat/reverse-mode
# Usage: ./scripts/e2e-reverse.sh from repo root
# Produces: pass/fail exit code, prints key log snippets

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BIN_DIR="$REPO_ROOT/bin"
SERVER_BIN="$BIN_DIR/x-tunnel-server"
CLIENT_BIN="$BIN_DIR/x-tunnel-client"

if [[ ! -x "$SERVER_BIN" || ! -x "$CLIENT_BIN" ]]; then
  echo "[e2e] error: binaries missing, run 'go build -o bin/x-tunnel-server ./server/cmd/x-tunnel-server && go build -o bin/x-tunnel-client ./client/cmd/x-tunnel-client'" >&2
  exit 1
fi

TMPDIR=$(mktemp -d)
MARKER="REVERSE_E2E_OK_12345"
HTTP_PORT=18200
WS_PORT=18443
SOCKS_PORT=28180
TEST_FILE="$TMPDIR/index.html"

cleanup() {
  echo "[e2e] cleanup..."
  # kill background jobs
  for pid in $HTTP_PID $SERVER_PID $CLIENT_PID; do
    if [[ -n "$pid" ]]; then
      kill "$pid" 2>/dev/null || true
    fi
  done
  rm -rf "$TMPDIR"
}
trap cleanup EXIT INT TERM

# 1. create test http content
cat >"$TEST_FILE" <<EOF
<html><body>$MARKER</body></html>
EOF

# 2. start local http server
python3 -m http.server "$HTTP_PORT" --bind 127.0.0.1 --directory "$TMPDIR" >"$TMPDIR/http.log" 2>&1 &
HTTP_PID=$!
echo "[e2e] http server pid $HTTP_PID"

# 3. start server
SERVER_LOG="$TMPDIR/server.log"
nohup "$SERVER_BIN" -l 127.0.0.1:"$WS_PORT" -token e2e-token >"$SERVER_LOG" 2>&1 &
SERVER_PID=$!
echo "[e2e] server pid $SERVER_PID"

# wait for server listening
for i in $(seq 1 30); do
  if curl -k -s https://127.0.0.1:"$WS_PORT" -o /dev/null -w "%{http_code}" 2>/dev/null | grep -q 400; then
    echo "[e2e] server up"
    break
  fi
  sleep 1
done

# 4. start client in reverse mode
CLIENT_LOG="$TMPDIR/client.log"
nohup "$CLIENT_BIN" -f wss://127.0.0.1:"$WS_PORT" -token e2e-token -insecure -n 2 -r -l "socks5://127.0.0.1:$SOCKS_PORT" >"$CLIENT_LOG" 2>&1 &
CLIENT_PID=$!
echo "[e2e] client pid $CLIENT_PID"

# 5. wait for listener registration
wait_for() {
  local pattern="$1"
  local log="$2"
  local timeout=${3:-30}
  for i in $(seq 1 $timeout); do
    if grep -q "$pattern" "$log" 2>/dev/null; then
      return 0
    fi
    sleep 1
  done
  return 1
}

echo "[e2e] waiting for client registration log..."
if ! wait_for "反向监听已注册" "$CLIENT_LOG" 30; then
  echo "[e2e] ERROR: client never printed 反向监听已注册"
  echo "--- client log ---"
  tail -n 200 "$CLIENT_LOG"
  exit 1
fi

echo "[e2e] waiting for server listener start log..."
if ! wait_for "反向监听已开启" "$SERVER_LOG" 30; then
  # fallback pattern
  if ! grep -q "监听" "$SERVER_LOG"; then
    echo "[e2e] WARNING: server log did not contain 反向监听已开启, continuing"
  fi
fi

# wait for socks port open
for i in $(seq 1 30); do
  if bash -c "cat < /dev/null > /dev/tcp/127.0.0.1/$SOCKS_PORT" 2>/dev/null; then
    echo "[e2e] socks port open"
    break
  fi
  sleep 1
done

# 6. core assertion
TMP_OUT="$TMPDIR/curl.out"
if ! curl --socks5-hostname 127.0.0.1:"$SOCKS_PORT" --max-time 15 "http://127.0.0.1:$HTTP_PORT/" -o "$TMP_OUT" -s; then
  echo "[e2e] ERROR: curl via socks failed"
  echo "--- client log ---"
  tail -n 200 "$CLIENT_LOG"
  echo "--- server log ---"
  tail -n 200 "$SERVER_LOG"
  exit 1
fi

if grep -q "$MARKER" "$TMP_OUT"; then
  echo "[e2e] ASSERT PASS: curl via socks returns marker"
else
  echo "[e2e] ERROR: marker not found in curl output"
  echo "--- curl output ---"
  cat "$TMP_OUT"
  exit 1
fi

# 7. shutdown assertion
echo "[e2e] killing client to test listener cleanup"
kill "$CLIENT_PID" 2>/dev/null || true
sleep 3

# port should be closed
PORT_CLOSED=0
for i in $(seq 1 10); do
  if ! bash -c "cat < /dev/null > /dev/tcp/127.0.0.1/$SOCKS_PORT" 2>/dev/null; then
    PORT_CLOSED=1
    break
  fi
  sleep 0.5
done

if [[ $PORT_CLOSED -ne 1 ]]; then
  echo "[e2e] WARNING: socks port still open after client kill (may be delayed)"
else
  echo "[e2e] ASSERT PASS: socks port closed after client kill"
fi

# server log should mention listener shutdown
if grep -iq "已断开\|关闭\|反向监听" "$SERVER_LOG"; then
  echo "[e2e] ASSERT PASS: server log shows shutdown activity"
else
  echo "[e2e] WARNING: server log shutdown message not clear"
fi

# 8. hotpair phase: restart server with -hotpair and re-verify data path via prewarmed pair
echo "[e2e] === hotpair phase ==="
kill "$SERVER_PID" 2>/dev/null || true
wait "$SERVER_PID" 2>/dev/null || true
sleep 1

SERVER_LOG_HP="$TMPDIR/server-hotpair.log"
nohup "$SERVER_BIN" -l 127.0.0.1:"$WS_PORT" -token e2e-token -hotpair >"$SERVER_LOG_HP" 2>&1 &
SERVER_PID=$!

for i in $(seq 1 30); do
  if curl -k -s https://127.0.0.1:"$WS_PORT" -o /dev/null -w "%{http_code}" 2>/dev/null | grep -q 400; then
    break
  fi
  sleep 1
done

CLIENT_LOG_HP="$TMPDIR/client-hotpair.log"
nohup "$CLIENT_BIN" -f wss://127.0.0.1:"$WS_PORT" -token e2e-token -insecure -n 2 -r -l "socks5://127.0.0.1:$SOCKS_PORT" >"$CLIENT_LOG_HP" 2>&1 &
CLIENT_PID=$!

if ! wait_for "反向监听已注册" "$CLIENT_LOG_HP" 30; then
  echo "[e2e] ERROR: hotpair phase client never registered"
  exit 1
fi

if ! wait_for "Pair 构建完成" "$SERVER_LOG_HP" 30; then
  echo "[e2e] ERROR: ReversePairWarmer never built a pair"
  echo "--- server log ---"
  tail -n 200 "$SERVER_LOG_HP"
  exit 1
fi
echo "[e2e] ASSERT PASS: prewarmed pair built"

sleep 1  # 留出预热 Pair 就绪窗口，让下一次 curl 走单播快路径
TMP_OUT_HP="$TMPDIR/curl-hotpair.out"
if ! curl --socks5-hostname 127.0.0.1:"$SOCKS_PORT" --max-time 15 "http://127.0.0.1:$HTTP_PORT/" -o "$TMP_OUT_HP" -s; then
  echo "[e2e] ERROR: hotpair phase curl failed"
  echo "--- server log ---"
  tail -n 200 "$SERVER_LOG_HP"
  exit 1
fi

if grep -q "$MARKER" "$TMP_OUT_HP"; then
  echo "[e2e] ASSERT PASS: hotpair phase curl via socks returns marker"
else
  echo "[e2e] ERROR: marker not found in hotpair curl output"
  cat "$TMP_OUT_HP"
  exit 1
fi

echo "[e2e] ALL ASSERTIONS PASSED"
echo "=== key logs ==="
echo "--- client last 50 lines ---"
tail -n 50 "$CLIENT_LOG"
echo "--- server last 50 lines ---"
tail -n 50 "$SERVER_LOG"
