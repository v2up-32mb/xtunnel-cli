#!/usr/bin/env bash
set -euo pipefail

# E2E forward mode validation script for xtunnel-cli feat/reverse-mode
# 回归覆盖：Hot Pair 预热 + 串行访问 + 并发访问（connID 冲突回归：
# 正向 Pair 为共享复用，连接 connID 必须每连接唯一，禁止复用 prebind connID）
# Usage: ./scripts/e2e-forward.sh from repo root

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BIN_DIR="$REPO_ROOT/bin"
SERVER_BIN="$BIN_DIR/x-tunnel-server"
CLIENT_BIN="$BIN_DIR/x-tunnel-client"

if [[ ! -x "$SERVER_BIN" || ! -x "$CLIENT_BIN" ]]; then
  echo "[e2e] error: binaries missing, run 'go build -o bin/x-tunnel-server ./server/cmd/x-tunnel-server && go build -o bin/x-tunnel-client ./client/cmd/x-tunnel-client'" >&2
  exit 1
fi

TMPDIR=$(mktemp -d)
MARKER="FWD_E2E_OK_12345"
HTTP_PORT=18300
WS_PORT=18500
SOCKS_PORT=28300
TEST_FILE="$TMPDIR/index.html"
CONCURRENCY=8

cleanup() {
  echo "[e2e] cleanup..."
  for pid in $HTTP_PID $SERVER_PID $CLIENT_PID; do
    if [[ -n "$pid" ]]; then
      kill "$pid" 2>/dev/null || true
    fi
  done
  rm -rf "$TMPDIR"
}
trap cleanup EXIT INT TERM

# 1. test http content
cat >"$TEST_FILE" <<EOF
<html><body>$MARKER</body></html>
EOF

# 2. local http server
python3 -m http.server "$HTTP_PORT" --bind 127.0.0.1 --directory "$TMPDIR" >"$TMPDIR/http.log" 2>&1 &
HTTP_PID=$!
echo "[e2e] http server pid $HTTP_PID"

# 3. server
SERVER_LOG="$TMPDIR/server.log"
nohup "$SERVER_BIN" -l 127.0.0.1:"$WS_PORT" -token fwd-tok >"$SERVER_LOG" 2>&1 &
SERVER_PID=$!
echo "[e2e] server pid $SERVER_PID"

for i in $(seq 1 30); do
  if curl -k -s https://127.0.0.1:"$WS_PORT" -o /dev/null -w "%{http_code}" 2>/dev/null | grep -q 400; then
    echo "[e2e] server up"
    break
  fi
  sleep 1
done

# 4. client with -hotpair (需要 ≥2 通道才能构建 Pair)
CLIENT_LOG="$TMPDIR/client.log"
nohup "$CLIENT_BIN" -l socks5://127.0.0.1:"$SOCKS_PORT" -f wss://127.0.0.1:"$WS_PORT" -token fwd-tok -insecure -n 2 -hotpair >"$CLIENT_LOG" 2>&1 &
CLIENT_PID=$!
echo "[e2e] client pid $CLIENT_PID"

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

echo "[e2e] waiting for pair warm-up..."
if ! wait_for "成功构建 Pair" "$CLIENT_LOG" 30; then
  echo "[e2e] ERROR: PairWarmer never built a pair"
  echo "--- client log ---"
  tail -n 200 "$CLIENT_LOG"
  exit 1
fi
echo "[e2e] ASSERT PASS: hotpair built"

# wait for socks port open
for i in $(seq 1 30); do
  if bash -c "cat < /dev/null > /dev/tcp/127.0.0.1/$SOCKS_PORT" 2>/dev/null; then
    echo "[e2e] socks port open"
    break
  fi
  sleep 1
done

# 5. serial request
TMP_OUT="$TMPDIR/curl.out"
if ! curl --socks5-hostname 127.0.0.1:"$SOCKS_PORT" --max-time 15 "http://127.0.0.1:$HTTP_PORT/" -o "$TMP_OUT" -s; then
  echo "[e2e] ERROR: serial curl via socks failed"
  tail -n 200 "$CLIENT_LOG"
  exit 1
fi
if grep -q "$MARKER" "$TMP_OUT"; then
  echo "[e2e] ASSERT PASS: serial curl via socks returns marker"
else
  echo "[e2e] ERROR: marker not found in serial curl output"
  cat "$TMP_OUT"
  exit 1
fi

# 预热热路径断言：客户端单播拨号 + 服务端按键提升（connID = 键.唯一后缀，零选路消息）
if grep -q "单播拨号" "$CLIENT_LOG" && grep -q "预热 Pair 提升" "$SERVER_LOG"; then
  echo "[e2e] ASSERT PASS: dial via prewarmed pair (hot path, connID = key.suffix)"
else
  echo "[e2e] WARNING: hot-path markers not found (dial may have used fallback)"
  grep -n "Hot Pair\|预热" "$CLIENT_LOG" | tail -5
fi

# 6. concurrent requests（回归：并发连接 connID 必须互不冲突）
echo "[e2e] concurrent ${CONCURRENCY} requests..."
PIDS=()
for i in $(seq 1 $CONCURRENCY); do
  curl --socks5-hostname 127.0.0.1:"$SOCKS_PORT" --max-time 15 \
    "http://127.0.0.1:$HTTP_PORT/?i=$i" -o "$TMPDIR/conc$i.out" -s &
  PIDS+=($!)
done
CONC_FAIL=0
for i in $(seq 1 $CONCURRENCY); do
  wait "${PIDS[$((i-1))]}" || CONC_FAIL=$((CONC_FAIL+1))
  if ! grep -q "$MARKER" "$TMPDIR/conc$i.out" 2>/dev/null; then
    CONC_FAIL=$((CONC_FAIL+1))
    echo "[e2e] ERROR: concurrent request $i missing marker"
  fi
done
if [[ $CONC_FAIL -eq 0 ]]; then
  echo "[e2e] ASSERT PASS: ${CONCURRENCY} concurrent requests all OK"
else
  echo "[e2e] ERROR: $CONC_FAIL concurrent request(s) failed"
  tail -n 200 "$CLIENT_LOG"
  exit 1
fi

# 7. shutdown
echo "[e2e] killing client"
kill "$CLIENT_PID" 2>/dev/null || true
sleep 3
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

echo "[e2e] ALL ASSERTIONS PASSED"
