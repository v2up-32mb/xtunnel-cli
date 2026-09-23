#!/usr/bin/env bash
set -euo pipefail

TMPDIR=$(mktemp -d)
INDEX_FILE="$TMPDIR/index.html"
SERVER_LOG="$TMPDIR/server.log"
CLIENT_LOG="$TMPDIR/client.log"
HTTP_PID=""
SERVER_PID=""
CLIENT_PID=""

cleanup() {
    echo "[E2E] 清理临时进程..."
    [ -n "$HTTP_PID" ] && kill "$HTTP_PID" 2>/dev/null || true
    [ -n "$CLIENT_PID" ] && kill "$CLIENT_PID" 2>/dev/null || true
    [ -n "$SERVER_PID" ] && kill "$SERVER_PID" 2>/dev/null || true
    sleep 0.5
}
trap cleanup EXIT

MARKER="E2E-MARKER-REV-2026-$(date +%s)"
cat > "$INDEX_FILE" <<EOF
<!doctype html><html><body>$MARKER</body></html>
EOF

echo "[E2E] 启动测试 HTTP 服务在 127.0.0.1:18200"
python3 -m http.server 18200 --bind 127.0.0.1 --directory "$TMPDIR" &
HTTP_PID=$!
sleep 1

echo "[E2E] 启动服务端"
bin/x-tunnel-server -l 127.0.0.1:18443 -token e2e-token >"$SERVER_LOG" 2>&1 &
SERVER_PID=$!
sleep 2

echo "[E2E] 启动客户端（反向模式 socks5://127.0.0.1:18180）"
bin/x-tunnel-client -f wss://127.0.0.1:18443 -token e2e-token -insecure -n 2 -r -l socks5://127.0.0.1:18180 >"$CLIENT_LOG" 2>&1 &
CLIENT_PID=$!

echo "[E2E] 等待端口 18180 就绪（30s）"
for i in $(seq 1 30); do
    if bash -c 'exec 3</dev/tcp/127.0.0.1/18180' 2>/dev/null; then
        echo "[E2E] 端口 18180 可达"
        break
    fi
    sleep 1
done
if ! bash -c 'exec 3</dev/tcp/127.0.0.1/18180' 2>/dev/null; then
    echo "[E2E] ERROR 端口 18180 未就绪"
    cat "$SERVER_LOG"
    cat "$CLIENT_LOG"
    exit 1
fi

echo "[E2E] 检查日志关键字"
if ! grep -q "反向监听已注册" "$CLIENT_LOG"; then
    echo "[E2E] ERROR 客户端未记录反向监听已注册"
    cat "$CLIENT_LOG"
    exit 1
fi
if ! grep -q "反向监听已开启" "$SERVER_LOG"; then
    echo "[E2E] ERROR 服务端未记录反向监听已开启"
    cat "$SERVER_LOG"
    exit 1
fi

echo "[E2E] 正向流量测试（SOCKS5 → HTTP）"
OUTPUT=$(curl --socks5-hostname 127.0.0.1:18180 --max-time 15 http://127.0.0.1:18200/ || true)
if [[ "$OUTPUT" != *"$MARKER"* ]]; then
    echo "[E2E] ERROR 流量未返回预期标记"
    echo "OUTPUT=$OUTPUT"
    echo "CLIENT_LOG:"
    tail -n 200 "$CLIENT_LOG"
    echo "SERVER_LOG:"
    tail -n 200 "$SERVER_LOG"
    exit 1
fi
echo "[E2E] 流量路径 OK"

echo "[E2E] 关闭客户端，检查服务端注销"
kill "$CLIENT_PID" 2>/dev/null || true
sleep 2
if ! grep -q "反向监听已关闭" "$SERVER_LOG"; then
    # 部分实现可能无该日志，退而求其次检查端口释放
    echo "[E2E] WARN 未找到反向监听已关闭日志，检查端口释放"
fi

# 检查端口是否可重新绑定
if python3 -c "import socket; s=socket.socket(); s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR,1); s.bind(('127.0.0.1',18180))" 2>/dev/null; then
    echo "[E2E] 端口 18180 已释放"
else
    echo "[E2E] ERROR 端口 18180 未释放"
    exit 1
fi

echo "[E2E] 所有断言通过"
echo "[E2E] 临时目录 $TMPDIR"
exit 0
