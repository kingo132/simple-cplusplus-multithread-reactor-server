#!/usr/bin/env bash
# Build everything and run the functional tests on localhost.
#   tests/run_tests.sh                 normal build
#   SANITIZE=address tests/run_tests.sh
#   SANITIZE=thread  tests/run_tests.sh
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
WORK="${WORK_DIR:-$(mktemp -d)}"
mkdir -p "$WORK"
SANITIZE="${SANITIZE:-}"
PORT_ECHO=${PORT_ECHO:-22345}
PORT_RELAY=${PORT_RELAY:-22346}
FAILED=0

log() { echo -e "\033[1;34m==> $*\033[0m"; }
fail() { echo -e "\033[1;31mFAIL: $*\033[0m"; FAILED=1; }

log "Building (SANITIZE=${SANITIZE:-none}) in $WORK"
make -s -C "$ROOT/MultithreadServer" clean >/dev/null
make -s -C "$ROOT/MultithreadServer" -j4 SANITIZE="$SANITIZE"
make -s -C "$ROOT/TestHandler" clean >/dev/null
make -s -C "$ROOT/TestHandler" SANITIZE="$SANITIZE"
make -s -C "$ROOT/TestClient"

log "Ring queue unit test"
SAN_FLAGS=""
[ -n "$SANITIZE" ] && SAN_FLAGS="-fsanitize=$SANITIZE"
g++ -std=c++11 -O1 -g -pthread $SAN_FLAGS -DSRV_ENGINE_INTERNAL -I"$ROOT/MultithreadServer" \
    "$ROOT/tests/ring_queue_test.cpp" "$ROOT/MultithreadServer/ring_queue.cpp" \
    "$ROOT/MultithreadServer/log_manager.cpp" "$ROOT/MultithreadServer/utility.cpp" -o "$WORK/ring_queue_test"
"$WORK/ring_queue_test" || fail "ring queue test"

# start_server <name> <port> <mode> [engine]
start_server() {
    local name=$1 port=$2 mode=$3 engine=${4:-epoll}
    local dir="$WORK/$name"
    mkdir -p "$dir/conf"
    printf "127.0.0.1 %s tcp 60\n127.0.0.1 %s udp 0\n" "$port" "$port" > "$dir/conf/bind.txt"
    cat > "$dir/conf/config.ini" <<CONF
log_dir = $dir/log
log_level = ${LOG_LEVEL:-6}
log_dest = 2
worker_num = 4
bind_file = $dir/conf/bind.txt
dll_file = $ROOT/TestHandler/libtesthandler.so
max_packet_size = 65536
send_high_watermark = 262144
event_engine = $engine
test_mode = $mode
relay_target_ip = 127.0.0.1
relay_target_port = $PORT_ECHO
CONF
    "$ROOT/MultithreadServer/mulserver" "$dir/conf/config.ini" > "$dir/stdout.txt" 2>&1 &
    echo $! > "$dir/pid"
    for _ in $(seq 50); do
        grep -q "Server started" "$dir/log/server.log" 2>/dev/null && return 0
        sleep 0.1
    done
    fail "$name did not start"; cat "$dir/stdout.txt"; return 1
}

stop_server() {
    local dir="$WORK/$1"
    local pid; pid=$(cat "$dir/pid")
    kill -INT "$pid" 2>/dev/null || true
    for _ in $(seq 100); do kill -0 "$pid" 2>/dev/null || break; sleep 0.1; done
    if kill -0 "$pid" 2>/dev/null; then fail "$1 did not stop"; kill -9 "$pid"; fi
    wait "$pid" 2>/dev/null || true
    if grep -E "ERROR: (AddressSanitizer|ThreadSanitizer|LeakSanitizer)|runtime error|WARNING: ThreadSanitizer" "$dir/stdout.txt" >/dev/null; then
        fail "$1: sanitizer reported problems"; head -60 "$dir/stdout.txt"
    fi
}

CLIENT="$ROOT/TestClient/tcp_test_client"
run_client() {
    echo "  client $*"
    "$CLIENT" "$@" || fail "client $*"
}

for engine in epoll poll; do
    log "Echo server ($engine)"
    start_server "echo-$engine" "$PORT_ECHO" echo "$engine"
    run_client -p "$PORT_ECHO" -c 20 -n 200 -s 8192
    run_client -p "$PORT_ECHO" -c 10 -n 50 -s 65536 -w 8
    run_client -p "$PORT_ECHO" -c 100 -n 50 -s 1024 -w 4
    run_client -p "$PORT_ECHO" -u -c 10 -n 100 -s 1400
    if [ "$engine" = epoll ]; then
        log "Relay server -> echo server"
        start_server relay "$PORT_RELAY" relay "$engine"
        run_client -p "$PORT_RELAY" -c 20 -n 200 -s 8192
        run_client -p "$PORT_RELAY" -c 10 -n 50 -s 65536 -w 16
        stop_server relay
    fi
    stop_server "echo-$engine"
done

if [ $FAILED -eq 0 ]; then
    echo -e "\033[1;32mALL TESTS PASSED\033[0m"
else
    echo -e "\033[1;31mSOME TESTS FAILED (logs in $WORK)\033[0m"
fi
exit $FAILED
