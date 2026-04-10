#!/bin/bash
# =============================================================
#  Ultimate VS Code Tunnel Keep-Alive Setup
#  Usage: bash setup_tunnel.sh <tunnel_name> <cli_data_dir>
#  Example: bash setup_tunnel.sh zq_tunnel ~/.zq_tunnel
# =============================================================

if [ -z "$1" ] || [ -z "$2" ]; then
    echo "Usage: bash setup_tunnel.sh <tunnel_name> <cli_data_dir>"
    echo "Example: bash setup_tunnel.sh zq_tunnel ~/.zq_tunnel"
    exit 1
fi

TUNNEL_NAME="$1"
CLI_DATA_DIR="$2"
CODE_URL="https://code.visualstudio.com/sha/download?build=stable&os=cli-alpine-x64"
# WORK_DIR="$HOME"
WORK_DIR="$(cd "$(dirname "$0")" && pwd)"
LOG_DIR="$WORK_DIR/.tunnel_logs"
PID_FILE="$WORK_DIR/.tunnel_keeper.pid"
LOCK_FILE="$WORK_DIR/.tunnel_keeper.lock"

# ===================== Validate tunnel name =====================
if [ ${#TUNNEL_NAME} -gt 20 ]; then
    echo "ERROR: Tunnel name '$TUNNEL_NAME' is ${#TUNNEL_NAME} chars, max 20!"
    echo "Pick a shorter name, e.g.: ${TUNNEL_NAME:0:20}"
    exit 1
fi

mkdir -p "$LOG_DIR"

# ===================== Step 1: Download code CLI =====================
cd "$WORK_DIR"
if [ ! -f ./code ]; then
    echo "==> Downloading VS Code CLI..."
    curl -sL "$CODE_URL" -o /tmp/vscode_cli.tar.gz
    tar -xzf /tmp/vscode_cli.tar.gz -C "$WORK_DIR"
    rm -f /tmp/vscode_cli.tar.gz
    echo "==> Done."
else
    echo "==> code CLI already exists, skipping download."
fi

# ===================== Step 2: Generate tunnel_keeper.sh =====================
cat > "$WORK_DIR/tunnel_keeper.sh" << 'KEEPER_EOF'
#!/bin/bash
# ---- Tunnel Keeper: infinite restart + exponential backoff ----

TUNNEL_NAME="__TUNNEL_NAME__"
CLI_DATA_DIR="__CLI_DATA_DIR__"
WORK_DIR="$HOME"
LOG_DIR="$WORK_DIR/.tunnel_logs"
PID_FILE="$WORK_DIR/.tunnel_keeper.pid"
LOCK_FILE="$WORK_DIR/.tunnel_keeper.lock"
LOG_FILE="$LOG_DIR/tunnel.log"

MAX_BACKOFF=300          # max retry delay: 5 minutes
STABLE_THRESHOLD=120     # if tunnel runs > 2 min, consider it "stable" and reset backoff
MAX_LOG_SIZE=10485760    # 10MB per log file
MAX_LOG_FILES=3          # keep 3 rotated logs

# ---- Flock: prevent duplicate instances ----
exec 200>"$LOCK_FILE"
if ! flock -n 200; then
    echo "[$(date)] Another tunnel_keeper is already running. Exiting."
    exit 0
fi

# ---- Write PID ----
echo $$ > "$PID_FILE"

# ---- Helper: log to both terminal and file ----
log() {
    echo "$@" | tee -a "$LOG_FILE"
}

# ---- Clean shutdown on signals ----
cleanup() {
    log "[$(date)] Received shutdown signal, cleaning up..."
    # Gracefully kill tunnel via code CLI first, then force kill
    "$WORK_DIR/code" tunnel --cli-data-dir "$CLI_DATA_DIR" kill 2>/dev/null
    [ -n "$TUNNEL_PID" ] && kill "$TUNNEL_PID" 2>/dev/null && wait "$TUNNEL_PID" 2>/dev/null
    rm -f "$PID_FILE" "$LOCK_FILE"
    exit 0
}
trap cleanup SIGTERM SIGINT SIGHUP EXIT

# ---- Log rotation ----
rotate_logs() {
    if [ -f "$LOG_FILE" ] && [ "$(stat -c%s "$LOG_FILE" 2>/dev/null || echo 0)" -ge "$MAX_LOG_SIZE" ]; then
        for i in $(seq $((MAX_LOG_FILES - 1)) -1 1); do
            [ -f "${LOG_FILE}.$i" ] && mv "${LOG_FILE}.$i" "${LOG_FILE}.$((i + 1))"
        done
        mv "$LOG_FILE" "${LOG_FILE}.1"
        rm -f "${LOG_FILE}.$((MAX_LOG_FILES + 1))"
        echo "[$(date)] Log rotated." | tee "$LOG_FILE"
    fi
}

# ---- Stop existing tunnel daemon (without nuking server cache) ----
stop_old_tunnel() {
    # 1. Gracefully stop tunnel daemon
    "$WORK_DIR/code" tunnel --cli-data-dir "$CLI_DATA_DIR" kill 2>/dev/null && \
        log "[$(date)] Stopped existing tunnel daemon via 'code tunnel kill'" || true

    # 2. Kill any leftover processes (match OUR cli-data-dir only)
    pkill -f "code tunnel --cli-data-dir $CLI_DATA_DIR" 2>/dev/null && \
        log "[$(date)] Killed stale tunnel process" || true

    sleep 1
}

# ---- Main loop ----
cd "$WORK_DIR"
BACKOFF=3
ATTEMPT=0
CONSECUTIVE_FAST_FAILURES=0
FAST_FAIL_THRESHOLD=3    # after 3 consecutive quick crashes, nuke server cache

while true; do
    ATTEMPT=$((ATTEMPT + 1))
    rotate_logs

    log "[$(date)] === Attempt #$ATTEMPT | backoff=${BACKOFF}s ==="

    # Always stop old daemon before starting new one
    stop_old_tunnel

    # If tunnel keeps crashing fast, the server cache is probably corrupt
    if [ "$CONSECUTIVE_FAST_FAILURES" -ge "$FAST_FAIL_THRESHOLD" ]; then
        log "[$(date)] $CONSECUTIVE_FAST_FAILURES consecutive fast crashes detected, server cache likely corrupt"
        if [ -d "$CLI_DATA_DIR/servers" ]; then
            rm -rf "$CLI_DATA_DIR/servers"
            log "[$(date)] Nuked stale servers/ cache in $CLI_DATA_DIR"
        fi
        CONSECUTIVE_FAST_FAILURES=0
    fi

    log "[$(date)] Starting tunnel (name=$TUNNEL_NAME)..."

    START_TIME=$(date +%s)

    # Launch tunnel in background so we can track its PID
    ./code tunnel --cli-data-dir "$CLI_DATA_DIR" --name "$TUNNEL_NAME" 2>&1 | tee -a "$LOG_FILE" &
    TUNNEL_PID=$!
    log "[$(date)] Tunnel PID: $TUNNEL_PID"

    # Wait for tunnel process to exit
    wait "$TUNNEL_PID" 2>/dev/null
    EXIT_CODE=$?
    TUNNEL_PID=""

    END_TIME=$(date +%s)
    RUNTIME=$((END_TIME - START_TIME))

    log "[$(date)] Tunnel exited (code=$EXIT_CODE, ran ${RUNTIME}s)"

    # If it ran long enough, it was "stable" -> reset everything
    if [ "$RUNTIME" -ge "$STABLE_THRESHOLD" ]; then
        BACKOFF=3
        ATTEMPT=0
        CONSECUTIVE_FAST_FAILURES=0
        log "[$(date)] Was stable (>${STABLE_THRESHOLD}s), backoff reset to ${BACKOFF}s"
    else
        CONSECUTIVE_FAST_FAILURES=$((CONSECUTIVE_FAST_FAILURES + 1))
        # Exponential backoff with jitter
        JITTER=$((RANDOM % 3))
        log "[$(date)] Unstable run (<${STABLE_THRESHOLD}s), fast_fail=$CONSECUTIVE_FAST_FAILURES/$FAST_FAIL_THRESHOLD, waiting ${BACKOFF}s (+${JITTER}s jitter)..."
        sleep $((BACKOFF + JITTER))
        BACKOFF=$((BACKOFF * 2))
        [ "$BACKOFF" -gt "$MAX_BACKOFF" ] && BACKOFF=$MAX_BACKOFF
    fi

    sleep 1  # minimum cooldown
done
KEEPER_EOF

# Replace placeholders
sed -i "s|__TUNNEL_NAME__|$TUNNEL_NAME|g" "$WORK_DIR/tunnel_keeper.sh"
sed -i "s|__CLI_DATA_DIR__|$CLI_DATA_DIR|g" "$WORK_DIR/tunnel_keeper.sh"
chmod +x "$WORK_DIR/tunnel_keeper.sh"
echo "==> Created tunnel_keeper.sh"

# ===================== Step 3: Generate watchdog.sh =====================
cat > "$WORK_DIR/tunnel_watchdog.sh" << 'WATCHDOG_EOF'
#!/bin/bash
# ---- Watchdog: cron calls this every minute to ensure tunnel is alive ----

WORK_DIR="$HOME"
PID_FILE="$WORK_DIR/.tunnel_keeper.pid"
LOG_DIR="$WORK_DIR/.tunnel_logs"
WATCHDOG_LOG="$LOG_DIR/watchdog.log"

mkdir -p "$LOG_DIR"

is_keeper_alive() {
    [ -f "$PID_FILE" ] || return 1
    local pid
    pid=$(cat "$PID_FILE" 2>/dev/null)
    [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null
}

# Also check if tunnel_keeper.sh is in process list (belt + suspenders)
is_keeper_in_ps() {
    pgrep -f "tunnel_keeper.sh" > /dev/null 2>&1
}

if is_keeper_alive || is_keeper_in_ps; then
    # All good, keeper is running
    exit 0
fi

# Keeper is dead, resurrect it
echo "[$(date)] Watchdog: tunnel_keeper is DEAD, resurrecting..." >> "$WATCHDOG_LOG"

# Prefer tmux if available
if command -v tmux &>/dev/null; then
    tmux kill-session -t tunnel 2>/dev/null || true
    tmux new-session -d -s tunnel "bash $WORK_DIR/tunnel_keeper.sh"
    echo "[$(date)] Watchdog: restarted via tmux" >> "$WATCHDOG_LOG"
else
    nohup bash "$WORK_DIR/tunnel_keeper.sh" > /dev/null 2>&1 &
    echo "[$(date)] Watchdog: restarted via nohup (PID=$!)" >> "$WATCHDOG_LOG"
fi
WATCHDOG_EOF

chmod +x "$WORK_DIR/tunnel_watchdog.sh"
echo "==> Created tunnel_watchdog.sh"

# ===================== Step 4: Clean slate before first run =====================
echo "==> Cleaning stale tunnel state..."
./code tunnel --cli-data-dir "$CLI_DATA_DIR" kill 2>/dev/null || true
pkill -f "code tunnel --cli-data-dir $CLI_DATA_DIR" 2>/dev/null || true
[ -d "$CLI_DATA_DIR/servers" ] && rm -rf "$CLI_DATA_DIR/servers" && echo "==> Removed stale servers/ cache"
sleep 1

# ===================== Step 5: First run (interactive auth) =====================
echo ""
echo "============================================"
echo "  Tunnel name : $TUNNEL_NAME"
echo "  CLI data dir: $CLI_DATA_DIR"
echo ""
echo "  First run requires authentication."
echo "  Follow the prompts to verify your device."
echo "  After verification, press Ctrl+C to stop."
echo "============================================"
echo ""
./code tunnel --cli-data-dir "$CLI_DATA_DIR" --name "$TUNNEL_NAME" || true

# ===================== Step 6: Start in tmux =====================
echo ""
echo "==> Launching tunnel_keeper in tmux..."
tmux kill-session -t tunnel 2>/dev/null || true
tmux new-session -d -s tunnel "bash $WORK_DIR/tunnel_keeper.sh"
echo "==> Tunnel is running in tmux session 'tunnel'."

# ===================== Step 7: Install crontab watchdog =====================
echo "==> Installing crontab watchdog..."

# Deduplicate: remove old entries first, then add fresh ones
CRON_MARKER="# vscode-tunnel-watchdog"
( crontab -l 2>/dev/null | grep -v "$CRON_MARKER" ;
  echo "* * * * * bash $WORK_DIR/tunnel_watchdog.sh $CRON_MARKER" ;
  echo "@reboot sleep 10 && bash $WORK_DIR/tunnel_watchdog.sh $CRON_MARKER"
) | crontab -
echo "==> Crontab installed (every-minute watchdog + @reboot)."

# ===================== Done =====================
echo ""
echo "========================================================"
echo "  ALL SET! Triple-layer keep-alive is active:"
echo ""
echo "  Layer 1: tunnel_keeper.sh"
echo "           while-true loop + exponential backoff"
echo "           + auto clean stale server cache on restart"
echo ""
echo "  Layer 2: tmux session 'tunnel'"
echo "           survives terminal disconnect"
echo ""
echo "  Layer 3: crontab watchdog (every 1 min + @reboot)"
echo "           resurrects everything if killed"
echo "========================================================"
echo ""
echo "Commands:"
echo "  tmux attach -t tunnel              # view live logs"
echo "  tail -f ~/.tunnel_logs/tunnel.log  # tail log file"
echo "  tail -f ~/.tunnel_logs/watchdog.log # watchdog log"
echo "  crontab -l                         # verify cron"
echo "  tmux kill-session -t tunnel        # stop (cron restarts in <1min)"
echo ""
echo "To FULLY stop:"
echo "  tmux kill-session -t tunnel 2>/dev/null"
echo "  crontab -l | grep -v '$CRON_MARKER' | crontab -"
echo "  rm -f $PID_FILE $LOCK_FILE"
echo ""
