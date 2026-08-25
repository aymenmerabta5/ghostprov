#!/usr/bin/env bash
set -euo pipefail

ACTION="${1:-}"
case "$ACTION" in
  install|uninstall|start|stop|restart|config-restart|status) ;;
  *) echo "usage: $0 {install|uninstall|start|stop|restart|config-restart|status}" >&2; exit 2 ;;
esac

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
WORKSPACE="$(cd -- "$SCRIPT_DIR/.." && pwd)"
BUN_BIN="$(command -v bun)"
UNIT_DIR="${XDG_CONFIG_HOME:-$HOME/.config}/systemd/user"
UNIT_FILE="$UNIT_DIR/ghostprov.service"

install_unit() {
  mkdir -p -- "$UNIT_DIR"
  cat > "$UNIT_FILE" <<EOF
[Unit]
Description=GhostProv resilient host supervisor
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
WorkingDirectory="$WORKSPACE"
ExecStart="$BUN_BIN" run start:supervised
ExecReload="$BUN_BIN" scripts/request_upgrade.ts
ExecStop="$BUN_BIN" scripts/request_shutdown.ts
Restart=always
RestartSec=2s
# supervise.ts caps lifecycle waits at 1800 seconds. systemd retains a finite
# cgroup-owned last resort with one minute of cleanup margin.
TimeoutStartSec=1860s
TimeoutStopSec=1860s
KillMode=mixed

[Install]
WantedBy=default.target
EOF
  systemctl --user daemon-reload
  systemctl --user enable --now ghostprov.service
  echo "GhostProv user service installed."
  echo "For startup before login, optionally run: loginctl enable-linger \"$USER\""
}

case "$ACTION" in
  install) install_unit ;;
  uninstall)
    systemctl --user disable --now ghostprov.service 2>/dev/null || true
    rm -f -- "$UNIT_FILE"
    systemctl --user daemon-reload
    echo "GhostProv user service uninstalled."
    ;;
  start) systemctl --user start ghostprov.service ;;
  stop) systemctl --user stop ghostprov.service ;;
  # A normal restart is intentionally only a rolling worker reload.  It keeps
  # the supervisor and its startup environment alive.
  restart) systemctl --user reload ghostprov.service ;;
  # A systemd restart runs ExecStop, waits for the supervisor/fleet to drain,
  # and then launches a new supervisor process. dotenv therefore reloads .env.
  # Do not report success merely because Type=simple forked: prove that a new
  # descriptor instance and its matching worker-ready status are both fresh.
  config-restart)
    RESTART_REQUESTED_AT="$($BUN_BIN -e 'console.log(new Date().toISOString())')"
    OLD_INSTANCE_NONCE="$(
      (
        cd -- "$WORKSPACE"
        "$BUN_BIN" scripts/show_supervisor_status.ts descriptor 2>/dev/null | \
          "$BUN_BIN" -e 'const value=JSON.parse(await Bun.stdin.text()); console.log(value.instanceNonce)'
      ) || true
    )"
    systemctl --user restart ghostprov.service
    (cd -- "$WORKSPACE" && "$BUN_BIN" scripts/show_supervisor_status.ts wait-ready \
      "$RESTART_REQUESTED_AT" "${OLD_INSTANCE_NONCE:--}" 1830000)
    echo "GhostProv full configuration restart completed with a fresh ready supervisor; .env was reloaded."
    ;;
  status)
    systemctl --user status ghostprov.service --no-pager
    (cd -- "$WORKSPACE" && "$BUN_BIN" scripts/show_supervisor_status.ts)
    ;;
esac
