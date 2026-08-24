#!/usr/bin/env bash
set -euo pipefail

ACTION="${1:-}"
case "$ACTION" in
  install|uninstall|start|stop|restart|status) ;;
  *) echo "usage: $0 {install|uninstall|start|stop|restart|status}" >&2; exit 2 ;;
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
TimeoutStartSec=180s
TimeoutStopSec=360s
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
  restart) systemctl --user reload ghostprov.service ;;
  status)
    systemctl --user status ghostprov.service --no-pager
    if [[ -f "$WORKSPACE/.ghostprov/supervisor.json" ]]; then
      cat -- "$WORKSPACE/.ghostprov/supervisor.json"
    fi
    ;;
esac
