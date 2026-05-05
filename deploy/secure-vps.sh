#!/usr/bin/env bash
set -euo pipefail

APP_USER="${ISTARI_APP_USER:-istari}"
APP_ROOT="${ISTARI_APP_ROOT:-/srv/project-istari}"
ENV_DIR="${ISTARI_ENV_DIR:-/etc/project-istari}"

if [ "$(id -u)" -ne 0 ]; then
  echo "Run as root." >&2
  exit 1
fi

apt-get update
apt-get install -y \
  caddy \
  fail2ban \
  git \
  python3-venv \
  sqlite3 \
  ufw

if ! id "$APP_USER" >/dev/null 2>&1; then
  adduser --system --group --home "$APP_ROOT" "$APP_USER"
fi

mkdir -p "$APP_ROOT/data" "$ENV_DIR"
chown -R "$APP_USER:$APP_USER" "$APP_ROOT"
chmod 750 "$APP_ROOT"
chmod 700 "$APP_ROOT/data" "$ENV_DIR"

ufw allow OpenSSH
ufw allow 80/tcp
ufw allow 443/tcp
ufw --force enable

install -d -m 0755 /etc/ssh/sshd_config.d
cat >/etc/ssh/sshd_config.d/99-project-istari.conf <<'EOF'
PasswordAuthentication no
KbdInteractiveAuthentication no
PermitRootLogin prohibit-password
X11Forwarding no
AllowTcpForwarding no
EOF
systemctl reload ssh || systemctl reload sshd

systemctl enable --now fail2ban

echo "Base VPS security is applied."
echo "Next: copy backend.env to $ENV_DIR/backend.env with mode 600, deploy the app to $APP_ROOT, then install the systemd and Caddy files."
