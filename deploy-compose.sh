#!/usr/bin/env bash
set -euo pipefail

# =====================================================
# Project deploy for docker-compose.yml with:
# - auto-load .env (project-local)
# - auto-detect EXPOSED_PORT from compose (127.0.0.1:HOST:CONTAINER)
# - port availability check
# - nginx reverse proxy + Let's Encrypt SSL (redirect)
# - DRY_RUN=1 support
# - nginx rollback on certbot failure
#
# Put this script inside project directory (same as docker-compose.yml)
#
# REQUIRED (from .env or env):
#   PROJECT_NAME, DOMAIN, LE_EMAIL
#
# OPTIONAL:
#   PROJECT_DIR (default: current directory)
#   COMPOSE_FILE (default: docker-compose.yml)
#   COMPOSE_PROJECT (default: PROJECT_NAME)
#   EXPOSED_PORT (auto-detected if empty)
#   NGINX_SITE_NAME (default: ${PROJECT_NAME}.conf)
#   DRY_RUN=1
# =====================================================

run() {
  if [[ "${DRY_RUN:-0}" == "1" ]]; then
    echo "[DRY_RUN] $*"
  else
    eval "$@"
  fi
}

die() { echo "ERROR: $*" >&2; exit 1; }

require_root() {
  [[ "$(id -u)" -eq 0 ]] || die "Run as root: sudo -E ./deploy-compose.sh"
}

# 0) Determine project dir (script location)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="${PROJECT_DIR:-$SCRIPT_DIR}"

# 1) Load .env if exists (project-local)
if [[ -f "$PROJECT_DIR/.env" ]]; then
  echo "==> Loading $PROJECT_DIR/.env"
  set -a
  # shellcheck disable=SC1090
  source "$PROJECT_DIR/.env"
  set +a
fi

# 2) Config
PROJECT_NAME="${PROJECT_NAME:-}"
DOMAIN="${DOMAIN:-}"
LE_EMAIL="${LE_EMAIL:-}"

COMPOSE_FILE="${COMPOSE_FILE:-docker-compose.yml}"
COMPOSE_PROJECT="${COMPOSE_PROJECT:-$PROJECT_NAME}"
EXPOSED_PORT="${EXPOSED_PORT:-}"

NGINX_SITE_NAME="${NGINX_SITE_NAME:-${PROJECT_NAME}.conf}"
NGINX_SITE_PATH="/etc/nginx/sites-available/${NGINX_SITE_NAME}"
NGINX_SITE_LINK="/etc/nginx/sites-enabled/${NGINX_SITE_NAME}"

# 3) Validate
require_root
[[ -n "$PROJECT_NAME" ]] || die "Missing PROJECT_NAME (set in .env or env)"
[[ -n "$DOMAIN" ]] || die "Missing DOMAIN (set in .env or env)"
[[ -n "$LE_EMAIL" ]] || die "Missing LE_EMAIL (set in .env or env)"
[[ -d "$PROJECT_DIR" ]] || die "PROJECT_DIR not found: $PROJECT_DIR"
[[ -f "$PROJECT_DIR/$COMPOSE_FILE" ]] || die "Compose file not found: $PROJECT_DIR/$COMPOSE_FILE"

echo "==> Deploying:        $PROJECT_NAME"
echo "==> Domain:           $DOMAIN"
echo "==> Email:            $LE_EMAIL"
echo "==> Project dir:      $PROJECT_DIR"
echo "==> Compose file:     $COMPOSE_FILE"
echo "==> Compose project:  $COMPOSE_PROJECT"
echo "==> DRY_RUN:          ${DRY_RUN:-0}"
echo ""

# 4) Docker + compose command detect
if ! command -v docker >/dev/null 2>&1; then
  echo "==> Installing Docker..."
  run "apt update -y"
  run "apt install -y docker.io"
  run "systemctl enable --now docker"
fi

if docker compose version >/dev/null 2>&1; then
  COMPOSE_CMD=(docker compose)
elif command -v docker-compose >/dev/null 2>&1; then
  COMPOSE_CMD=(docker-compose)
else
  echo "==> Installing docker compose plugin..."
  run "apt update -y"
  run "apt install -y docker-compose-plugin"
  COMPOSE_CMD=(docker compose)
fi

# 5) Install nginx + certbot
echo "==> Installing nginx + certbot..."
run "apt update -y"
run "apt install -y nginx certbot python3-certbot-nginx ca-certificates curl"

# 6) Auto-detect EXPOSED_PORT if not set
detect_port_from_compose() {
  local compose_path="$1"

  # We look for patterns like:
  #   - "127.0.0.1:3007:3000"
  #   - '127.0.0.1:3007:3000'
  #   - 127.0.0.1:3007:3000
  #
  # Pick first host port after 127.0.0.1:
  local p
  p="$(grep -Eo "127\.0\.0\.1:[0-9]{2,5}:[0-9]{2,5}" "$compose_path" | head -n 1 | awk -F: '{print $2}' || true)"
  echo "$p"
}

if [[ -z "$EXPOSED_PORT" ]]; then
  EXPOSED_PORT="$(detect_port_from_compose "$PROJECT_DIR/$COMPOSE_FILE")"
fi

[[ -n "$EXPOSED_PORT" ]] || die "EXPOSED_PORT is missing and was not detected from compose. Set EXPOSED_PORT in .env or use 127.0.0.1:PORT:... in compose."

echo "==> Exposed port:     $EXPOSED_PORT (must match compose binding to 127.0.0.1)"
echo ""

# 7) Check port availability
port_in_use() {
  ss -lnt 2>/dev/null | awk '{print $4}' | grep -Eq "[:.]${EXPOSED_PORT}$"
}

if port_in_use; then
  # It may be this same project already running. We'll warn (not hard fail),
  # because compose might recreate it anyway.
  echo "WARNING: Port $EXPOSED_PORT seems to be in use. If this is another service, change EXPOSED_PORT."
  echo "You can inspect: sudo ss -lntp | grep ':$EXPOSED_PORT'"
fi

# 8) Deploy compose
echo "==> Starting docker compose (build + up)..."
run "cd \"$PROJECT_DIR\" && ${COMPOSE_CMD[*]} -p \"$COMPOSE_PROJECT\" -f \"$COMPOSE_FILE\" up -d --build"

# 9) Local health probe (best-effort)
echo "==> Checking local endpoint (best-effort): http://127.0.0.1:$EXPOSED_PORT ..."
if [[ "${DRY_RUN:-0}" != "1" ]]; then
  if ! curl -fsS "http://127.0.0.1:$EXPOSED_PORT" >/dev/null 2>&1; then
    echo "WARNING: Local endpoint did not respond yet. Continue anyway."
    echo "Logs: cd $PROJECT_DIR && ${COMPOSE_CMD[*]} -p $COMPOSE_PROJECT -f $COMPOSE_FILE logs -f"
  fi
fi

# 10) Nginx config + rollback preparation
echo "==> Writing nginx config: $NGINX_SITE_PATH"
BACKUP_PATH=""
if [[ -f "$NGINX_SITE_PATH" ]]; then
  BACKUP_PATH="${NGINX_SITE_PATH}.bak.$(date +%s)"
  run "cp \"$NGINX_SITE_PATH\" \"$BACKUP_PATH\""
fi

write_nginx() {
  cat > "$NGINX_SITE_PATH" <<EOF
server {
    listen 80;
    server_name $DOMAIN;

    location / {
        proxy_pass http://127.0.0.1:$EXPOSED_PORT;
        proxy_http_version 1.1;

        proxy_buffering on;
        proxy_buffers 8 256k;
        proxy_buffer_size 128k;
        proxy_busy_buffers_size 512k;
        proxy_max_temp_file_size 0;

        proxy_set_header Host \$host;
        proxy_set_header X-Real-IP \$remote_addr;
        proxy_set_header X-Forwarded-For \$proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto \$scheme;

        proxy_set_header Upgrade \$http_upgrade;
        proxy_set_header Connection "upgrade";
    }
}
EOF
}

if [[ "${DRY_RUN:-0}" == "1" ]]; then
  echo "[DRY_RUN] write nginx config to $NGINX_SITE_PATH"
else
  write_nginx
fi

run "rm -f \"$NGINX_SITE_LINK\""
run "ln -s \"$NGINX_SITE_PATH\" \"$NGINX_SITE_LINK\""

# Optional: remove default site
if [[ -e /etc/nginx/sites-enabled/default ]]; then
  run "rm -f /etc/nginx/sites-enabled/default"
fi

echo "==> Testing and reloading nginx..."
run "nginx -t"
run "systemctl reload nginx"

# 11) SSL via certbot with rollback on failure
echo "==> Requesting Let's Encrypt certificate..."
CERTBOT_OK=1
if [[ "${DRY_RUN:-0}" == "1" ]]; then
  echo "[DRY_RUN] certbot --nginx -n --agree-tos -m \"$LE_EMAIL\" -d \"$DOMAIN\" --redirect"
else
  set +e
  certbot --nginx -n --agree-tos -m "$LE_EMAIL" -d "$DOMAIN" --redirect
  rc=$?
  set -e
  if [[ $rc -ne 0 ]]; then
    CERTBOT_OK=0
  fi
fi

if [[ "$CERTBOT_OK" -ne 1 ]]; then
  echo "ERROR: certbot failed. Rolling back nginx config..."
  if [[ -n "$BACKUP_PATH" && -f "$BACKUP_PATH" ]]; then
    cp "$BACKUP_PATH" "$NGINX_SITE_PATH"
    nginx -t && systemctl reload nginx || true
    echo "Rolled back nginx config from: $BACKUP_PATH"
  else
    echo "No previous nginx config backup found to restore."
  fi
  die "SSL setup failed. Fix DNS / port 80 access / domain and retry."
fi

echo "==> Reloading nginx after SSL..."
run "nginx -t"
run "systemctl reload nginx"

echo ""
echo "✅ Done!"
echo "URL: https://$DOMAIN"
echo "Project: $PROJECT_NAME"
echo "Port: 127.0.0.1:$EXPOSED_PORT (nginx upstream)"
echo ""
echo "Useful:"
echo "  Logs:  cd $PROJECT_DIR && ${COMPOSE_CMD[*]} -p $COMPOSE_PROJECT -f $COMPOSE_FILE logs -f"
echo "  PS:    cd $PROJECT_DIR && ${COMPOSE_CMD[*]} -p $COMPOSE_PROJECT -f $COMPOSE_FILE ps"
echo "  Nginx: sudo nginx -t && sudo systemctl reload nginx"
