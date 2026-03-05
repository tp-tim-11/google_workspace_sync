#!/usr/bin/env bash
set -Eeuo pipefail

TARGET_USER=""
SHOW_ONLY=0

MARK_START="# >>> google-workspace-sync (managed) >>>"
MARK_END="# <<< google-workspace-sync (managed) <<<"

usage() {
  cat <<'EOF'
Usage:
  remove_google_sync_cron.sh [options]

Options:
  --user USER      Remove managed block from USER crontab via sudo
  --show-only      Print resulting crontab without installing
  -h, --help       Show help

Examples:
  ./scripts/remove_google_sync_cron.sh
  ./scripts/remove_google_sync_cron.sh --user adamveres
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --user)
      TARGET_USER="$2"
      shift 2
      ;;
    --show-only)
      SHOW_ONLY=1
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      usage
      exit 2
      ;;
  esac
done

if [[ -n "${TARGET_USER}" ]]; then
  existing="$(sudo crontab -u "${TARGET_USER}" -l 2>/dev/null || true)"
else
  existing="$(crontab -l 2>/dev/null || true)"
fi

if [[ -z "${existing}" ]]; then
  echo "No crontab entries found. Nothing to remove."
  exit 0
fi

cleaned="$(printf '%s\n' "${existing}" | awk -v s="${MARK_START}" -v e="${MARK_END}" '
  $0 == s {skip=1; next}
  $0 == e {skip=0; next}
  !skip {print}
')"

if [[ "${SHOW_ONLY}" -eq 1 ]]; then
  printf '%s\n' "${cleaned}"
  exit 0
fi

if [[ -n "${TARGET_USER}" ]]; then
  if [[ -n "${cleaned}" ]]; then
    printf '%s\n' "${cleaned}" | sudo crontab -u "${TARGET_USER}" -
  else
    sudo crontab -u "${TARGET_USER}" -r 2>/dev/null || true
  fi
  echo "Removed managed cron block for user ${TARGET_USER}."
else
  if [[ -n "${cleaned}" ]]; then
    printf '%s\n' "${cleaned}" | crontab -
  else
    crontab -r 2>/dev/null || true
  fi
  echo "Removed managed cron block for current user."
fi
