#!/usr/bin/env bash
set -Eeuo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
LOG_DIR="${PROJECT_DIR}/logs"

UV_BIN="$(command -v uv || true)"
FLOCK_BIN="$(command -v flock || true)"
BASH_BIN="$(command -v bash || true)"

UV_ARGS=""
FLOCK_ARGS="-n"
DRIVE_SCHEDULE="*/5 * * * *"
SHEET_SCHEDULE="*/15 * * * *"
TARGET_USER=""
SHOW_ONLY=0

MARK_START="# >>> google-workspace-sync (managed) >>>"
MARK_END="# <<< google-workspace-sync (managed) <<<"

usage() {
  cat <<'EOF'
Usage:
  add_google_sync_cron.sh [options]

Options:
  --project-dir PATH         Project root directory
  --uv-bin PATH              Path to uv binary
  --uv-args STRING           Extra args passed to "uv run"
  --flock-bin PATH           Path to flock binary
  --flock-args STRING        Extra args passed to flock (default: -n)
  --bash-bin PATH            Path to bash binary used in cron
  --drive-schedule CRON      Cron schedule for drive sync (default: */5 * * * *)
  --sheet-schedule CRON      Cron schedule for sheet sync (default: */15 * * * *)
                             Set to "off" to disable sheet sync cron line
  --user USER                Install into USER crontab via sudo
  --show-only                Print managed block, do not install
  -h, --help                 Show help

Examples:
  ./scripts/add_google_sync_cron.sh
  ./scripts/add_google_sync_cron.sh --user adamveres
  ./scripts/add_google_sync_cron.sh --sheet-schedule off
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --project-dir)
      PROJECT_DIR="$2"
      LOG_DIR="${PROJECT_DIR}/logs"
      shift 2
      ;;
    --uv-bin)
      UV_BIN="$2"
      shift 2
      ;;
    --uv-args)
      UV_ARGS="$2"
      shift 2
      ;;
    --flock-bin)
      FLOCK_BIN="$2"
      shift 2
      ;;
    --flock-args)
      FLOCK_ARGS="$2"
      shift 2
      ;;
    --bash-bin)
      BASH_BIN="$2"
      shift 2
      ;;
    --drive-schedule)
      DRIVE_SCHEDULE="$2"
      shift 2
      ;;
    --sheet-schedule)
      SHEET_SCHEDULE="$2"
      shift 2
      ;;
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

if [[ ! -d "${PROJECT_DIR}" ]]; then
  echo "Project directory not found: ${PROJECT_DIR}" >&2
  exit 1
fi

if [[ -z "${UV_BIN}" || ! -x "${UV_BIN}" ]]; then
  echo "uv binary not found or not executable: ${UV_BIN}" >&2
  exit 1
fi

if [[ -z "${FLOCK_BIN}" || ! -x "${FLOCK_BIN}" ]]; then
  echo "flock binary not found or not executable: ${FLOCK_BIN}" >&2
  exit 1
fi

if [[ -z "${BASH_BIN}" || ! -x "${BASH_BIN}" ]]; then
  echo "bash binary not found or not executable: ${BASH_BIN}" >&2
  exit 1
fi

mkdir -p "${LOG_DIR}"

uv_args_prefix=""
if [[ -n "${UV_ARGS}" ]]; then
  uv_args_prefix="${UV_ARGS} "
fi

drive_inner="cd \"${PROJECT_DIR}\" && \"${UV_BIN}\" run ${uv_args_prefix}google-workspace-sync sync --mode drive >> \"${LOG_DIR}/drive-sync.log\" 2>&1"
drive_line="${DRIVE_SCHEDULE} ${FLOCK_BIN} ${FLOCK_ARGS} /tmp/gws_drive.lock ${BASH_BIN} -lc '${drive_inner}'"

sheet_line=""
if [[ "${SHEET_SCHEDULE}" != "off" ]]; then
  sheet_inner="cd \"${PROJECT_DIR}\" && \"${UV_BIN}\" run ${uv_args_prefix}google-workspace-sync sync --mode sheet >> \"${LOG_DIR}/sheet-sync.log\" 2>&1"
  sheet_line="${SHEET_SCHEDULE} ${FLOCK_BIN} ${FLOCK_ARGS} /tmp/gws_sheet.lock ${BASH_BIN} -lc '${sheet_inner}'"
fi

managed_block="${MARK_START}
# Managed by scripts/add_google_sync_cron.sh
${drive_line}"

if [[ -n "${sheet_line}" ]]; then
  managed_block="${managed_block}
${sheet_line}"
fi

managed_block="${managed_block}
${MARK_END}"

if [[ "${SHOW_ONLY}" -eq 1 ]]; then
  printf '%s\n' "${managed_block}"
  exit 0
fi

if [[ -n "${TARGET_USER}" ]]; then
  existing="$(sudo crontab -u "${TARGET_USER}" -l 2>/dev/null || true)"
else
  existing="$(crontab -l 2>/dev/null || true)"
fi

cleaned="$(printf '%s\n' "${existing}" | awk -v s="${MARK_START}" -v e="${MARK_END}" '
  $0 == s {skip=1; next}
  $0 == e {skip=0; next}
  !skip {print}
')"

if [[ -n "${cleaned}" ]]; then
  new_crontab="${cleaned}
${managed_block}
"
else
  new_crontab="${managed_block}
"
fi

if [[ -n "${TARGET_USER}" ]]; then
  printf '%s' "${new_crontab}" | sudo crontab -u "${TARGET_USER}" -
  echo "Installed managed cron block for user ${TARGET_USER}."
else
  printf '%s' "${new_crontab}" | crontab -
  echo "Installed managed cron block for current user."
fi
