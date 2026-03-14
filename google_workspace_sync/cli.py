from argparse import ArgumentParser, Namespace
from collections.abc import Callable, Sequence
from pathlib import Path
import time

from .drive_sync import DriveMirrorSync
from .google_clients import build_drive_client, build_sheets_client
from .ingest_sync import Uc2IngestService, load_drive_manifest
from .settings import Settings, settings
from .sheet_sync import (
    SheetPushService,
    SheetSyncService,
    ensure_resources_notify_trigger,
    ensure_resources_schema,
    listen_for_resource_changes,
    validate_resources_schema,
)

DEFAULT_RESOURCES_NOTIFY_CHANNEL = "resources_changed"


def build_parser() -> ArgumentParser:
    parser = ArgumentParser(
        prog="google-workspace-sync",
        description=(
            "One-shot Google Workspace pull sync for cron: "
            "Drive folder mirror + optional Sheets to PostgreSQL sync."
        ),
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    sync_parser = subparsers.add_parser(
        "sync",
        help="Run one pull synchronization pass.",
    )
    sync_parser.add_argument(
        "--mode",
        choices=("drive", "sheet", "sheet-push", "all"),
        default="drive",
        help="Select what to sync (default: drive).",
    )
    sync_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print intended changes without modifying files.",
    )
    sync_parser.add_argument(
        "--no-hard-delete",
        action="store_true",
        help="Disable hard mirror delete for this run.",
    )
    sync_parser.add_argument(
        "--full-reconcile",
        action="store_true",
        help="Force full Drive reconcile instead of token-based incremental sync.",
    )
    sync_parser.set_defaults(handler=run_sync)

    init_db_parser = subparsers.add_parser(
        "init-db",
        aliases=("init",),
        help="Reset and recreate the PostgreSQL sync schema.",
    )
    init_db_parser.set_defaults(handler=run_init_db)

    watch_sheet_push_parser = subparsers.add_parser(
        "watch-sheet-push",
        help="Listen for DB changes and push resources to Google Sheets.",
    )
    watch_sheet_push_parser.add_argument(
        "--channel",
        default="",
        help=(
            "PostgreSQL NOTIFY channel name "
            f"(default: {DEFAULT_RESOURCES_NOTIFY_CHANNEL})."
        ),
    )
    watch_sheet_push_parser.add_argument(
        "--poll-timeout",
        type=float,
        default=None,
        help="LISTEN poll timeout in seconds (default: SHEET_PUSH_POLL_TIMEOUT_SECONDS).",
    )
    watch_sheet_push_parser.add_argument(
        "--skip-initial-push",
        action="store_true",
        help="Do not perform an initial full push before listening.",
    )
    watch_sheet_push_parser.set_defaults(handler=run_watch_sheet_push)

    return parser


def run_sync(arguments: Namespace, settings: Settings) -> int:
    exit_code = 0
    _validate_credentials_path(settings)

    if arguments.mode in {"drive", "all"}:
        _require_drive_folder_id(settings)
        if settings.drive_ingest_enabled and not arguments.dry_run:
            _require_database_settings(settings)
        drive_client = build_drive_client(settings)
        hard_delete = settings.drive_hard_delete and not arguments.no_hard_delete
        drive_download_root = Path(settings.drive_download_root).expanduser()
        drive_state_file = Path(settings.drive_state_file).expanduser()
        previous_drive_manifest = (
            load_drive_manifest(drive_state_file)
            if settings.drive_ingest_enabled and not arguments.dry_run
            else {}
        )

        drive_sync = DriveMirrorSync(
            drive_client=drive_client,
            source_folder_id=settings.google_drive_documents_folder_id,
            download_root=drive_download_root,
            state_file=drive_state_file,
            recursive=settings.drive_recursive,
            hard_delete=hard_delete,
            dry_run=arguments.dry_run,
            force_full_reconcile=arguments.full_reconcile,
        )
        drive_stats = drive_sync.sync()
        print(
            "[drive-sync] "
            f"discovered={drive_stats.discovered_files} "
            f"downloaded={drive_stats.downloaded_files} "
            f"skipped={drive_stats.skipped_files} "
            f"deleted={drive_stats.deleted_files} "
            f"failed={drive_stats.failed_files}"
        )

        if drive_stats.failed_files > 0:
            exit_code = 1

        if settings.drive_ingest_enabled and not arguments.dry_run:
            if drive_stats.failed_files > 0:
                print("[uc2-ingest] Skipped because drive sync reported failures.")
            else:
                current_drive_manifest = load_drive_manifest(drive_state_file)
                ingest_service = Uc2IngestService(
                    settings=settings,
                    download_root=drive_download_root,
                )
                ingest_stats = ingest_service.sync(
                    sync_mode=drive_stats.sync_mode,
                    previous_manifest=previous_drive_manifest,
                    current_manifest=current_drive_manifest,
                )
                print(
                    "[uc2-ingest] "
                    f"ingested={ingest_stats.ingested_files} "
                    f"deleted={ingest_stats.deleted_docs} "
                    f"failed={ingest_stats.failed_files}"
                )
                if ingest_stats.failed_files > 0:
                    exit_code = 1

    if arguments.mode in {"sheet", "all"}:
        if arguments.dry_run:
            raise ValueError("Sheet sync does not support --dry-run yet.")

        _require_database_settings(settings)
        sheet_id = _require_sheet_id(settings)
        sheet_range = settings.google_sheets_range.strip() or "Sheet1!A:F"

        sheets_client = build_sheets_client(settings)
        sheet_sync = SheetSyncService(
            sheets_client=sheets_client,
            settings=settings,
            spreadsheet_id=sheet_id,
            spreadsheet_range=sheet_range,
        )
        sheet_stats = sheet_sync.sync()
        print(
            "[sheet-sync] "
            f"fetched={sheet_stats.fetched_rows} "
            f"upserted={sheet_stats.upserted_rows} "
            f"soft_deleted={sheet_stats.soft_deleted_rows} "
            f"skipped={sheet_stats.skipped_rows}"
        )

    if arguments.mode == "sheet-push":
        if arguments.dry_run:
            raise ValueError("Sheet push does not support --dry-run yet.")

        _require_database_settings(settings)
        validate_resources_schema(settings)
        push_service = _build_sheet_push_service(settings)
        pushed_rows = push_service.push()
        print(f"[sheet-push] pushed_rows={pushed_rows}")

    return exit_code


def run_init_db(_arguments: Namespace, settings: Settings) -> int:
    _require_database_settings(settings)
    ensure_resources_schema(settings)
    validate_resources_schema(settings)
    print("[sheet-sync] database schema reset and ready.")
    return 0


def run_watch_sheet_push(arguments: Namespace, settings: Settings) -> int:
    _validate_credentials_path(settings)
    _require_database_settings(settings)

    channel = arguments.channel.strip() or DEFAULT_RESOURCES_NOTIFY_CHANNEL
    poll_timeout_raw = arguments.poll_timeout
    poll_timeout = (
        settings.sheet_push_poll_timeout_seconds
        if poll_timeout_raw is None
        else poll_timeout_raw
    )
    sheet_id = _require_sheet_id(settings)
    sheet_range = settings.google_sheets_range.strip() or "Sheet1!A:F"

    print(
        "[sheet-push-watch] startup "
        f"db_host={settings.db_host} db_name={settings.db_name} "
        f"db_user={settings.db_user} channel={channel} "
        f"poll_timeout={poll_timeout:.2f}s "
        f"sheet_id={_abbreviate_id(sheet_id)} range={sheet_range}"
    )

    validate_resources_schema(settings)
    push_service = _build_sheet_push_service(settings)
    ensure_resources_notify_trigger(settings, channel=channel)
    print(f"[sheet-push-watch] ensured notify trigger channel={channel}")

    def push_with_log(trigger: str) -> int:
        started_at = time.monotonic()
        pushed_rows = push_service.push()
        elapsed_seconds = time.monotonic() - started_at
        print(
            "[sheet-push] "
            f"pushed_rows={pushed_rows} trigger={trigger} "
            f"elapsed={elapsed_seconds:.2f}s"
        )
        return pushed_rows

    def on_change() -> None:
        push_with_log("db-notify")

    if not arguments.skip_initial_push:
        push_with_log("startup")
    else:
        print("[sheet-push-watch] startup push skipped")

    try:
        listen_for_resource_changes(
            settings=settings,
            channel=channel,
            poll_timeout_seconds=poll_timeout,
            on_change=on_change,
        )
    except KeyboardInterrupt:
        print("[sheet-push-watch] stopped")

    return 0


def _build_sheet_push_service(settings: Settings) -> SheetPushService:
    sheet_id = _require_sheet_id(settings)
    sheet_range = settings.google_sheets_range.strip() or "Sheet1!A:F"
    sheets_client = build_sheets_client(settings)
    return SheetPushService(
        sheets_client=sheets_client,
        settings=settings,
        spreadsheet_id=sheet_id,
        spreadsheet_range=sheet_range,
    )


def _validate_credentials_path(settings: Settings) -> None:
    credentials_path = Path(
        settings.google_service_account_credentials_file
    ).expanduser()
    if not credentials_path.exists():
        raise ValueError(
            f"Service account credentials file does not exist: {credentials_path}"
        )


def _require_drive_folder_id(settings: Settings) -> str:
    value = settings.google_drive_documents_folder_id.strip()
    if value == "":
        raise ValueError("GOOGLE_DRIVE_DOCUMENTS_FOLDER_ID must be set for drive sync.")
    return value


def _require_sheet_id(settings: Settings) -> str:
    value = settings.google_sheets_id.strip()
    if value == "":
        raise ValueError("GOOGLE_SHEETS_ID must be set for sheet sync/push.")
    return value


def _require_database_settings(settings: Settings) -> None:
    missing: list[str] = []
    if settings.db_host.strip() == "":
        missing.append("db_host")
    if settings.db_name.strip() == "":
        missing.append("db_name")
    if settings.db_user.strip() == "":
        missing.append("db_user")
    if settings.db_password.strip() == "":
        missing.append("db_password")

    if missing:
        raise ValueError(
            "Database settings missing for "
            "sheet sync/sheet push/watch/init-db/drive ingest: "
            f"{', '.join(missing)}"
        )


def _abbreviate_id(value: str) -> str:
    cleaned = value.strip()
    if len(cleaned) <= 10:
        return cleaned
    return f"{cleaned[:6]}...{cleaned[-4:]}"


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    arguments = parser.parse_args(argv)

    try:
        handler: Callable[[Namespace, Settings], int] = arguments.handler
        return handler(arguments, settings)
    except Exception as error:
        print(f"[error] {error}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
