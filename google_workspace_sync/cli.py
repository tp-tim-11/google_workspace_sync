from argparse import ArgumentParser, Namespace
from collections.abc import Callable, Sequence
from pathlib import Path

from .drive_sync import DriveMirrorSync
from .google_clients import build_drive_client, build_sheets_client
from .settings import Settings, settings
from .sheet_sync import (
    SheetSyncService,
    ensure_resources_schema,
    validate_resources_schema,
)


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
        choices=("drive", "sheet", "all"),
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

    return parser


def run_sync(arguments: Namespace, settings: Settings) -> int:
    exit_code = 0
    _validate_credentials_path(settings)

    if arguments.mode in {"drive", "all"}:
        _require_drive_folder_id(settings)
        drive_client = build_drive_client(settings)
        hard_delete = settings.drive_hard_delete and not arguments.no_hard_delete

        drive_sync = DriveMirrorSync(
            drive_client=drive_client,
            source_folder_id=settings.google_drive_documents_folder_id,
            download_root=Path(settings.drive_download_root).expanduser(),
            state_file=Path(settings.drive_state_file).expanduser(),
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

    return exit_code


def run_init_db(_arguments: Namespace, settings: Settings) -> int:
    _require_database_settings(settings)
    ensure_resources_schema(settings)
    validate_resources_schema(settings)
    print("[sheet-sync] database schema reset and ready.")
    return 0


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
        raise ValueError("GOOGLE_SHEETS_ID must be set for sheet sync.")
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
            f"Database settings missing for sheet sync/init-db: {', '.join(missing)}"
        )


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
