from argparse import ArgumentParser, Namespace
from collections.abc import Callable, Sequence

from .config import Settings, load_settings, require_postgres, require_sheet_id
from .drive_sync import DriveMirrorSync
from .google_clients import build_drive_client, build_sheets_client
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
    sync_parser.set_defaults(handler=run_sync)

    init_db_parser = subparsers.add_parser(
        "init-db",
        help="Create resources table/indexes for sheet sync.",
    )
    init_db_parser.set_defaults(handler=run_init_db)

    return parser


def run_sync(arguments: Namespace, settings: Settings) -> int:
    exit_code = 0

    if arguments.mode in {"drive", "all"}:
        drive_client = build_drive_client(settings)
        hard_delete = settings.drive_hard_delete and not arguments.no_hard_delete

        drive_sync = DriveMirrorSync(
            drive_client=drive_client,
            source_folder_id=settings.google_drive_documents_folder_id,
            download_root=settings.drive_download_root,
            recursive=settings.drive_recursive,
            hard_delete=hard_delete,
            dry_run=arguments.dry_run,
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

        sheet_id = require_sheet_id(settings)
        postgres = require_postgres(settings)

        sheets_client = build_sheets_client(settings)
        sheet_sync = SheetSyncService(
            sheets_client=sheets_client,
            postgres=postgres,
            spreadsheet_id=sheet_id,
            spreadsheet_range=settings.google_sheets_range,
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
    postgres = require_postgres(settings)
    ensure_resources_schema(postgres)
    validate_resources_schema(postgres)
    print("[sheet-sync] resources schema ready.")
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    arguments = parser.parse_args(argv)

    try:
        settings = load_settings()
        handler: Callable[[Namespace, Settings], int] = arguments.handler
        return handler(arguments, settings)
    except Exception as error:
        print(f"[error] {error}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
