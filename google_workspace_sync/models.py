from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True, slots=True)
class DriveFile:
    file_id: str
    name: str
    mime_type: str
    modified_time: datetime
    folder_parts: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class ResourceRow:
    nazov: str
    pozicia: str | None
    led_pozicia: str | None
    status: str | None
    vypozicane_komu: str | None


@dataclass(slots=True)
class DriveSyncStats:
    discovered_files: int = 0
    downloaded_files: int = 0
    skipped_files: int = 0
    deleted_files: int = 0
    failed_files: int = 0


@dataclass(slots=True)
class SheetSyncStats:
    fetched_rows: int = 0
    upserted_rows: int = 0
    soft_deleted_rows: int = 0
    skipped_rows: int = 0
