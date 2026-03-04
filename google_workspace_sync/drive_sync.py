import os
from datetime import datetime
from pathlib import Path
from typing import cast

from googleapiclient.http import MediaIoBaseDownload

from .google_api_protocols import DriveService
from .models import DriveFile, DriveSyncStats

GOOGLE_FOLDER_MIME_TYPE = "application/vnd.google-apps.folder"
GOOGLE_APPS_PREFIX = "application/vnd.google-apps."

GOOGLE_EXPORT_MIME_MAP = {
    "application/vnd.google-apps.document": "text/plain",
    "application/vnd.google-apps.spreadsheet": "text/csv",
    "application/vnd.google-apps.presentation": "application/pdf",
}

EXPORT_EXTENSION_BY_MIME = {
    "text/plain": ".txt",
    "text/csv": ".csv",
    "application/pdf": ".pdf",
}


class DriveMirrorSync:
    def __init__(
        self,
        drive_client: DriveService,
        source_folder_id: str,
        download_root: Path,
        recursive: bool = True,
        hard_delete: bool = True,
        dry_run: bool = False,
    ) -> None:
        self.drive_client = drive_client
        self.source_folder_id = source_folder_id
        self.download_root = download_root
        self.recursive = recursive
        self.hard_delete = hard_delete
        self.dry_run = dry_run

    def sync(self) -> DriveSyncStats:
        stats = DriveSyncStats()
        self.download_root.mkdir(parents=True, exist_ok=True)

        drive_files = self._list_folder_files(self.source_folder_id, ())
        drive_files.sort(
            key=lambda item: (
                item.folder_parts,
                item.name.lower(),
                item.file_id,
            )
        )
        stats.discovered_files = len(drive_files)

        expected_paths: set[Path] = set()
        used_paths: set[Path] = set()

        for drive_file in drive_files:
            relative_destination = self._build_relative_destination(
                drive_file=drive_file,
                used_paths=used_paths,
            )
            expected_paths.add(relative_destination)
            destination = self.download_root / relative_destination

            try:
                if self._needs_download(destination, drive_file.modified_time):
                    self._download_drive_file(drive_file, destination)
                    stats.downloaded_files += 1
                else:
                    stats.skipped_files += 1
            except Exception as error:
                stats.failed_files += 1
                print(
                    "[drive-sync] Failed to sync file "
                    f"{drive_file.file_id} ({drive_file.name}): {error}"
                )

        if self.hard_delete:
            stats.deleted_files = self._delete_local_extras(expected_paths)

        return stats

    def _list_folder_files(
        self,
        folder_id: str,
        folder_parts: tuple[str, ...],
    ) -> list[DriveFile]:
        discovered_files: list[DriveFile] = []
        page_token: str | None = None
        query = f"'{folder_id}' in parents and trashed = false"

        while True:
            response = (
                self.drive_client.files()
                .list(
                    q=query,
                    pageToken=page_token,
                    supportsAllDrives=True,
                    includeItemsFromAllDrives=True,
                    fields=(
                        "nextPageToken,files(id,name,mimeType,modifiedTime,trashed)"
                    ),
                )
                .execute()
            )

            file_items_raw = response.get("files", [])
            if isinstance(file_items_raw, list):
                file_items: list[object] = cast(list[object], file_items_raw)
            else:
                file_items = []

            for file_info in file_items:
                if not isinstance(file_info, dict):
                    continue

                file_info_dict = cast(dict[str, object], file_info)

                file_id = str(file_info_dict.get("id", "")).strip()
                if file_id == "":
                    continue

                mime_type = str(file_info_dict.get("mimeType", "")).strip()
                name = str(file_info_dict.get("name", file_id)).strip()

                if mime_type == GOOGLE_FOLDER_MIME_TYPE:
                    if not self.recursive:
                        continue
                    nested_parts = folder_parts + (name,)
                    discovered_files.extend(
                        self._list_folder_files(
                            folder_id=file_id,
                            folder_parts=nested_parts,
                        )
                    )
                    continue

                modified_time_raw = str(file_info_dict.get("modifiedTime", "")).strip()
                if modified_time_raw == "":
                    print(f"[drive-sync] Skipping file without modifiedTime: {file_id}")
                    continue

                try:
                    modified_time = _parse_google_timestamp(modified_time_raw)
                except ValueError:
                    print(
                        "[drive-sync] Skipping file with invalid modifiedTime: "
                        f"{file_id} ({modified_time_raw})"
                    )
                    continue

                discovered_files.append(
                    DriveFile(
                        file_id=file_id,
                        name=name,
                        mime_type=mime_type,
                        modified_time=modified_time,
                        folder_parts=folder_parts,
                    )
                )

            next_page_token_raw = response.get("nextPageToken")
            if isinstance(next_page_token_raw, str):
                page_token = next_page_token_raw
            else:
                break

        return discovered_files

    def _build_relative_destination(
        self,
        drive_file: DriveFile,
        used_paths: set[Path],
    ) -> Path:
        filename = self._build_local_filename(drive_file)
        candidate = Path(filename)

        if candidate not in used_paths:
            used_paths.add(candidate)
            return candidate

        path_obj = Path(filename)
        stem = path_obj.stem
        suffix = path_obj.suffix

        duplicate_name = f"{stem}__{drive_file.file_id}{suffix}"
        candidate = Path(duplicate_name)
        duplicate_index = 1

        while candidate in used_paths:
            duplicate_name = f"{stem}__{drive_file.file_id}_{duplicate_index}{suffix}"
            candidate = Path(duplicate_name)
            duplicate_index += 1

        used_paths.add(candidate)
        return candidate

    def _build_local_filename(self, drive_file: DriveFile) -> str:
        base_name = _sanitize_path_segment(drive_file.name, drive_file.file_id)
        export_mime = GOOGLE_EXPORT_MIME_MAP.get(drive_file.mime_type)

        if export_mime is None:
            return base_name

        extension = EXPORT_EXTENSION_BY_MIME[export_mime]
        current_path = Path(base_name)

        if current_path.suffix.lower() == extension:
            return base_name
        if current_path.suffix:
            return f"{current_path.stem}{extension}"
        return f"{base_name}{extension}"

    @staticmethod
    def _needs_download(destination: Path, modified_time: datetime) -> bool:
        if not destination.exists():
            return True
        if destination.is_dir():
            return True

        local_timestamp = destination.stat().st_mtime
        remote_timestamp = modified_time.timestamp()
        return abs(local_timestamp - remote_timestamp) > 1.0

    def _download_drive_file(self, drive_file: DriveFile, destination: Path) -> None:
        if self.dry_run:
            print(
                f"[drive-sync] DRY RUN download: {drive_file.file_id} -> {destination}"
            )
            return

        if destination.exists() and destination.is_dir():
            raise IsADirectoryError(f"Destination path is a directory: {destination}")

        destination.parent.mkdir(parents=True, exist_ok=True)
        temp_path = destination.with_name(f"{destination.name}.tmp")

        if temp_path.exists():
            temp_path.unlink()

        request = self._build_download_request(drive_file)
        with temp_path.open("wb") as file_handle:
            downloader = MediaIoBaseDownload(file_handle, request)
            done = False
            while not done:
                _, done = downloader.next_chunk()

        remote_timestamp = drive_file.modified_time.timestamp()
        os.utime(temp_path, (remote_timestamp, remote_timestamp))
        temp_path.replace(destination)

    def _build_download_request(self, drive_file: DriveFile) -> object:
        export_mime = GOOGLE_EXPORT_MIME_MAP.get(drive_file.mime_type)
        if export_mime is not None:
            return self.drive_client.files().export(
                fileId=drive_file.file_id,
                mimeType=export_mime,
            )

        if drive_file.mime_type.startswith(GOOGLE_APPS_PREFIX):
            raise ValueError(
                "Unsupported Google Workspace MIME type "
                f"for export: {drive_file.mime_type}"
            )

        return self.drive_client.files().get_media(fileId=drive_file.file_id)

    def _delete_local_extras(self, expected_paths: set[Path]) -> int:
        if not self.download_root.exists():
            return 0

        deleted_files = 0
        local_files = sorted(
            (path for path in self.download_root.rglob("*") if path.is_file()),
            key=lambda path: len(path.parts),
            reverse=True,
        )

        for local_path in local_files:
            relative_path = local_path.relative_to(self.download_root)
            if relative_path in expected_paths:
                continue

            if self.dry_run:
                print(f"[drive-sync] DRY RUN delete: {local_path}")
            else:
                local_path.unlink()
            deleted_files += 1

        self._remove_empty_directories()
        return deleted_files

    def _remove_empty_directories(self) -> None:
        directories = sorted(
            (path for path in self.download_root.rglob("*") if path.is_dir()),
            key=lambda path: len(path.parts),
            reverse=True,
        )

        for directory in directories:
            try:
                next(directory.iterdir())
            except StopIteration:
                if self.dry_run:
                    print(f"[drive-sync] DRY RUN remove empty dir: {directory}")
                else:
                    directory.rmdir()


def _parse_google_timestamp(raw_value: str) -> datetime:
    if raw_value.endswith("Z"):
        raw_value = f"{raw_value[:-1]}+00:00"
    return datetime.fromisoformat(raw_value)


def _sanitize_path_segment(segment: str, fallback: str) -> str:
    cleaned = segment.strip().replace("/", "_").replace("\\", "_")
    if cleaned in {"", ".", ".."}:
        return fallback
    return cleaned
