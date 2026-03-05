import json
import os
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import cast

from googleapiclient.errors import HttpError
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

STATE_VERSION = 1
CHANGES_PAGE_SIZE = 100


@dataclass(frozen=True, slots=True)
class TrackedFileState:
    file_id: str
    name: str
    mime_type: str
    modified_time: str
    local_path: str


@dataclass(frozen=True, slots=True)
class DriveSyncState:
    version: int
    source_folder_id: str
    page_token: str
    files: dict[str, TrackedFileState]


@dataclass(frozen=True, slots=True)
class DriveMetadata:
    file_id: str
    name: str
    mime_type: str
    modified_time: datetime
    parents: tuple[str, ...]
    trashed: bool


class DriveMirrorSync:
    def __init__(
        self,
        drive_client: DriveService,
        source_folder_id: str,
        download_root: Path,
        state_file: Path,
        recursive: bool = True,
        hard_delete: bool = True,
        dry_run: bool = False,
        force_full_reconcile: bool = False,
    ) -> None:
        self.drive_client = drive_client
        self.source_folder_id = source_folder_id
        self.download_root = download_root
        self.state_file = state_file
        self.recursive = recursive
        self.hard_delete = hard_delete
        self.dry_run = dry_run
        self.force_full_reconcile = force_full_reconcile
        self._folder_membership_cache: dict[str, bool] = {}

    def sync(self) -> DriveSyncStats:
        self.download_root.mkdir(parents=True, exist_ok=True)
        self._folder_membership_cache = {}

        state = self._load_state_safely()
        if (
            self.force_full_reconcile
            or state is None
            or state.source_folder_id != self.source_folder_id
            or state.page_token.strip() == ""
        ):
            if self.force_full_reconcile:
                print("[drive-sync] Running full reconcile (--full-reconcile).")
            return self._run_full_reconcile(previous_state=state)

        try:
            return self._run_incremental_sync(state)
        except HttpError as error:
            if _http_status(error) == 410:
                print(
                    "[drive-sync] Stored page token is invalid/expired. "
                    "Falling back to full reconcile."
                )
                return self._run_full_reconcile(previous_state=state)
            raise

    def _run_full_reconcile(
        self,
        previous_state: DriveSyncState | None,
    ) -> DriveSyncStats:
        stats = DriveSyncStats()
        previous_files = previous_state.files if previous_state is not None else {}
        working_files = dict(previous_files)

        drive_files = self._list_folder_files(self.source_folder_id)
        drive_files.sort(key=lambda item: (item.name.lower(), item.file_id))
        stats.discovered_files = len(drive_files)

        seen_file_ids: set[str] = set()
        for drive_file in drive_files:
            seen_file_ids.add(drive_file.file_id)
            self._sync_single_file(drive_file, working_files, stats)

        for file_id in list(working_files):
            if file_id not in seen_file_ids:
                stats.deleted_files += self._remove_from_manifest(
                    file_id, working_files
                )

        if self.hard_delete:
            stats.deleted_files += self._delete_local_extras(working_files)

        if self.dry_run:
            return stats

        if stats.failed_files > 0:
            print(
                "[drive-sync] Full reconcile had failures; state file was not updated."
            )
            return stats

        page_token = self._get_start_page_token()
        self._save_state(
            DriveSyncState(
                version=STATE_VERSION,
                source_folder_id=self.source_folder_id,
                page_token=page_token,
                files=working_files,
            )
        )
        return stats

    def _run_incremental_sync(self, state: DriveSyncState) -> DriveSyncStats:
        stats = DriveSyncStats()
        working_files = dict(state.files)
        page_token = state.page_token

        while True:
            response = (
                self.drive_client.changes()
                .list(
                    pageToken=page_token,
                    spaces="drive",
                    pageSize=CHANGES_PAGE_SIZE,
                    supportsAllDrives=True,
                    includeItemsFromAllDrives=True,
                    fields=(
                        "nextPageToken,newStartPageToken,"
                        "changes(fileId,removed,file(name,mimeType,parents,trashed,modifiedTime))"
                    ),
                )
                .execute()
            )

            changes_raw = response.get("changes", [])
            if isinstance(changes_raw, list):
                changes = cast(list[object], changes_raw)
            else:
                changes = []

            for change_raw in changes:
                if not isinstance(change_raw, dict):
                    continue

                change = cast(dict[str, object], change_raw)
                should_reconcile = self._apply_change(change, working_files, stats)
                if should_reconcile:
                    print(
                        "[drive-sync] Folder structure changed. "
                        "Running full reconcile to ensure consistency."
                    )
                    fallback_state = DriveSyncState(
                        version=STATE_VERSION,
                        source_folder_id=self.source_folder_id,
                        page_token=page_token,
                        files=working_files,
                    )
                    return self._run_full_reconcile(previous_state=fallback_state)

            next_page_token_raw = response.get("nextPageToken")
            if (
                isinstance(next_page_token_raw, str)
                and next_page_token_raw.strip() != ""
            ):
                page_token = next_page_token_raw
                continue

            new_start_page_token_raw = response.get("newStartPageToken")
            if (
                isinstance(new_start_page_token_raw, str)
                and new_start_page_token_raw.strip() != ""
            ):
                page_token = new_start_page_token_raw
            break

        if stats.failed_files > 0:
            print("[drive-sync] Failures detected; state token was not advanced.")
            return stats

        if self.hard_delete:
            stats.deleted_files += self._delete_local_extras(working_files)

        if self.dry_run:
            return stats

        self._save_state(
            DriveSyncState(
                version=STATE_VERSION,
                source_folder_id=self.source_folder_id,
                page_token=page_token,
                files=working_files,
            )
        )
        return stats

    def _apply_change(
        self,
        change: dict[str, object],
        manifest: dict[str, TrackedFileState],
        stats: DriveSyncStats,
    ) -> bool:
        file_id = str(change.get("fileId", "")).strip()
        if file_id == "":
            return False

        stats.discovered_files += 1
        removed = bool(change.get("removed", False))
        if removed:
            stats.deleted_files += self._remove_from_manifest(file_id, manifest)
            return False

        metadata = self._fetch_file_metadata(file_id)
        if metadata is None or metadata.trashed:
            stats.deleted_files += self._remove_from_manifest(file_id, manifest)
            return False

        if metadata.mime_type == GOOGLE_FOLDER_MIME_TYPE:
            return True

        if not self._is_within_source_folder(metadata.parents):
            stats.deleted_files += self._remove_from_manifest(file_id, manifest)
            return False

        drive_file = DriveFile(
            file_id=metadata.file_id,
            name=metadata.name,
            mime_type=metadata.mime_type,
            modified_time=metadata.modified_time,
            folder_parts=(),
        )
        self._sync_single_file(drive_file, manifest, stats)
        return False

    def _sync_single_file(
        self,
        drive_file: DriveFile,
        manifest: dict[str, TrackedFileState],
        stats: DriveSyncStats,
    ) -> None:
        existing_entry = manifest.get(drive_file.file_id)
        relative_destination = self._build_relative_destination(drive_file, manifest)
        destination = self.download_root / relative_destination
        old_relative_path = (
            Path(existing_entry.local_path) if existing_entry is not None else None
        )
        path_changed = (
            old_relative_path is not None and old_relative_path != relative_destination
        )

        try:
            if self._needs_download(destination, drive_file.modified_time):
                self._download_drive_file(drive_file, destination)
                stats.downloaded_files += 1
            else:
                stats.skipped_files += 1

            if path_changed and old_relative_path is not None:
                self._remove_replaced_local_path(
                    old_relative_path, relative_destination
                )

            manifest[drive_file.file_id] = TrackedFileState(
                file_id=drive_file.file_id,
                name=drive_file.name,
                mime_type=drive_file.mime_type,
                modified_time=drive_file.modified_time.isoformat(),
                local_path=str(relative_destination),
            )
        except Exception as error:
            stats.failed_files += 1
            print(
                "[drive-sync] Failed to sync file "
                f"{drive_file.file_id} ({drive_file.name}): {error}"
            )

    def _build_relative_destination(
        self,
        drive_file: DriveFile,
        manifest: dict[str, TrackedFileState],
    ) -> Path:
        filename = self._build_local_filename(drive_file)
        candidate = Path(filename)

        used_paths = {
            Path(entry.local_path)
            for file_id, entry in manifest.items()
            if file_id != drive_file.file_id
        }
        if candidate not in used_paths:
            return candidate

        path_obj = Path(filename)
        stem = path_obj.stem
        suffix = path_obj.suffix
        candidate = Path(f"{stem}__{drive_file.file_id}{suffix}")
        duplicate_index = 1

        while candidate in used_paths:
            candidate = Path(f"{stem}__{drive_file.file_id}_{duplicate_index}{suffix}")
            duplicate_index += 1

        return candidate

    def _remove_from_manifest(
        self,
        file_id: str,
        manifest: dict[str, TrackedFileState],
    ) -> int:
        entry = manifest.pop(file_id, None)
        if entry is None:
            return 0

        if not self.hard_delete:
            return 0

        local_path = self.download_root / entry.local_path
        if self.dry_run:
            print(f"[drive-sync] DRY RUN delete: {local_path}")
            return 1

        if local_path.exists() and local_path.is_file():
            local_path.unlink()
            self._remove_empty_directories()
            return 1

        return 0

    def _remove_replaced_local_path(
        self,
        old_relative_path: Path,
        new_relative_path: Path,
    ) -> None:
        if old_relative_path == new_relative_path:
            return

        old_local_path = self.download_root / old_relative_path
        if self.dry_run:
            print(f"[drive-sync] DRY RUN delete replaced file: {old_local_path}")
            return

        if old_local_path.exists() and old_local_path.is_file():
            old_local_path.unlink()
            self._remove_empty_directories()

    def _delete_local_extras(self, manifest: dict[str, TrackedFileState]) -> int:
        tracked_paths = {Path(entry.local_path) for entry in manifest.values()}
        deleted_files = 0
        local_files = sorted(
            (path for path in self.download_root.rglob("*") if path.is_file()),
            key=lambda path: len(path.parts),
            reverse=True,
        )

        for local_path in local_files:
            relative_path = local_path.relative_to(self.download_root)
            if relative_path in tracked_paths:
                continue

            if self.dry_run:
                print(f"[drive-sync] DRY RUN delete: {local_path}")
            else:
                local_path.unlink()
            deleted_files += 1

        self._remove_empty_directories()
        return deleted_files

    def _list_folder_files(self, folder_id: str) -> list[DriveFile]:
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
                    fields="nextPageToken,files(id,name,mimeType,modifiedTime)",
                )
                .execute()
            )

            file_items_raw = response.get("files", [])
            if isinstance(file_items_raw, list):
                file_items = cast(list[object], file_items_raw)
            else:
                file_items = []

            for file_item in file_items:
                if not isinstance(file_item, dict):
                    continue

                item = cast(dict[str, object], file_item)
                file_id = str(item.get("id", "")).strip()
                if file_id == "":
                    continue

                mime_type = str(item.get("mimeType", "")).strip()
                name = str(item.get("name", file_id)).strip()

                if mime_type == GOOGLE_FOLDER_MIME_TYPE:
                    if self.recursive:
                        discovered_files.extend(self._list_folder_files(file_id))
                    continue

                modified_time_raw = str(item.get("modifiedTime", "")).strip()
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
                        folder_parts=(),
                    )
                )

            next_page_token_raw = response.get("nextPageToken")
            if (
                isinstance(next_page_token_raw, str)
                and next_page_token_raw.strip() != ""
            ):
                page_token = next_page_token_raw
            else:
                break

        return discovered_files

    def _fetch_file_metadata(self, file_id: str) -> DriveMetadata | None:
        try:
            response = (
                self.drive_client.files()
                .get(
                    fileId=file_id,
                    supportsAllDrives=True,
                    fields="id,name,mimeType,modifiedTime,parents,trashed",
                )
                .execute()
            )
        except HttpError as error:
            if _http_status(error) == 404:
                return None
            raise

        return self._metadata_from_response(response)

    def _metadata_from_response(
        self, response: dict[str, object]
    ) -> DriveMetadata | None:
        file_id = str(response.get("id", "")).strip()
        if file_id == "":
            return None

        name = str(response.get("name", file_id)).strip()
        mime_type = str(response.get("mimeType", "")).strip()
        if mime_type == "":
            return None

        modified_time_raw = str(response.get("modifiedTime", "")).strip()
        if modified_time_raw == "":
            modified_time = datetime.fromtimestamp(0)
        else:
            try:
                modified_time = _parse_google_timestamp(modified_time_raw)
            except ValueError:
                return None

        parents_raw = response.get("parents", [])
        parents: tuple[str, ...]
        if isinstance(parents_raw, list):
            parents = tuple(
                str(parent).strip() for parent in parents_raw if str(parent).strip()
            )
        else:
            parents = ()

        trashed = bool(response.get("trashed", False))
        return DriveMetadata(
            file_id=file_id,
            name=name,
            mime_type=mime_type,
            modified_time=modified_time,
            parents=parents,
            trashed=trashed,
        )

    def _is_within_source_folder(self, parents: tuple[str, ...]) -> bool:
        if self.source_folder_id in parents:
            return True

        for parent_id in parents:
            if self._folder_is_within_source(parent_id, set()):
                return True
        return False

    def _folder_is_within_source(self, folder_id: str, visiting: set[str]) -> bool:
        if folder_id == self.source_folder_id:
            return True

        cached = self._folder_membership_cache.get(folder_id)
        if cached is not None:
            return cached

        if folder_id in visiting:
            return False
        visiting.add(folder_id)

        metadata = self._fetch_file_metadata(folder_id)
        if metadata is None or metadata.trashed:
            result = False
        elif self.source_folder_id in metadata.parents:
            result = True
        elif not metadata.parents:
            result = False
        else:
            result = any(
                self._folder_is_within_source(parent_id, visiting)
                for parent_id in metadata.parents
            )

        visiting.remove(folder_id)
        self._folder_membership_cache[folder_id] = result
        return result

    def _get_start_page_token(self) -> str:
        response = (
            self.drive_client.changes()
            .getStartPageToken(
                supportsAllDrives=True,
            )
            .execute()
        )
        token_raw = response.get("startPageToken")
        if not isinstance(token_raw, str) or token_raw.strip() == "":
            raise ValueError("Drive API did not return a valid startPageToken")
        return token_raw

    def _load_state_safely(self) -> DriveSyncState | None:
        try:
            return self._load_state()
        except Exception as error:
            print(
                "[drive-sync] Failed to read state file "
                f"{self.state_file}: {error}. Falling back to full reconcile."
            )
            return None

    def _load_state(self) -> DriveSyncState | None:
        if not self.state_file.exists():
            return None

        raw_text = self.state_file.read_text(encoding="utf-8")
        raw_state = json.loads(raw_text)
        if not isinstance(raw_state, dict):
            raise ValueError("state file root must be a JSON object")

        version = raw_state.get("version")
        if not isinstance(version, int) or version != STATE_VERSION:
            raise ValueError(
                f"state version mismatch: expected {STATE_VERSION}, got {version}"
            )

        source_folder_id = str(raw_state.get("source_folder_id", "")).strip()
        page_token = str(raw_state.get("page_token", "")).strip()
        files_raw = raw_state.get("files", {})
        if not isinstance(files_raw, dict):
            raise ValueError("state.files must be an object")

        files: dict[str, TrackedFileState] = {}
        for file_id_raw, entry_raw in files_raw.items():
            file_id = str(file_id_raw).strip()
            if file_id == "" or not isinstance(entry_raw, dict):
                continue

            name = str(entry_raw.get("name", "")).strip()
            mime_type = str(entry_raw.get("mime_type", "")).strip()
            modified_time = str(entry_raw.get("modified_time", "")).strip()
            local_path = str(entry_raw.get("local_path", "")).strip()
            if local_path == "":
                continue

            files[file_id] = TrackedFileState(
                file_id=file_id,
                name=name,
                mime_type=mime_type,
                modified_time=modified_time,
                local_path=local_path,
            )

        return DriveSyncState(
            version=STATE_VERSION,
            source_folder_id=source_folder_id,
            page_token=page_token,
            files=files,
        )

    def _save_state(self, state: DriveSyncState) -> None:
        self.state_file.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "version": state.version,
            "source_folder_id": state.source_folder_id,
            "page_token": state.page_token,
            "files": {
                file_id: {
                    "name": entry.name,
                    "mime_type": entry.mime_type,
                    "modified_time": entry.modified_time,
                    "local_path": entry.local_path,
                }
                for file_id, entry in state.files.items()
            },
        }

        temp_path = self.state_file.with_suffix(f"{self.state_file.suffix}.tmp")
        temp_path.write_text(
            json.dumps(payload, ensure_ascii=True, indent=2, sort_keys=True),
            encoding="utf-8",
        )
        temp_path.replace(self.state_file)

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
        if not destination.exists() or destination.is_dir():
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


def _http_status(error: HttpError) -> int | None:
    response = getattr(error, "resp", None)
    if response is None:
        return None

    status = getattr(response, "status", None)
    if isinstance(status, int):
        return status
    return None
