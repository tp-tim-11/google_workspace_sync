import hashlib
import json
import os
import re
import subprocess
import time
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path

from .models import IngestSyncStats
from .postgres import open_postgres_connection
from .settings import Settings

PRIMARY_INGEST_PY = (
    "import sys; "
    "from kluky_mcp.tools.uc02_utils.pageIndexPipeline import ingest_with_pageindex; "
    "ingest_with_pageindex(sys.argv[1], doc_key=sys.argv[2])"
)


@dataclass(frozen=True, slots=True)
class DriveManifestEntry:
    file_id: str
    local_path: str
    modified_time: str


@dataclass(frozen=True, slots=True)
class FileIngestResult:
    file_id: str
    entry: DriveManifestEntry
    success: bool
    elapsed_seconds: float


def _redact_secrets(value: str) -> str:
    redacted = re.sub(r"sk-[A-Za-z0-9_-]+", "[REDACTED_API_KEY]", value)
    redacted = re.sub(r"sk_[A-Za-z0-9_-]+", "[REDACTED_API_KEY]", redacted)
    return redacted


def load_drive_manifest(state_file: Path) -> dict[str, DriveManifestEntry]:
    if not state_file.exists():
        return {}

    raw_text = state_file.read_text(encoding="utf-8")
    payload = json.loads(raw_text)
    if not isinstance(payload, dict):
        return {}

    files_raw = payload.get("files", {})
    if not isinstance(files_raw, dict):
        return {}

    entries: dict[str, DriveManifestEntry] = {}
    for file_id_raw, entry_raw in files_raw.items():
        file_id = str(file_id_raw).strip()
        if file_id == "" or not isinstance(entry_raw, dict):
            continue

        local_path = str(entry_raw.get("local_path", "")).strip()
        modified_time = str(entry_raw.get("modified_time", "")).strip()
        if local_path == "" or modified_time == "":
            continue

        entries[file_id] = DriveManifestEntry(
            file_id=file_id,
            local_path=local_path,
            modified_time=modified_time,
        )

    return entries


class Uc2IngestService:
    def __init__(
        self,
        settings: Settings,
        download_root: Path,
    ) -> None:
        self.settings = settings
        self.download_root = download_root
        self.kluky_project_root = (
            Path(settings.kluky_mcp_project_root).expanduser().resolve()
        )
        if not self.kluky_project_root.exists():
            raise ValueError(
                f"KLUKY_MCP_PROJECT_ROOT does not exist: {self.kluky_project_root}"
            )
        if not self.kluky_project_root.is_dir():
            raise ValueError(
                f"KLUKY_MCP_PROJECT_ROOT is not a directory: {self.kluky_project_root}"
            )
        self.command_cwd = self.kluky_project_root

    def sync(
        self,
        *,
        sync_mode: str,
        previous_manifest: dict[str, DriveManifestEntry],
        current_manifest: dict[str, DriveManifestEntry],
    ) -> IngestSyncStats:
        if sync_mode == "full_reconcile":
            return self._sync_full_reconcile(current_manifest)

        return self._sync_incremental(
            previous_manifest=previous_manifest,
            current_manifest=current_manifest,
        )

    def _sync_full_reconcile(
        self,
        current_manifest: dict[str, DriveManifestEntry],
    ) -> IngestSyncStats:
        stats = IngestSyncStats()
        self._truncate_doc_units()

        ingest_results = self._ingest_manifest_entries(
            sorted(current_manifest.items()),
            phase_label="full-reconcile",
        )

        for ingest_result in ingest_results:
            if ingest_result.success:
                stats.ingested_files += 1
            else:
                stats.failed_files += 1

        return stats

    def _sync_incremental(
        self,
        *,
        previous_manifest: dict[str, DriveManifestEntry],
        current_manifest: dict[str, DriveManifestEntry],
    ) -> IngestSyncStats:
        stats = IngestSyncStats()

        removed_file_ids = sorted(set(previous_manifest) - set(current_manifest))
        for file_id in removed_file_ids:
            doc_id = _stable_doc_id_from_doc_key(file_id)
            if self._delete_doc_units_for_doc_id(doc_id):
                stats.deleted_docs += 1

        changed_file_ids = {
            file_id
            for file_id, current_entry in current_manifest.items()
            if _manifest_changed(previous_manifest.get(file_id), current_entry)
        }

        doc_ids_by_file_id = {
            file_id: _stable_doc_id_from_doc_key(file_id)
            for file_id in current_manifest
        }
        existing_doc_ids = self._fetch_existing_doc_ids(
            set(doc_ids_by_file_id.values())
        )
        retry_file_ids = {
            file_id
            for file_id, doc_id in doc_ids_by_file_id.items()
            if doc_id not in existing_doc_ids
        }

        ingest_file_ids = sorted(changed_file_ids | retry_file_ids)

        ingest_results = self._ingest_manifest_entries(
            [(file_id, current_manifest[file_id]) for file_id in ingest_file_ids],
            phase_label="incremental",
        )

        for ingest_result in ingest_results:
            if ingest_result.success:
                stats.ingested_files += 1
            else:
                stats.failed_files += 1

        return stats

    def _run_kluky_ingest(self, entry: DriveManifestEntry, file_id: str) -> bool:
        input_path = (self.download_root / entry.local_path).resolve()
        if not input_path.exists() or not input_path.is_file():
            print(
                f"[uc2-ingest] Skipping missing local file for {file_id}: {input_path}"
            )
            return False

        primary_command = [
            "uv",
            "run",
            "--project",
            str(self.kluky_project_root),
            "python",
            "-c",
            PRIMARY_INGEST_PY,
            str(input_path),
            file_id,
        ]
        primary = self._run_kluky_command(primary_command)
        if primary.returncode == 0:
            return True

        stdout_text = primary.stdout.strip()
        stderr_text = primary.stderr.strip()
        redacted_stdout = _redact_secrets(stdout_text)
        redacted_stderr = _redact_secrets(stderr_text)
        print(
            "[uc2-ingest] Ingest failed for "
            f"{entry.local_path} ({file_id}). "
            f"exit={primary.returncode}"
        )
        if redacted_stdout:
            print(f"[uc2-ingest] stdout: {redacted_stdout}")
        if redacted_stderr:
            print(f"[uc2-ingest] stderr: {redacted_stderr}")
        return False

    def _ingest_manifest_entries(
        self,
        entries: list[tuple[str, DriveManifestEntry]],
        *,
        phase_label: str,
    ) -> list[FileIngestResult]:
        if not entries:
            return []

        worker_count = max(1, self.settings.drive_ingest_workers)
        worker_count = min(worker_count, len(entries))
        total = len(entries)
        print(f"[uc2-ingest] phase={phase_label} files={total} workers={worker_count}")

        results: list[FileIngestResult] = []
        if worker_count == 1:
            for index, (file_id, entry) in enumerate(entries, start=1):
                print(
                    "[uc2-ingest] "
                    f"started {index}/{total}: {entry.local_path} ({file_id})"
                )
                started_at = time.monotonic()
                success = self._run_kluky_ingest(entry, file_id)
                elapsed_seconds = time.monotonic() - started_at
                status_label = "ok" if success else "failed"
                print(
                    "[uc2-ingest] "
                    f"finished {index}/{total}: {entry.local_path} "
                    f"status={status_label} elapsed={elapsed_seconds:.1f}s"
                )
                results.append(
                    FileIngestResult(
                        file_id=file_id,
                        entry=entry,
                        success=success,
                        elapsed_seconds=elapsed_seconds,
                    )
                )
            return results

        with ThreadPoolExecutor(max_workers=worker_count) as executor:
            future_map: dict[
                Future[tuple[bool, float]], tuple[str, DriveManifestEntry]
            ] = {}
            for index, (file_id, entry) in enumerate(entries, start=1):
                print(
                    "[uc2-ingest] "
                    f"queued {index}/{total}: {entry.local_path} ({file_id})"
                )
                future = executor.submit(self._run_kluky_ingest_timed, entry, file_id)
                future_map[future] = (file_id, entry)

            completed = 0
            for future in as_completed(future_map):
                completed += 1
                file_id, entry = future_map[future]
                success, elapsed_seconds = future.result()
                status_label = "ok" if success else "failed"
                print(
                    "[uc2-ingest] "
                    f"finished {completed}/{total}: {entry.local_path} "
                    f"status={status_label} elapsed={elapsed_seconds:.1f}s"
                )
                results.append(
                    FileIngestResult(
                        file_id=file_id,
                        entry=entry,
                        success=success,
                        elapsed_seconds=elapsed_seconds,
                    )
                )

        return results

    def _run_kluky_ingest_timed(
        self,
        entry: DriveManifestEntry,
        file_id: str,
    ) -> tuple[bool, float]:
        started_at = time.monotonic()
        success = self._run_kluky_ingest(entry, file_id)
        elapsed_seconds = time.monotonic() - started_at
        return success, elapsed_seconds

    def _run_kluky_command(
        self,
        command: list[str],
    ) -> subprocess.CompletedProcess[str]:
        try:
            return subprocess.run(
                command,
                cwd=self.command_cwd,
                capture_output=True,
                text=True,
                check=False,
                env=self._build_kluky_env(),
                timeout=240,
            )
        except subprocess.TimeoutExpired as error:
            stdout_raw = error.stdout
            if isinstance(stdout_raw, bytes):
                stdout_text = stdout_raw.decode("utf-8", errors="replace")
            elif isinstance(stdout_raw, str):
                stdout_text = stdout_raw
            else:
                stdout_text = ""

            stderr_raw = error.stderr
            if isinstance(stderr_raw, bytes):
                stderr_text = stderr_raw.decode("utf-8", errors="replace")
            elif isinstance(stderr_raw, str):
                stderr_text = stderr_raw
            else:
                stderr_text = ""

            return subprocess.CompletedProcess(
                args=command,
                returncode=124,
                stdout=stdout_text,
                stderr=stderr_text + "\nProcess timed out after 240 seconds.",
            )

    def _build_kluky_env(self) -> dict[str, str]:
        env = dict(os.environ)
        env.pop("VIRTUAL_ENV", None)
        for key in (
            "open_ai_api_key",
            "OPENAI_API_KEY",
            "CHATGPT_API_KEY",
            "open_ai_api_base",
            "OPENAI_BASE_URL",
            "OPENAI_API_BASE",
            "PAGEINDEX_MODEL",
        ):
            env.pop(key, None)

        env["DB_HOST"] = self.settings.db_host
        env["DB_NAME"] = self.settings.db_name
        env["DB_USER"] = self.settings.db_user
        env["DB_PASSWORD"] = self.settings.db_password
        env["DB_PORT"] = str(self.settings.db_port)
        env["DB_SSLMODE"] = self.settings.db_sslmode
        env["DB_POOL_MODE"] = self.settings.db_pool_mode
        return env

    def _truncate_doc_units(self) -> None:
        with (
            open_postgres_connection(self.settings) as connection,
            connection.cursor() as cursor,
        ):
            cursor.execute("TRUNCATE TABLE public.doc_units;")

    def _delete_doc_units_for_doc_id(self, doc_id: str) -> bool:
        with (
            open_postgres_connection(self.settings) as connection,
            connection.cursor() as cursor,
        ):
            cursor.execute(
                "DELETE FROM public.doc_units WHERE doc_id = %s",
                (doc_id,),
            )
            return cursor.rowcount > 0

    def _fetch_existing_doc_ids(self, doc_ids: set[str]) -> set[str]:
        if not doc_ids:
            return set()

        with (
            open_postgres_connection(self.settings) as connection,
            connection.cursor() as cursor,
        ):
            cursor.execute(
                "SELECT DISTINCT doc_id FROM public.doc_units WHERE doc_id = ANY(%s)",
                (list(doc_ids),),
            )
            rows = cursor.fetchall()

        existing_doc_ids: set[str] = set()
        for row in rows:
            if not row:
                continue
            doc_id = str(row[0]).strip()
            if doc_id:
                existing_doc_ids.add(doc_id)

        return existing_doc_ids


def _manifest_changed(
    previous: DriveManifestEntry | None,
    current: DriveManifestEntry,
) -> bool:
    if previous is None:
        return True

    if previous.modified_time != current.modified_time:
        return True

    return previous.local_path != current.local_path


def _stable_doc_id_from_doc_key(doc_key: str) -> str:
    normalized = " ".join(doc_key.split()).strip().lower()
    return hashlib.sha1(normalized.encode("utf-8")).hexdigest()
