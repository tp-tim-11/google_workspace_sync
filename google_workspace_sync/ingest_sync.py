import hashlib
import json
import os
import re
import subprocess
from dataclasses import dataclass
from pathlib import Path

from .models import IngestSyncStats
from .postgres import open_postgres_connection
from .settings import Settings

INGEST_STATE_VERSION = 1

PRIMARY_INGEST_PY = (
    "import sys; "
    "from kluky_mcp.tools.uc02_utils.pageIndexPipeline import ingest_with_pageindex; "
    "ingest_with_pageindex(sys.argv[1], doc_key=sys.argv[2])"
)

FALLBACK_INGEST_PY = """
import sys
from pathlib import Path

from kluky_mcp.db import get_db_connection
from kluky_mcp.tools.uc02_utils.pageIndexUtils import (
    DocUnit,
    PageIndexStore,
    convert_to_markdown,
    stable_doc_id_from_doc_key,
)

input_path = sys.argv[1]
doc_key = sys.argv[2]

doc_id = stable_doc_id_from_doc_key(doc_key)
path_obj = Path(input_path)
manual_name = path_obj.name
source_type = path_obj.suffix.lower().lstrip('.') or 'unknown'

parse_error = ''
text_payload = ''
try:
    text_payload = (convert_to_markdown(input_path) or '').strip()
except Exception as error:
    parse_error = str(error)

if text_payload == '':
    text_payload = '[FALLBACK_INGEST] Unable to extract text content from source file.'

if parse_error:
    text_payload = f"{text_payload}\\n\\n[parse_error] {parse_error}".strip()

summary = text_payload
if len(summary) > 240:
    summary = summary[:239].rstrip() + '...'

unit = DocUnit(
    unit_type='chunk',
    unit_no=1,
    start_page=None,
    end_page=None,
    title=manual_name,
    heading_path=None,
    summary=summary,
    text=text_payload,
)

conn = get_db_connection()
store = PageIndexStore(conn)
try:
    store.reindex_doc(
        doc_id=doc_id,
        manual_name=manual_name,
        source_path=input_path,
        source_type=source_type,
        units=[unit],
    )
finally:
    conn.close()
""".strip()


@dataclass(frozen=True, slots=True)
class DriveManifestEntry:
    file_id: str
    local_path: str
    modified_time: str


@dataclass(frozen=True, slots=True)
class IngestedFileState:
    doc_id: str
    local_path: str
    modified_time: str


def _primary_disable_reason(stdout_text: str, stderr_text: str) -> str | None:
    haystack = f"{stdout_text}\n{stderr_text}".lower()

    if "authentication error" in haystack or "error code: 401" in haystack:
        return "authentication failed"
    if "missing api key" in haystack:
        return "missing api key"
    if "process timed out" in haystack:
        return "process timeout"

    return None


def _redact_secrets(value: str) -> str:
    redacted = re.sub(r"sk-[A-Za-z0-9_-]+", "[REDACTED_API_KEY]", value)
    redacted = re.sub(r"sk_[A-Za-z0-9_-]+", "[REDACTED_API_KEY]", redacted)
    return redacted


def _read_env_file(file_path: Path) -> dict[str, str]:
    if not file_path.exists() or not file_path.is_file():
        return {}

    loaded: dict[str, str] = {}
    for raw_line in file_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if line == "" or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[7:].strip()
        if "=" not in line:
            continue

        key, value = line.split("=", 1)
        env_key = key.strip()
        env_value = value.strip()
        if env_key == "":
            continue

        if (
            len(env_value) >= 2
            and env_value[0] == env_value[-1]
            and env_value[0] in {'"', "'"}
        ):
            env_value = env_value[1:-1]

        loaded[env_key] = env_value

    return loaded


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
        self.state_file = Path(settings.drive_ingest_state_file).expanduser()
        self.kluky_project_root = Path(settings.kluky_mcp_project_root).expanduser()
        self.command_cwd = Path.cwd().resolve()
        self.kluky_env_file = self.kluky_project_root / ".env"
        if not self.kluky_project_root.exists():
            raise ValueError(
                f"KLUKY_MCP_PROJECT_ROOT does not exist: {self.kluky_project_root}"
            )
        if not self.kluky_project_root.is_dir():
            raise ValueError(
                f"KLUKY_MCP_PROJECT_ROOT is not a directory: {self.kluky_project_root}"
            )
        self._primary_ingest_disabled_reason: str | None = None

    def sync(
        self,
        *,
        sync_mode: str,
        previous_manifest: dict[str, DriveManifestEntry],
        current_manifest: dict[str, DriveManifestEntry],
    ) -> IngestSyncStats:
        ingest_state = self._load_ingest_state_safely()

        if sync_mode == "full_reconcile":
            return self._sync_full_reconcile(current_manifest)

        return self._sync_incremental(
            previous_manifest=previous_manifest,
            current_manifest=current_manifest,
            ingest_state=ingest_state,
        )

    def _sync_full_reconcile(
        self,
        current_manifest: dict[str, DriveManifestEntry],
    ) -> IngestSyncStats:
        stats = IngestSyncStats()
        self._truncate_doc_units()

        next_state: dict[str, IngestedFileState] = {}
        for file_id, entry in sorted(current_manifest.items()):
            if self._run_kluky_ingest(entry, file_id):
                stats.ingested_files += 1
                doc_id = _stable_doc_id_from_doc_key(file_id)
                next_state[file_id] = IngestedFileState(
                    doc_id=doc_id,
                    local_path=entry.local_path,
                    modified_time=entry.modified_time,
                )
            else:
                stats.failed_files += 1

        self._save_ingest_state(next_state)
        return stats

    def _sync_incremental(
        self,
        *,
        previous_manifest: dict[str, DriveManifestEntry],
        current_manifest: dict[str, DriveManifestEntry],
        ingest_state: dict[str, IngestedFileState],
    ) -> IngestSyncStats:
        stats = IngestSyncStats()

        removed_file_ids = sorted(set(ingest_state) - set(current_manifest))
        for file_id in removed_file_ids:
            old_state = ingest_state.pop(file_id)
            doc_id = old_state.doc_id or _stable_doc_id_from_doc_key(file_id)
            self._delete_doc_units_for_doc_id(doc_id)
            stats.deleted_docs += 1

        changed_file_ids = {
            file_id
            for file_id, current_entry in current_manifest.items()
            if _manifest_changed(previous_manifest.get(file_id), current_entry)
        }

        retry_file_ids = {
            file_id
            for file_id, current_entry in current_manifest.items()
            if _ingest_state_stale(ingest_state.get(file_id), current_entry)
        }

        ingest_file_ids = sorted(changed_file_ids | retry_file_ids)
        for file_id in ingest_file_ids:
            current_entry = current_manifest[file_id]
            if self._run_kluky_ingest(current_entry, file_id):
                stats.ingested_files += 1
                doc_id = _stable_doc_id_from_doc_key(file_id)
                ingest_state[file_id] = IngestedFileState(
                    doc_id=doc_id,
                    local_path=current_entry.local_path,
                    modified_time=current_entry.modified_time,
                )
            else:
                stats.failed_files += 1

        self._save_ingest_state(ingest_state)
        return stats

    def _run_kluky_ingest(self, entry: DriveManifestEntry, file_id: str) -> bool:
        input_path = (self.download_root / entry.local_path).resolve()
        if not input_path.exists() or not input_path.is_file():
            print(
                f"[uc2-ingest] Skipping missing local file for {file_id}: {input_path}"
            )
            return False

        if (
            not self._is_text_like_file(input_path)
            and self._primary_ingest_disabled_reason is None
        ):
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

            disable_reason = _primary_disable_reason(stdout_text, stderr_text)
            if disable_reason is not None:
                self._primary_ingest_disabled_reason = disable_reason
                print(
                    "[uc2-ingest] Disabling PageIndex primary ingest "
                    f"for this run: {disable_reason}."
                )

        fallback_command = [
            "uv",
            "run",
            "--project",
            str(self.kluky_project_root),
            "python",
            "-c",
            FALLBACK_INGEST_PY,
            str(input_path),
            file_id,
        ]
        fallback = self._run_kluky_command(fallback_command)
        if fallback.returncode == 0:
            print(
                "[uc2-ingest] Fallback ingest succeeded for "
                f"{entry.local_path} ({file_id})."
            )
            return True

        fallback_stdout = fallback.stdout.strip()
        fallback_stderr = fallback.stderr.strip()
        redacted_fallback_stdout = _redact_secrets(fallback_stdout)
        redacted_fallback_stderr = _redact_secrets(fallback_stderr)
        print(
            "[uc2-ingest] Fallback ingest failed for "
            f"{entry.local_path} ({file_id}). "
            f"exit={fallback.returncode}"
        )
        if redacted_fallback_stdout:
            print(f"[uc2-ingest] fallback stdout: {redacted_fallback_stdout}")
        if redacted_fallback_stderr:
            print(f"[uc2-ingest] fallback stderr: {redacted_fallback_stderr}")
        return False

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

    def _is_text_like_file(self, input_path: Path) -> bool:
        text_like_extensions = {
            ".txt",
            ".md",
            ".markdown",
            ".csv",
            ".tsv",
            ".json",
            ".xml",
            ".html",
            ".htm",
            ".log",
        }
        return input_path.suffix.lower() in text_like_extensions

    def _build_kluky_env(self) -> dict[str, str]:
        env = dict(os.environ)
        env.pop("VIRTUAL_ENV", None)

        for key, value in _read_env_file(self.kluky_env_file).items():
            env.setdefault(key, value)

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

    def _delete_doc_units_for_doc_id(self, doc_id: str) -> None:
        with (
            open_postgres_connection(self.settings) as connection,
            connection.cursor() as cursor,
        ):
            cursor.execute(
                "DELETE FROM public.doc_units WHERE doc_id = %s",
                (doc_id,),
            )

    def _load_ingest_state_safely(self) -> dict[str, IngestedFileState]:
        try:
            return self._load_ingest_state()
        except Exception as error:
            print(
                "[uc2-ingest] Failed to read state file "
                f"{self.state_file}: {error}. Starting with empty ingest state."
            )
            return {}

    def _load_ingest_state(self) -> dict[str, IngestedFileState]:
        if not self.state_file.exists():
            return {}

        raw_text = self.state_file.read_text(encoding="utf-8")
        payload = json.loads(raw_text)
        if not isinstance(payload, dict):
            raise ValueError("state file root must be a JSON object")

        version = payload.get("version")
        if not isinstance(version, int) or version != INGEST_STATE_VERSION:
            raise ValueError(
                f"ingest state version mismatch: expected {INGEST_STATE_VERSION}, got {version}"
            )

        files_raw = payload.get("files", {})
        if not isinstance(files_raw, dict):
            raise ValueError("state.files must be an object")

        state: dict[str, IngestedFileState] = {}
        for file_id_raw, entry_raw in files_raw.items():
            file_id = str(file_id_raw).strip()
            if file_id == "" or not isinstance(entry_raw, dict):
                continue

            doc_id = str(entry_raw.get("doc_id", "")).strip()
            local_path = str(entry_raw.get("local_path", "")).strip()
            modified_time = str(entry_raw.get("modified_time", "")).strip()
            if doc_id == "" or local_path == "" or modified_time == "":
                continue

            state[file_id] = IngestedFileState(
                doc_id=doc_id,
                local_path=local_path,
                modified_time=modified_time,
            )

        return state

    def _save_ingest_state(self, state: dict[str, IngestedFileState]) -> None:
        self.state_file.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "version": INGEST_STATE_VERSION,
            "files": {
                file_id: {
                    "doc_id": entry.doc_id,
                    "local_path": entry.local_path,
                    "modified_time": entry.modified_time,
                }
                for file_id, entry in state.items()
            },
        }

        temp_path = self.state_file.with_suffix(f"{self.state_file.suffix}.tmp")
        temp_path.write_text(
            json.dumps(payload, ensure_ascii=True, indent=2, sort_keys=True),
            encoding="utf-8",
        )
        temp_path.replace(self.state_file)


def _manifest_changed(
    previous: DriveManifestEntry | None,
    current: DriveManifestEntry,
) -> bool:
    if previous is None:
        return True

    if previous.modified_time != current.modified_time:
        return True

    return previous.local_path != current.local_path


def _ingest_state_stale(
    ingest_state: IngestedFileState | None,
    current: DriveManifestEntry,
) -> bool:
    if ingest_state is None:
        return True

    if ingest_state.modified_time != current.modified_time:
        return True

    return ingest_state.local_path != current.local_path


def _stable_doc_id_from_doc_key(doc_key: str) -> str:
    normalized = " ".join(doc_key.split()).strip().lower()
    return hashlib.sha1(normalized.encode("utf-8")).hexdigest()
