import os
from dataclasses import dataclass
from pathlib import Path

_TRUE_VALUES = {"1", "true", "yes", "on"}
_FALSE_VALUES = {"0", "false", "no", "off"}

ENV_GOOGLE_SERVICE_ACCOUNT_CREDENTIALS_FILE = (
    "GOOGLE_SERVICE_ACCOUNT_CREDENTIALS_FILE",
    "google_service_account_credentials_file",
)
ENV_GOOGLE_DRIVE_DOCUMENTS_FOLDER_ID = (
    "GOOGLE_DRIVE_DOCUMENTS_FOLDER_ID",
    "google_drive_documents_folder_id",
)
ENV_GOOGLE_SHEETS_ID = ("GOOGLE_SHEETS_ID", "google_sheets_id")
ENV_GOOGLE_SHEETS_RANGE = ("GOOGLE_SHEETS_RANGE", "google_sheets_range")

ENV_DRIVE_DOWNLOAD_ROOT = ("DRIVE_DOWNLOAD_ROOT", "drive_download_root")
ENV_DRIVE_RECURSIVE = ("DRIVE_RECURSIVE", "drive_recursive")
ENV_DRIVE_HARD_DELETE = ("DRIVE_HARD_DELETE", "drive_hard_delete")

ENV_POSTGRES_HOST = ("POSTGRES_HOST", "postgres_host")
ENV_POSTGRES_DB = ("POSTGRES_DB", "postgres_db")
ENV_POSTGRES_USER = ("POSTGRES_USER", "postgres_user")
ENV_POSTGRES_PASSWORD = ("POSTGRES_PASSWORD", "postgres_password")
ENV_POSTGRES_PORT = ("POSTGRES_PORT", "postgres_port")

ENV_CUSTOM_ENV_FILE = (
    "GOOGLE_WORKSPACE_SYNC_ENV_FILE",
    "google_workspace_sync_env_file",
)


@dataclass(frozen=True, slots=True)
class PostgresSettings:
    host: str
    database: str
    user: str
    password: str
    port: int = 5432


@dataclass(frozen=True, slots=True)
class Settings:
    google_service_account_credentials_file: Path
    google_drive_documents_folder_id: str
    google_sheets_id: str | None
    google_sheets_range: str
    drive_download_root: Path
    drive_recursive: bool
    drive_hard_delete: bool
    postgres: PostgresSettings | None


def _normalize_env_value(raw_value: str | None) -> str | None:
    if raw_value is None:
        return None
    normalized = raw_value.strip()
    if normalized == "":
        return None
    return normalized


def _env_optional(*names: str) -> str | None:
    for name in names:
        value = _normalize_env_value(os.getenv(name))
        if value is not None:
            return value
    return None


def _env_required(*names: str) -> str:
    value = _env_optional(*names)
    if value is not None:
        return value

    names_joined = ", ".join(names)
    raise ValueError(
        f"Missing required environment variable. Set one of: {names_joined}"
    )


def _env_int(default: int, *names: str) -> int:
    raw_value = _env_optional(*names)
    if raw_value is None:
        return default
    return int(raw_value)


def _env_bool(default: bool, *names: str) -> bool:
    raw_value = _env_optional(*names)
    if raw_value is None:
        return default

    normalized = raw_value.lower()
    if normalized in _TRUE_VALUES:
        return True
    if normalized in _FALSE_VALUES:
        return False

    raise ValueError(
        "Invalid boolean value "
        f"for {names[0]}: {raw_value}. "
        "Use one of: 1/0, true/false, yes/no, on/off."
    )


def _parse_dotenv_line(raw_line: str) -> tuple[str, str] | None:
    line = raw_line.strip()
    if line == "" or line.startswith("#"):
        return None

    key, separator, value = line.partition("=")
    if separator == "":
        return None

    normalized_key = key.strip()
    normalized_value = value.strip()
    if (
        len(normalized_value) >= 2
        and normalized_value[0] == normalized_value[-1]
        and normalized_value[0] in {'"', "'"}
    ):
        normalized_value = normalized_value[1:-1]

    if normalized_key == "":
        return None
    return normalized_key, normalized_value


def _load_dotenv_file(path: Path) -> None:
    if not path.exists() or not path.is_file():
        return

    for line in path.read_text(encoding="utf-8").splitlines():
        parsed = _parse_dotenv_line(line)
        if parsed is None:
            continue

        key, value = parsed
        os.environ.setdefault(key, value)


def _load_dotenv() -> None:
    custom_env_file = _env_optional(*ENV_CUSTOM_ENV_FILE)
    if custom_env_file is not None:
        _load_dotenv_file(Path(custom_env_file).expanduser())
        return

    _load_dotenv_file(Path(".env"))
    _load_dotenv_file(Path(".env.local"))


def _load_postgres_settings() -> PostgresSettings | None:
    values = {
        "POSTGRES_HOST": _env_optional(*ENV_POSTGRES_HOST),
        "POSTGRES_DB": _env_optional(*ENV_POSTGRES_DB),
        "POSTGRES_USER": _env_optional(*ENV_POSTGRES_USER),
        "POSTGRES_PASSWORD": _env_optional(*ENV_POSTGRES_PASSWORD),
    }
    provided_values = {
        name: value for name, value in values.items() if value is not None
    }

    if not provided_values:
        return None

    missing_names = [name for name, value in values.items() if value is None]
    if missing_names:
        missing_joined = ", ".join(sorted(missing_names))
        raise ValueError(
            f"Missing required PostgreSQL environment variables: {missing_joined}."
        )

    return PostgresSettings(
        host=values["POSTGRES_HOST"] or "",
        database=values["POSTGRES_DB"] or "",
        user=values["POSTGRES_USER"] or "",
        password=values["POSTGRES_PASSWORD"] or "",
        port=_env_int(5432, *ENV_POSTGRES_PORT),
    )


def require_sheet_id(settings: Settings) -> str:
    if settings.google_sheets_id is None:
        raise ValueError(
            "GOOGLE_SHEETS_ID must be set for sheet sync "
            "(lowercase alias google_sheets_id is also supported)."
        )
    return settings.google_sheets_id


def require_postgres(settings: Settings) -> PostgresSettings:
    if settings.postgres is None:
        raise ValueError(
            "PostgreSQL settings missing. Set POSTGRES_HOST, POSTGRES_DB, "
            "POSTGRES_USER, and POSTGRES_PASSWORD "
            "(or lowercase postgres_* aliases)."
        )
    return settings.postgres


def load_settings() -> Settings:
    _load_dotenv()

    sheet_range = _env_optional(*ENV_GOOGLE_SHEETS_RANGE) or "Sheet1!A:E"
    credentials_path = Path(
        _env_required(*ENV_GOOGLE_SERVICE_ACCOUNT_CREDENTIALS_FILE)
    ).expanduser()
    if not credentials_path.exists():
        raise ValueError(
            f"Service account credentials file does not exist: {credentials_path}"
        )

    return Settings(
        google_service_account_credentials_file=credentials_path,
        google_drive_documents_folder_id=_env_required(
            *ENV_GOOGLE_DRIVE_DOCUMENTS_FOLDER_ID
        ),
        google_sheets_id=_env_optional(*ENV_GOOGLE_SHEETS_ID),
        google_sheets_range=sheet_range,
        drive_download_root=Path(
            _env_optional(*ENV_DRIVE_DOWNLOAD_ROOT) or "./drive_mirror"
        ).expanduser(),
        drive_recursive=_env_bool(True, *ENV_DRIVE_RECURSIVE),
        drive_hard_delete=_env_bool(True, *ENV_DRIVE_HARD_DELETE),
        postgres=_load_postgres_settings(),
    )
