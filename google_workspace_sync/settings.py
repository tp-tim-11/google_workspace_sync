"""Application settings."""

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    google_service_account_credentials_file: str = (
        "../credentials/google_service_account_api_key.json"
    )
    google_drive_documents_folder_id: str = ""
    google_sheets_id: str = ""
    google_sheets_range: str = ""
    drive_download_root: str = "./drive_mirror"
    drive_state_file: str = "./drive_sync_state.json"
    drive_recursive: bool = True
    drive_hard_delete: bool = True
    drive_ingest_enabled: bool = True
    drive_ingest_state_file: str = "./uc2_ingest_state.json"
    drive_ingest_workers: int = 2
    kluky_mcp_project_root: str = "../kluky_mcp"

    db_host: str = "aws-1-eu-west-1.pooler.supabase.com"
    db_port: int = 5432
    db_name: str = "postgres"
    db_user: str = "postgres.szejlmlpxxinjwgcpqqp"
    db_password: str = ""
    db_sslmode: str = "prefer"
    db_pool_mode: str = "session"
    sheet_push_poll_timeout_seconds: float = 30.0

    model_config = SettingsConfigDict(env_file=".env", env_prefix="", extra="ignore")


settings = Settings()
