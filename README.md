# google_workspace_sync

Cron-friendly pull sync for Google Workspace:
- Mirrors files from a specific Google Drive documents folder ID.
- Optionally syncs a Google Sheet into PostgreSQL `resources` table.

This implementation is pull-based. It does not rely on Drive webhooks.
It compares Drive `modifiedTime` with local file mtime and downloads only changed files.

Sheet sync behavior:
- Parses rows by header names (not fixed column positions).
- Requires a `nazov`-equivalent header (for example `Názov nástroja`).
- Soft-deletes missing tools by setting `deleted=true`.
- Fails safely if the sheet fetch is empty or the header row is missing.

Drive sync behavior:
- Recursively scans the configured documents folder ID.
- Flattens files from nested Drive subfolders directly into `DRIVE_DOWNLOAD_ROOT`.

## Tooling

Use `uv` for everything:

```bash
uv sync
```

## Environment variables

Settings are loaded via `pydantic-settings` from `.env`.

Required for Drive sync:
- `GOOGLE_SERVICE_ACCOUNT_CREDENTIALS_FILE` (path to service account JSON)
- `GOOGLE_DRIVE_DOCUMENTS_FOLDER_ID` (Drive folder ID to mirror)

Optional Drive settings:
- `DRIVE_DOWNLOAD_ROOT` (default: `./drive_mirror`)
- `DRIVE_RECURSIVE` (default: `true`)
- `DRIVE_HARD_DELETE` (default: `true`)

Required for Sheet sync:
- `GOOGLE_SHEETS_ID`
- `DB_HOST`
- `DB_NAME`
- `DB_USER`
- `DB_PASSWORD`

Optional Sheet settings:
- `GOOGLE_SHEETS_RANGE` (default: `Sheet1!A:E`)
- `DB_PORT` (default: `5432`)
- `DB_SSLMODE` (default: `prefer`)

## Commands

Run Drive pull sync:

```bash
uv run google-workspace-sync sync --mode drive
```

Run Sheet sync:

```bash
uv run google-workspace-sync sync --mode sheet
```

Run both:

```bash
uv run google-workspace-sync sync --mode all
```

Initialize DB schema for Sheet sync:

```bash
uv run google-workspace-sync init-db
```

## Cron example

Every 5 minutes, mirror Drive folder with hard delete:

```cron
*/5 * * * * cd /home/adamveres/Projects/team-project/google_workspace_sync && uv run google-workspace-sync sync --mode drive >> /var/log/google_workspace_sync.log 2>&1
```

If you need to keep local leftovers for one run, pass `--no-hard-delete`.
