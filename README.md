# google_workspace_sync

Cron-friendly pull sync for Google Workspace:
- Mirrors files from a specific Google Drive documents folder ID.
- Optionally syncs a Google Sheet into PostgreSQL `resources` table.

This implementation is pull-based. It does not rely on Drive webhooks.
Drive sync uses the Drive Changes API with a persisted token/state file.

Sheet sync behavior:
- Uses strict canonical headers.
- Requires all headers: `NAME`, `ESP`, `PIN`, `LED`, `STATUS`, `BORROWED BY`.
- Ignores extra/unrecognized columns.
- Column values may be empty; only header presence is required.
- Soft-deletes missing tools by setting `deleted=true`.
- Fails safely if the sheet fetch is empty or the header row is missing.

Recommended sheet header template:
- `NAME, ESP, PIN, LED, STATUS, BORROWED BY`

Drive sync behavior:
- Uses `changes.list` page tokens persisted in `DRIVE_STATE_FILE`.
- Falls back to full reconcile when token is missing/invalid.
- Recursively scans the configured documents folder ID for initial/full reconcile.
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
- `DRIVE_STATE_FILE` (default: `./drive_sync_state.json`)
- `DRIVE_RECURSIVE` (default: `true`)
- `DRIVE_HARD_DELETE` (default: `true`)

Required for Sheet sync:
- `GOOGLE_SHEETS_ID`
- `DB_HOST`
- `DB_NAME`
- `DB_USER`
- `DB_PASSWORD`

Optional Sheet settings:
 - `GOOGLE_SHEETS_RANGE` (default: `Sheet1!A:F`)
- `DB_PORT` (default: `5432`)
- `DB_SSLMODE` (default: `prefer`)

## Commands

Run Drive pull sync:

```bash
uv run google-workspace-sync sync --mode drive
```

Force full Drive reconcile (ignore token state for one run):

```bash
uv run google-workspace-sync sync --mode drive --full-reconcile
```

Run Sheet sync:

```bash
uv run google-workspace-sync sync --mode sheet
```

Run both:

```bash
uv run google-workspace-sync sync --mode all
```

Reset and recreate DB schema for Sheet sync (destructive):

```bash
uv run google-workspace-sync init-db
```

Alias:

```bash
uv run google-workspace-sync init
```

`init-db`/`init` is the only command that executes schema reset SQL.
Normal sync commands never run the full schema reset.

The schema SQL is split into editable files under
`google_workspace_sync/sql/init_db/`.

## Cron example

Every 5 minutes, mirror Drive folder with hard delete:

```cron
*/5 * * * * cd /home/adamveres/Projects/team-project/google_workspace_sync && uv run google-workspace-sync sync --mode drive >> /var/log/google_workspace_sync.log 2>&1
```

If you need to keep local leftovers for one run, pass `--no-hard-delete`.
