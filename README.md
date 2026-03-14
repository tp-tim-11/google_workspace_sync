# google_workspace_sync

Cron-friendly Google Workspace + PostgreSQL sync tool.

Current capabilities:
- Drive -> local mirror (with Drive Changes API token cursor)
- Drive mirror -> UC2 ingest (`kluky_mcp` / `doc_units`)
- Sheet -> DB (`resources` table)
- DB (`resources`) -> Sheet (one-shot or listener)

This implementation is pull-oriented and does not use Drive webhooks.

## Current behavior

Drive sync:
- Uses `changes.list` with persisted state in `DRIVE_STATE_FILE`.
- Falls back to full reconcile when token/state is missing or invalid.
- Mirrors nested folders under `DRIVE_DOWNLOAD_ROOT`.
- Exports Google-native files (Docs/Sheets/Slides) as PDF into the mirror.

UC2 ingest:
- Runs after successful Drive sync when `DRIVE_INGEST_ENABLED=true`.
- Uses only primary `kluky_mcp` PageIndex ingest (no secondary fallback ingest path).
- On full reconcile, truncates `public.doc_units` then re-ingests all mirrored files.
- On incremental sync, ingests changed/new files and deletes units for removed files.
- Ingest truth source is DB (`doc_units`), not ingest JSON state.
- Ingest subprocess runs with `cwd=KLUKY_MCP_PROJECT_ROOT`.

Sheet -> DB sync (`sync --mode sheet`):
- Strict headers required: `NAME, ESP, PIN, LED, STATUS, BORROWED BY`.
- Ignores unknown extra columns.
- Canonicalizes `STATUS` to enum values (`AVAILABLE`, `BORROWED`, `BROKEN`, `LOST`).
- Normalizes `BORROWED BY=NIKTO` to `NULL`.
- Soft-deletes missing tools (`deleted=true`).

DB -> Sheet push:
- One-shot push: `sync --mode sheet-push`.
- Continuous listener: `watch-sheet-push` (LISTEN/NOTIFY).
- Listener ensures the `resources` notify trigger/function exists.

## Tooling

Use `uv` for everything:

```bash
uv sync
```

## Environment variables

Settings are loaded via `pydantic-settings` from `.env`.

Required for all Google operations:
- `GOOGLE_SERVICE_ACCOUNT_CREDENTIALS_FILE`

Required for Drive sync:
- `GOOGLE_DRIVE_DOCUMENTS_FOLDER_ID`

Optional Drive settings:
- `DRIVE_DOWNLOAD_ROOT` (default: `./drive_mirror`)
- `DRIVE_STATE_FILE` (default: `./drive_sync_state.json`)
- `DRIVE_RECURSIVE` (default: `true`)
- `DRIVE_HARD_DELETE` (default: `true`)
- `DRIVE_INGEST_ENABLED` (default: `true`)
- `DRIVE_INGEST_WORKERS` (default: `2`)
- `KLUKY_MCP_PROJECT_ROOT` (default: `../kluky_mcp`)

Required when Drive ingest is enabled:
- `DB_HOST`, `DB_NAME`, `DB_USER`, `DB_PASSWORD`

`kluky_mcp` credentials (including OpenAI values) are read from
`KLUKY_MCP_PROJECT_ROOT/.env`.

Required for sheet sync/push/watch:
- `GOOGLE_SHEETS_ID`
- `DB_HOST`, `DB_NAME`, `DB_USER`, `DB_PASSWORD`

Optional sheet settings:
- `GOOGLE_SHEETS_RANGE` (default: `Sheet1!A:F`)
- `SHEET_PUSH_POLL_TIMEOUT_SECONDS` (default: `30`)
- `DB_PORT` (default: `5432`)
- `DB_SSLMODE` (default: `prefer`)

Note: `DRIVE_INGEST_STATE_FILE` remains in settings/env template for compatibility,
but ingest state tracking is now DB-driven.

## Commands

Drive sync + ingest:

```bash
uv run google-workspace-sync sync --mode drive
```

Force full reconcile:

```bash
uv run google-workspace-sync sync --mode drive --full-reconcile
```

Sheet -> DB:

```bash
uv run google-workspace-sync sync --mode sheet
```

DB -> Sheet (one-shot):

```bash
uv run google-workspace-sync sync --mode sheet-push
```

DB -> Sheet (continuous listener):

```bash
uv run google-workspace-sync watch-sheet-push
```

Run drive + sheet pull in one command (`all` does not start the listener):

```bash
uv run google-workspace-sync sync --mode all
```

Reset/recreate DB schema (destructive):

```bash
uv run google-workspace-sync init-db
```

Alias:

```bash
uv run google-workspace-sync init
```

## Schema notes

`init-db` creates all sync-related tables, including:
- `resources` (sheet mirror target)
- `doc_units` (UC2 ingest units)
- `drive_documents` (drive file mapping + sync token/ingest status metadata)

Schema SQL files are under `google_workspace_sync/sql/init_db/`.

## Cron

Every 5 minutes:

```cron
*/5 * * * * cd /home/adamveres/Projects/team-project/google_workspace_sync && uv run google-workspace-sync sync --mode drive >> /var/log/google_workspace_sync.log 2>&1
```

If you need DB -> Sheet near-real-time updates, run `watch-sheet-push` as a
separate long-running process (systemd/tmux/supervisor/etc.).

Managed cron helper scripts:

```bash
./scripts/add_google_sync_cron.sh
./scripts/remove_google_sync_cron.sh
```

Install for another user (via sudo):

```bash
./scripts/add_google_sync_cron.sh --user your-user
./scripts/remove_google_sync_cron.sh --user your-user
```
