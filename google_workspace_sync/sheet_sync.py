from pathlib import Path
from typing import LiteralString, cast

from .google_api_protocols import SheetsService
from .models import ResourceRow, SheetSyncStats
from .postgres import open_postgres_connection
from .settings import Settings

INIT_DB_SQL_FILE_NAMES: tuple[str, ...] = (
    "001_drop_objects.sql",
    "010_function.sql",
    "030_tables.sql",
    "040_indexes.sql",
    "050_trigger.sql",
    "060_constraints.sql",
)

UPSERT_RESOURCES_SQL = """
INSERT INTO public.resources (
    nazov,
    esp,
    pin,
    led,
    status,
    vypozicane_komu,
    deleted
)
VALUES (%s, %s, %s, %s, %s, %s, false)
ON CONFLICT (nazov)
DO UPDATE SET
    esp = EXCLUDED.esp,
    pin = EXCLUDED.pin,
    led = EXCLUDED.led,
    status = EXCLUDED.status,
    vypozicane_komu = EXCLUDED.vypozicane_komu,
    deleted = false,
    updated_at = now();
"""

MARK_MISSING_RESOURCES_DELETED_SQL = """
UPDATE public.resources
SET deleted = true,
    updated_at = now()
WHERE deleted = false
  AND NOT (nazov = ANY(%s));
"""

MARK_ALL_RESOURCES_DELETED_SQL = """
UPDATE public.resources
SET deleted = true,
    updated_at = now()
WHERE deleted = false;
"""

READ_RESOURCES_COLUMNS_SQL = """
SELECT column_name, data_type, is_nullable
FROM information_schema.columns
WHERE table_schema = 'public' AND table_name = 'resources'
ORDER BY ordinal_position;
"""

READ_RESOURCES_INDEXES_SQL = """
SELECT indexname
FROM pg_indexes
WHERE schemaname = 'public' AND tablename = 'resources';
"""

EXPECTED_RESOURCE_COLUMNS: tuple[tuple[str, str, str], ...] = (
    ("id", "integer", "NO"),
    ("nazov", "text", "NO"),
    ("esp", "text", "YES"),
    ("pin", "text", "YES"),
    ("led", "text", "YES"),
    ("status", "text", "YES"),
    ("vypozicane_komu", "text", "YES"),
    ("created_at", "timestamp with time zone", "NO"),
    ("updated_at", "timestamp with time zone", "NO"),
    ("deleted", "boolean", "NO"),
)

REQUIRED_RESOURCE_INDEXES = frozenset(
    {
        "resources_pkey",
        "resources_nazov_unique",
        "resources_not_deleted_idx",
    }
)

CANONICAL_SHEET_HEADERS: dict[str, str] = {
    "NAME": "nazov",
    "ESP": "esp",
    "PIN": "pin",
    "LED": "led",
    "STATUS": "status",
    "BORROWED BY": "vypozicane_komu",
}

REQUIRED_HEADER_FIELDS = frozenset(CANONICAL_SHEET_HEADERS.values())


def _normalize_header_name(value: str) -> str:
    normalized = value.strip().replace("_", " ").upper()
    sanitized = "".join(
        character if character.isalnum() or character.isspace() else " "
        for character in normalized
    )
    return " ".join(sanitized.split())


def _build_expected_header_lookup() -> dict[str, str]:
    return {
        _normalize_header_name(header_name): field_name
        for header_name, field_name in CANONICAL_SHEET_HEADERS.items()
    }


EXPECTED_HEADER_LOOKUP = _build_expected_header_lookup()


def ensure_resources_schema(settings: Settings) -> None:
    with (
        open_postgres_connection(settings) as connection,
        connection.cursor() as cursor,
    ):
        for sql_path in _iter_init_sql_paths():
            sql_text = sql_path.read_text(encoding="utf-8").strip()
            if sql_text == "":
                continue
            cursor.execute(cast(LiteralString, sql_text))


def _iter_init_sql_paths() -> tuple[Path, ...]:
    sql_directory = Path(__file__).resolve().parent / "sql" / "init_db"
    sql_paths = tuple(sql_directory / name for name in INIT_DB_SQL_FILE_NAMES)

    missing_paths = [path for path in sql_paths if not path.exists()]
    if missing_paths:
        missing_list = ", ".join(str(path) for path in missing_paths)
        raise FileNotFoundError(f"Missing init-db SQL files: {missing_list}")

    return sql_paths


def validate_resources_schema(settings: Settings) -> None:
    with (
        open_postgres_connection(settings) as connection,
        connection.cursor() as cursor,
    ):
        cursor.execute(READ_RESOURCES_COLUMNS_SQL)
        raw_column_rows = cursor.fetchall()
        column_rows = cast(list[tuple[str, str, str]], raw_column_rows)

        if not column_rows:
            raise ValueError(
                "Schema mismatch: public.resources table is missing. "
                "Run `uv run google-workspace-sync init-db` first."
            )

        found_columns = {
            name: (data_type, is_nullable)
            for name, data_type, is_nullable in column_rows
        }
        _validate_resource_columns(found_columns)

        cursor.execute(READ_RESOURCES_INDEXES_SQL)
        raw_index_rows = cursor.fetchall()
        index_rows = cast(list[tuple[str]], raw_index_rows)
        found_indexes = {name for (name,) in index_rows}
        missing_indexes = sorted(REQUIRED_RESOURCE_INDEXES - found_indexes)
        if missing_indexes:
            raise ValueError(
                "Schema mismatch in public.resources indexes. Missing: "
                f"{', '.join(missing_indexes)}"
            )


def _validate_resource_columns(
    found_columns: dict[str, tuple[str, str]],
) -> None:
    missing_columns: list[str] = []
    mismatched_columns: list[str] = []

    for column_name, expected_type, expected_nullable in EXPECTED_RESOURCE_COLUMNS:
        column_meta = found_columns.get(column_name)
        if column_meta is None:
            missing_columns.append(column_name)
            continue

        found_type, found_nullable = column_meta
        if found_type != expected_type or found_nullable != expected_nullable:
            mismatched_columns.append(
                f"{column_name} expected ({expected_type}, {expected_nullable}) "
                f"got ({found_type}, {found_nullable})"
            )

    if not missing_columns and not mismatched_columns:
        return

    message_parts: list[str] = ["Schema mismatch in public.resources columns."]
    if missing_columns:
        message_parts.append(f"Missing: {', '.join(sorted(missing_columns))}.")
    if mismatched_columns:
        message_parts.append("Mismatched: " + "; ".join(mismatched_columns) + ".")

    raise ValueError(" ".join(message_parts))


class SheetSyncService:
    def __init__(
        self,
        sheets_client: SheetsService,
        settings: Settings,
        spreadsheet_id: str,
        spreadsheet_range: str,
    ) -> None:
        self.sheets_client = sheets_client
        self.settings = settings
        self.spreadsheet_id = spreadsheet_id
        self.spreadsheet_range = spreadsheet_range

    def sync(self) -> SheetSyncStats:
        values = self._fetch_sheet_values()
        header_row = self._extract_header(values)
        header_index_map = self._build_header_index_map(header_row)
        resources, skipped_rows = self._parse_resource_rows(values, header_index_map)

        validate_resources_schema(self.settings)
        upserted_rows, soft_deleted_rows = self._persist_rows(resources)

        fetched_rows = len(values) - 1
        return SheetSyncStats(
            fetched_rows=fetched_rows,
            upserted_rows=upserted_rows,
            soft_deleted_rows=soft_deleted_rows,
            skipped_rows=skipped_rows,
        )

    def _fetch_sheet_values(self) -> list[list[str]]:
        response = (
            self.sheets_client.spreadsheets()
            .values()
            .get(
                spreadsheetId=self.spreadsheet_id,
                range=self.spreadsheet_range,
            )
            .execute()
        )

        values_raw = response.get("values", [])
        if not isinstance(values_raw, list):
            return []

        values: list[list[str]] = []
        for row in values_raw:
            if not isinstance(row, list):
                continue

            values.append([str(cell).strip() for cell in row])

        return values

    @staticmethod
    def _extract_header(values: list[list[str]]) -> list[str]:
        if not values:
            raise ValueError(
                "Sheet sync failed: spreadsheet returned no rows. "
                "Refusing to modify database."
            )

        header_row = [cell.strip() for cell in values[0]]
        if not header_row or all(cell == "" for cell in header_row):
            raise ValueError(
                "Sheet sync failed: spreadsheet has no header row. "
                "Refusing to modify database."
            )

        return header_row

    @staticmethod
    def _build_header_index_map(header_row: list[str]) -> dict[str, int]:
        header_index_map: dict[str, int] = {}
        for index, raw_name in enumerate(header_row):
            normalized = _normalize_header_name(raw_name)
            if normalized == "":
                continue

            field_name = EXPECTED_HEADER_LOOKUP.get(normalized)
            if field_name is None:
                continue

            header_index_map.setdefault(field_name, index)

        missing_required_fields = sorted(REQUIRED_HEADER_FIELDS - set(header_index_map))
        if missing_required_fields:
            missing_headers = [
                header_name
                for header_name, field_name in CANONICAL_SHEET_HEADERS.items()
                if field_name in missing_required_fields
            ]
            raise ValueError(
                "Sheet sync failed: missing required headers: "
                f"{', '.join(missing_headers)}. "
                "Refusing to modify database."
            )

        return header_index_map

    def _parse_resource_rows(
        self,
        values: list[list[str]],
        header_index_map: dict[str, int],
    ) -> tuple[list[ResourceRow], int]:
        if len(values) <= 1:
            return [], 0

        resources_by_name: dict[str, ResourceRow] = {}
        skipped_rows = 0

        for row in values[1:]:
            if not row or all(cell.strip() == "" for cell in row):
                skipped_rows += 1
                continue

            nazov = self._cell_by_header(row, header_index_map, "nazov")
            esp = self._cell_by_header(row, header_index_map, "esp")
            pin = self._cell_by_header(row, header_index_map, "pin")
            led = self._cell_by_header(row, header_index_map, "led")
            status = self._cell_by_header(row, header_index_map, "status")
            vypozicane_komu = self._cell_by_header(
                row,
                header_index_map,
                "vypozicane_komu",
            )

            if nazov == "":
                skipped_rows += 1
                continue

            resources_by_name[nazov] = ResourceRow(
                nazov=nazov,
                esp=esp or None,
                pin=pin or None,
                led=led or None,
                status=status or None,
                vypozicane_komu=vypozicane_komu or None,
            )

        return list(resources_by_name.values()), skipped_rows

    def _persist_rows(self, rows: list[ResourceRow]) -> tuple[int, int]:
        upsert_params = [
            (
                row.nazov,
                row.esp,
                row.pin,
                row.led,
                row.status,
                row.vypozicane_komu,
            )
            for row in rows
        ]
        seen_names = [row.nazov for row in rows]

        with (
            open_postgres_connection(self.settings) as connection,
            connection.cursor() as cursor,
        ):
            if upsert_params:
                cursor.executemany(UPSERT_RESOURCES_SQL, upsert_params)
                cursor.execute(MARK_MISSING_RESOURCES_DELETED_SQL, (seen_names,))
                soft_deleted_rows = cursor.rowcount
                return len(upsert_params), max(soft_deleted_rows, 0)

            cursor.execute(MARK_ALL_RESOURCES_DELETED_SQL)
            soft_deleted_rows = cursor.rowcount
            return 0, max(soft_deleted_rows, 0)

    @staticmethod
    def _cell_by_header(
        row: list[str],
        header_index_map: dict[str, int],
        field_name: str,
    ) -> str:
        index = header_index_map.get(field_name)
        if index is None:
            return ""
        if index >= len(row):
            return ""
        return row[index].strip()
