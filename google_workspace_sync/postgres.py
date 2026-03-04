from psycopg import Connection, connect

from .config import PostgresSettings


def open_postgres_connection(postgres: PostgresSettings) -> Connection:
    return connect(
        host=postgres.host,
        dbname=postgres.database,
        user=postgres.user,
        password=postgres.password,
        port=postgres.port,
    )
