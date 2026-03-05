from psycopg import Connection, connect

from .settings import Settings


def open_postgres_connection(settings: Settings) -> Connection:
    return connect(
        host=settings.db_host,
        dbname=settings.db_name,
        user=settings.db_user,
        password=settings.db_password,
        port=settings.db_port,
        sslmode=settings.db_sslmode,
    )
