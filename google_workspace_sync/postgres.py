from psycopg2 import connect
from psycopg2.extensions import connection as PgConnection

from .settings import Settings


def open_postgres_connection(settings: Settings) -> PgConnection:
    """Create a database connection using environment variables."""
    conn_params = {
        "host": settings.db_host,
        "database": settings.db_name,
        "user": settings.db_user,
        "password": settings.db_password,
        "port": settings.db_port,
    }
    if settings.db_sslmode:
        conn_params["sslmode"] = settings.db_sslmode

    if settings.db_pool_mode:
        conn_params["options"] = f"-c pool_mode={settings.db_pool_mode}"

    conn = connect(**conn_params)
    with conn.cursor() as cur:
        cur.execute("SET TIME ZONE 'Europe/Bratislava'")
    return conn
