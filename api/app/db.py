# app/db.py
import os
from psycopg import errors
from psycopg.pool import ConnectionPool
from dotenv import load_dotenv

load_dotenv()
DB_URL = os.getenv("DATABASE_URL")
if not DB_URL:
    raise RuntimeError("DATABASE_URL no configurada")

pool = ConnectionPool(conninfo=DB_URL, min_size=1, max_size=10)

def run_write(sql, params):
    try:
        with pool.connection() as conn, conn.cursor() as cur:
            cur.execute(sql, params)
            conn.commit()
    except errors.ReadOnlySqlTransaction:
        # el pool estaba pegado al standby → recrear una vez
        pool.close()
        pool.open()  # o crea uno nuevo si prefieres
        with pool.connection() as conn, conn.cursor() as cur:
            cur.execute(sql, params)
            conn.commit()

def run_write_many(sql, params_seq):
    """Batch con executemany y el mismo patrón de reintento 1 vez."""
    try:
        with pool.connection() as conn, conn.cursor() as cur:
            cur.executemany(sql, params_seq)
            conn.commit()
    except errors.ReadOnlySqlTransaction:
        pool.close()
        pool.open()
        with pool.connection() as conn, conn.cursor() as cur:
            cur.executemany(sql, params_seq)
            conn.commit()
