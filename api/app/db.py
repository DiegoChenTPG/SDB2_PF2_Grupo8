# Opción compat: usa psycopg_pool si no tienes psycopg.pool
try:
    from psycopg.pool import ConnectionPool  # psycopg >= 3.2
except ModuleNotFoundError:
    from psycopg_pool import ConnectionPool  # paquete separado

import os
from dotenv import load_dotenv

load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL no configurada")

pool = ConnectionPool(conninfo=DATABASE_URL, min_size=1, max_size=10)
