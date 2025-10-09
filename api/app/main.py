# app/main.py
from fastapi import FastAPI, HTTPException
from app.models import NameBasicIn, BatchIn
from app.db import run_write, run_write_many, pool  # <- importa del nuevo db.py

INSERT_SQL = """
INSERT INTO name_basics (nconst, primaryName, birthYear, deathYear)
VALUES (%s, %s, %s, %s)
ON CONFLICT (nconst) DO NOTHING;
"""
UPSERT_SQL = """
INSERT INTO name_basics (nconst, primaryName, birthYear, deathYear)
VALUES (%s, %s, %s, %s)
ON CONFLICT (nconst) DO UPDATE
SET primaryName = EXCLUDED.primaryName,
    birthYear   = EXCLUDED.birthYear,
    deathYear   = EXCLUDED.deathYear;
"""

app = FastAPI(title="Name Basics API", version="1.0.0")

@app.get("/health")
def health():
    # opcional: muestra rol actual
    try:
        with pool.connection() as conn, conn.cursor() as cur:
            cur.execute("SELECT pg_is_in_recovery();")
            ro = cur.fetchone()[0]
        return {"status": "ok", "role": "standby" if ro else "primary"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/name_basics")
def insert_one(item: NameBasicIn, upsert: bool = True):
    sql = UPSERT_SQL if upsert else INSERT_SQL
    try:
        run_write(sql, (item.nconst, item.primaryName, item.birthYear, item.deathYear))
        return {"inserted": True, "upsert": upsert, "nconst": item.nconst}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/name_basics/batch")
def insert_batch(batch: BatchIn):
    sql = UPSERT_SQL if batch.upsert else INSERT_SQL
    try:
        params_seq = [(it.nconst, it.primaryName, it.birthYear, it.deathYear) for it in batch.items]
        run_write_many(sql, params_seq)
        return {"count": len(batch.items), "upsert": batch.upsert}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
