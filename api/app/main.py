from fastapi import FastAPI, HTTPException
from app.models import NameBasicIn, BatchIn
from app.db import pool

app = FastAPI(title="Name Basics API", version="1.0.0")

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

@app.get("/health")
def health():
    return {"status": "ok"}

@app.post("/name_basics")
def insert_one(item: NameBasicIn, upsert: bool = True):
    sql = UPSERT_SQL if upsert else INSERT_SQL
    try:
        with pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (item.nconst, item.primaryName, item.birthYear, item.deathYear))
                conn.commit()
        return {"inserted": True, "upsert": upsert, "nconst": item.nconst}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/name_basics/batch")
def insert_batch(batch: BatchIn):
    sql = UPSERT_SQL if batch.upsert else INSERT_SQL
    try:
        with pool.connection() as conn:
            with conn.cursor() as cur:
                cur.executemany(sql, [
                    (it.nconst, it.primaryName, it.birthYear, it.deathYear)
                    for it in batch.items
                ])
                conn.commit()
        return {"count": len(batch.items), "upsert": batch.upsert}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
