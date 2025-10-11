# app/backup_logs.py
from typing import Optional, List
from datetime import datetime, timezone
from pydantic import BaseModel, Field
from fastapi import APIRouter
from redis import Redis
import json

# Si la API corre en Windows (fuera de Docker), deja "localhost".
# Si la API estuviera en un contenedor del mismo compose, usa host="redis".
r = Redis(host="localhost", port=6379, db=0, decode_responses=True)

BACKUP_LOG_KEY = "backups:logs"

class BackupLogIn(BaseModel):
    when: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    node: str                   # ej. "pg-bases2"
    stanza: str                 # ej. "bases2-db"
    dbname: str                 # ej. "bases2_proyectos"
    type: str                   # "full" | "diff" | "incr"
    label: Optional[str] = None
    repo_size_bytes: Optional[int] = None
    db_backup_bytes: Optional[int] = None
    duration_sec: Optional[float] = None
    wal_start: Optional[str] = None
    wal_stop: Optional[str] = None
    notes: Optional[str] = None

class BackupLogOut(BackupLogIn):
    id: str

router = APIRouter(prefix="/backup", tags=["backup-logs"])

@router.post("/log", response_model=BackupLogOut)
def push_backup_log(entry: BackupLogIn):
    base = int(entry.when.timestamp())
    suffix = entry.label or entry.type
    log_id = f"{base}:{suffix}"

    payload = entry.model_dump()
    payload["id"] = log_id

    r.lpush(BACKUP_LOG_KEY, json.dumps(payload))
    r.ltrim(BACKUP_LOG_KEY, 0, 499)  # conserva últimos 500

    return BackupLogOut(**payload)

@router.get("/logs", response_model=List[BackupLogOut])
def list_backup_logs(limit: int = 50):
    items = r.lrange(BACKUP_LOG_KEY, 0, max(limit, 1) - 1)
    out: List[BackupLogOut] = []
    for it in items:
        try:
            out.append(BackupLogOut(**json.loads(it)))
        except Exception:
            continue
    return out
