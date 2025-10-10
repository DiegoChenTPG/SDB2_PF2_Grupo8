# locustfile.py
import os
import random
import itertools
from typing import List, Dict, Optional
from locust import HttpUser, task, between, events

FIRST = ["Ana","Luis","María","Carlos","Sofía","Jorge","Elena","Mateo","Lucía","Diego"]
LAST  = ["García","Hernández","Martínez","López","González","Pérez","Rodríguez","Sánchez","Ramírez","Flores"]


# --- Config ---
BATCH_SIZE = int(os.getenv("LOCUST_BATCH_SIZE", "50"))

# Un offset aleatorio por proceso para evitar colisiones si corres en distribuido
_BASE_OFFSET = random.randint(0, 9_000_000)
_counter = itertools.count(start=_BASE_OFFSET)

def _nconst_next() -> str:
    """Genera nm + 7 dígitos, evitando (en lo posible) colisiones por proceso."""
    i = next(_counter) % 10_000_000  # aseguramos 7 dígitos
    return f"nm{i:07d}"

def random_person_name():
    return f"{random.choice(FIRST)} {random.choice(LAST)}"

def synthetic_record() -> Dict:
    by = random.randint(1850, 2010)
    # 75% sin deathYear; si lo tiene, que sea >= birthYear
    if random.random() < 0.75:
        dy: Optional[int] = None
    else:
        dy = random.randint(max(by, 1900), 2024)
    return {
        "nconst": _nconst_next(),
        "primaryName": random_person_name(),
        "birthYear": by,
        "deathYear": dy,
    }

def synthetic_batch(k: int) -> List[Dict]:
    return [synthetic_record() for _ in range(k)]

class NameBasicsUser(HttpUser):

    wait_time = between(1.0, 2.0)

    @task(3)
    def insert_one(self):
        payload = synthetic_record()
        self.client.post("/name_basics", json=payload, name="POST /name_basics")

    @task(1)
    def insert_batch(self):
        items = synthetic_batch(BATCH_SIZE)
        payload = {"items": items, "upsert": True}
        self.client.post("/name_basics/batch", json=payload, name="POST /name_basics/batch")



# locust -f locustfile.py --host=http://localhost:8000