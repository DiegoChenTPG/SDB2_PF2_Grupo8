import csv
import os
import random
import logging
from locust import HttpUser, task, between, events

logger = logging.getLogger("locust")  # <-- usa el logger de locust

BASE_PATH = os.path.dirname(__file__)
DATA_CSV = os.path.join(BASE_PATH, "sample_nbs.csv")

_records = []

@events.init.add_listener
def on_locust_init(environment, **kwargs):
    """Carga datos una sola vez al iniciar Locust."""
    global _records
    try:
        if os.path.isfile(DATA_CSV):
            with open(DATA_CSV, newline="", encoding="utf-8") as f:
                reader = csv.reader(f)
                for row in reader:
                    if not row or row[0].startswith("#"):
                        continue
                    nconst = row[0].strip()
                    pname = row[1].strip()
                    byear = row[2].strip() or None
                    dyear = row[3].strip() or None
                    _records.append({
                        "nconst": nconst,
                        "primaryName": pname,
                        "birthYear": int(byear) if byear else None,
                        "deathYear": int(dyear) if dyear else None
                    })
            logger.info("Cargados %d registros desde %s", len(_records), DATA_CSV)
        else:
            logger.warning("No se encontró %s; se generarán datos sintéticos", DATA_CSV)
    except Exception as e:
        logger.exception("Error cargando %s: %s", DATA_CSV, e)

def synthetic_record(i: int):
    return {
        "nconst": f"nm{i:07d}",
        "primaryName": f"Name {i}",
        "birthYear": random.randint(1850, 2010),
        "deathYear": None if random.random() < 0.7 else random.randint(1900, 2024)
    }

class NameBasicsUser(HttpUser):
    wait_time = between(0.2, 1.0)

    @task(3)
    def insert_one(self):
        payload = random.choice(_records) if _records else synthetic_record(random.randint(1, 9_999_999))
        self.client.post("/name_basics", json=payload, name="POST /name_basics")

    @task(1)
    def insert_batch(self):
        items = random.sample(_records, k=min(50, len(_records))) if _records else [
            synthetic_record(random.randint(1, 9_999_999)) for _ in range(50)
        ]
        payload = {"items": items, "upsert": True}
        self.client.post("/name_basics/batch", json=payload, name="POST /name_basics/batch")
