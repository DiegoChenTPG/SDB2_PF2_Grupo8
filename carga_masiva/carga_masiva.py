import os
import csv
import sys
from typing import List, Tuple, Optional

import psycopg2
from psycopg2.extras import execute_values

# --------------------------------------------------------------------
# Configuración
# --------------------------------------------------------------------
DB_CONFIG = {
    "host": os.getenv("PGHOST", "localhost"),
    "port": int(os.getenv("PGPORT", "5432")),
    "user": os.getenv("PGUSER", "postgres"),
    "password": os.getenv("PGPASSWORD", "bases2_proyecto"),
    "dbname": os.getenv("PGDATABASE", "bases2_proyectos"),
}

# Usa "imdb" si creaste el schema como en el compose sugerido.
SCHEMA = os.getenv("PGSCHEMA", "imdb")

# Ruta base a los TSV de IMDb (ajusta si es necesario)
BASE_DIR = os.getenv(
    "IMDB_DATA_DIR",
    os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "data"))
)

# Aumentar el límite del lector CSV para campos grandes
try:
    csv.field_size_limit(sys.maxsize)
except OverflowError:
    csv.field_size_limit(2**31 - 1)

# Tamaños de lote razonables (ajústalos si tu máquina lo permite)
BATCH_SMALL = 2000
BATCH_MED   = 4000


# --------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------
def _none(x: Optional[str]) -> Optional[str]:
    if x is None:
        return None
    x = x.strip()
    return None if x == r"\N" or x == "" else x

def _to_int(x: Optional[str]) -> Optional[int]:
    x = _none(x)
    if x is None:
        return None
    try:
        return int(x)
    except (ValueError, TypeError):
        return None

def _to_float(x: Optional[str]) -> Optional[float]:
    x = _none(x)
    if x is None:
        return None
    try:
        return float(x)
    except (ValueError, TypeError):
        return None

def _to_bool_01(x: Optional[str]) -> bool:
    # En fuente es '0' o '1'; cualquier valor distinto de "1" se considerará False
    return True if _none(x) == "1" else False

def _to_year(x: Optional[str]) -> Optional[int]:
    # IMDb trae años como "YYYY" o "\N"
    return _to_int(x)

def _split_csv(x: Optional[str]) -> List[str]:
    x = _none(x)
    if x is None:
        return []
    return [p.strip() for p in x.split(",") if p.strip()]

def _execute_values(cur, sql: str, rows: List[Tuple], page_size: int = 1000):
    if not rows:
        return 0
    execute_values(cur, sql, rows, page_size=page_size)
    return len(rows)

def _qualified(table: str) -> str:
    # Devuelve "schema.table"
    return f'{SCHEMA}."{table}"' if SCHEMA else f'"{table}"'


# --------------------------------------------------------------------
# Loaders
# --------------------------------------------------------------------
def _load_title_basics_and_genres(cur, conn) -> Tuple[int, int]:
    path = os.path.join(BASE_DIR, "title.basics.tsv")
    tb_rows, bg_rows = [], []
    ins_tb = ins_bg = 0

    with open(path, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f, delimiter="\t")
        for row in reader:
            tconst = row["tconst"]
            titleType = _none(row["titleType"])
            primaryTitle = _none(row["primaryTitle"])
            originalTitle = _none(row["originalTitle"])
            isAdult = _to_bool_01(row["isAdult"])
            startYear = _to_year(row["startYear"])
            endYear = _to_year(row["endYear"])
            runtimeMinutes = _to_int(row["runtimeMinutes"])

            tb_rows.append((tconst, titleType, primaryTitle, originalTitle,
                            isAdult, startYear, endYear, runtimeMinutes))
            # géneros normalizados
            for g in _split_csv(row.get("genres")):
                bg_rows.append((tconst, primaryTitle, g))

            if len(tb_rows) >= BATCH_SMALL:
                sql_tb = f"""
                    INSERT INTO {_qualified("title_basics")}
                    (tconst, "titleType", "primaryTitle", "originalTitle", "isAdult",
                    "startYear", "endYear", "runtimeMinutes")
                    VALUES %s
                    ON CONFLICT (tconst) DO NOTHING
                """
                ins_tb += _execute_values(cur, sql_tb, tb_rows, page_size=1000)
                tb_rows.clear()
                conn.commit()

            if len(bg_rows) >= BATCH_MED:
                sql_bg = f"""
                    INSERT INTO {_qualified("basics_genres")}
                    (tconst, "primaryTitle", genre)
                    VALUES %s
                    ON CONFLICT (tconst, genre) DO NOTHING
                """
                ins_bg += _execute_values(cur, sql_bg, bg_rows, page_size=2000)
                bg_rows.clear()
                conn.commit()

    if tb_rows:
        sql_tb = f"""
            INSERT INTO {_qualified("title_basics")}
            (tconst, "titleType", "primaryTitle", "originalTitle", "isAdult",
            "startYear", "endYear", "runtimeMinutes")
            VALUES %s
            ON CONFLICT (tconst) DO NOTHING
        """
        ins_tb += _execute_values(cur, sql_tb, tb_rows, page_size=1000)
        conn.commit()

    if bg_rows:
        sql_bg = f"""
            INSERT INTO {_qualified("basics_genres")}
            (tconst, "primaryTitle", genre)
            VALUES %s
            ON CONFLICT (tconst, genre) DO NOTHING
        """
        ins_bg += _execute_values(cur, sql_bg, bg_rows, page_size=2000)
        conn.commit()

    return ins_tb, ins_bg


def _load_name_basics_professions_known(cur, conn) -> Tuple[int, int, int]:
    path = os.path.join(BASE_DIR, "name.basics.tsv")
    nb_rows, np_rows, nk_rows = [], [], []
    ins_nb = ins_np = ins_nk = 0

    with open(path, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f, delimiter="\t")
        for row in reader:
            nconst = row["nconst"]
            primaryName = _none(row["primaryName"])
            birthYear = _to_year(row["birthYear"])
            deathYear = _to_year(row["deathYear"])

            nb_rows.append((nconst, primaryName, birthYear, deathYear))
            for prof in _split_csv(row.get("primaryProfession")):
                np_rows.append((nconst, prof))
            for tconst in _split_csv(row.get("knownForTitles")):
                nk_rows.append((nconst, tconst))

            if len(nb_rows) >= BATCH_SMALL:
                sql_nb = f"""
                    INSERT INTO {_qualified("name_basics")}
                    (nconst, "primaryName", "birthYear", "deathYear")
                    VALUES %s
                    ON CONFLICT (nconst) DO NOTHING
                """
                ins_nb += _execute_values(cur, sql_nb, nb_rows, page_size=1000)
                nb_rows.clear()
                conn.commit()

            if len(np_rows) >= BATCH_MED:
                sql_np = f"""
                    INSERT INTO {_qualified("name_professions")}
                    (nconst, profession)
                    VALUES %s
                    ON CONFLICT (nconst, profession) DO NOTHING
                """
                ins_np += _execute_values(cur, sql_np, np_rows, page_size=2000)
                np_rows.clear()
                conn.commit()

            if len(nk_rows) >= BATCH_MED:
                sql_nk = f"""
                    INSERT INTO {_qualified("name_known_for")}
                    (nconst, tconst)
                    VALUES %s
                    ON CONFLICT (nconst, tconst) DO NOTHING
                """
                ins_nk += _execute_values(cur, sql_nk, nk_rows, page_size=2000)
                nk_rows.clear()
                conn.commit()

    if nb_rows:
        sql_nb = f"""
            INSERT INTO {_qualified("name_basics")}
            (nconst, "primaryName", "birthYear", "deathYear")
            VALUES %s
            ON CONFLICT (nconst) DO NOTHING
        """
        ins_nb += _execute_values(cur, sql_nb, nb_rows, page_size=1000)
        conn.commit()

    if np_rows:
        sql_np = f"""
            INSERT INTO {_qualified("name_professions")}
            (nconst, profession)
            VALUES %s
            ON CONFLICT (nconst, profession) DO NOTHING
        """
        ins_np += _execute_values(cur, sql_np, np_rows, page_size=2000)
        conn.commit()

    if nk_rows:
        sql_nk = f"""
            INSERT INTO {_qualified("name_known_for")}
            (nconst, tconst)
            VALUES %s
            ON CONFLICT (nconst, tconst) DO NOTHING
        """
        ins_nk += _execute_values(cur, sql_nk, nk_rows, page_size=2000)
        conn.commit()

    return ins_nb, ins_np, ins_nk


def _load_title_akas_and_parts(cur, conn) -> Tuple[int, int, int]:
    path = os.path.join(BASE_DIR, "title.akas.tsv")
    aka_rows, typ_rows, att_rows = [], [], []
    ins_aka = ins_typ = ins_att = 0

    with open(path, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f, delimiter="\t")
        for row in reader:
            titleId = row["titleId"]
            ordering = _to_int(row["ordering"])
            title = _none(row["title"])
            region = _none(row.get("region"))
            isOriginalTitle = _to_bool_01(row.get("isOriginalTitle"))

            aka_rows.append((titleId, ordering, title, region, isOriginalTitle))
            for t in _split_csv(row.get("types")):
                typ_rows.append((titleId, ordering, t))
            for a in _split_csv(row.get("attributes")):
                att_rows.append((titleId, ordering, a))

            if len(aka_rows) >= BATCH_SMALL:
                sql_aka = f"""
                    INSERT INTO {_qualified("akas")}
                    ("titleId", ordering, title, region, "isOriginalTitle")
                    VALUES %s
                    ON CONFLICT ("titleId", ordering) DO NOTHING
                """
                ins_aka += _execute_values(cur, sql_aka, aka_rows, page_size=1000)
                aka_rows.clear()
                conn.commit()

            if len(typ_rows) >= BATCH_MED:
                sql_typ = f"""
                    INSERT INTO {_qualified("aka_types")}
                    ("titleId", ordering, type)
                    VALUES %s
                    ON CONFLICT ("titleId", ordering, type) DO NOTHING
                """
                ins_typ += _execute_values(cur, sql_typ, typ_rows, page_size=2000)
                typ_rows.clear()
                conn.commit()

            if len(att_rows) >= BATCH_MED:
                sql_att = f"""
                    INSERT INTO {_qualified("aka_attributes")}
                    ("titleId", ordering, attribute)
                    VALUES %s
                    ON CONFLICT ("titleId", ordering, attribute) DO NOTHING
                """
                ins_att += _execute_values(cur, sql_att, att_rows, page_size=2000)
                att_rows.clear()
                conn.commit()

    if aka_rows:
        sql_aka = f"""
            INSERT INTO {_qualified("akas")}
            ("titleId", ordering, title, region, "isOriginalTitle")
            VALUES %s
            ON CONFLICT ("titleId", ordering) DO NOTHING
        """
        ins_aka += _execute_values(cur, sql_aka, aka_rows, page_size=1000)
        conn.commit()

    if typ_rows:
        sql_typ = f"""
            INSERT INTO {_qualified("aka_types")}
            ("titleId", ordering, type)
            VALUES %s
            ON CONFLICT ("titleId", ordering, type) DO NOTHING
        """
        ins_typ += _execute_values(cur, sql_typ, typ_rows, page_size=2000)
        conn.commit()

    if att_rows:
        sql_att = f"""
            INSERT INTO {_qualified("aka_attributes")}
            ("titleId", ordering, attribute)
            VALUES %s
            ON CONFLICT ("titleId", ordering, attribute) DO NOTHING
        """
        ins_att += _execute_values(cur, sql_att, att_rows, page_size=2000)
        conn.commit()

    return ins_aka, ins_typ, ins_att


def _load_title_crew(cur, conn) -> Tuple[int, int]:
    path = os.path.join(BASE_DIR, "title.crew.tsv")
    dir_rows, wri_rows = [], []
    ins_dir = ins_wri = 0

    with open(path, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f, delimiter="\t")
        for row in reader:
            tconst = row["tconst"]
            for d in _split_csv(row.get("directors")):
                dir_rows.append((tconst, d))
            for w in _split_csv(row.get("writers")):
                wri_rows.append((tconst, w))

            if len(dir_rows) >= BATCH_MED:
                sql_dir = f"""
                    INSERT INTO {_qualified("crew_directors")}
                    (tconst, nconst)
                    VALUES %s
                    ON CONFLICT (tconst, nconst) DO NOTHING
                """
                ins_dir += _execute_values(cur, sql_dir, dir_rows, page_size=2000)
                dir_rows.clear()
                conn.commit()

            if len(wri_rows) >= BATCH_MED:
                sql_wri = f"""
                    INSERT INTO {_qualified("crew_writers")}
                    (tconst, nconst)
                    VALUES %s
                    ON CONFLICT (tconst, nconst) DO NOTHING
                """
                ins_wri += _execute_values(cur, sql_wri, wri_rows, page_size=2000)
                wri_rows.clear()
                conn.commit()

    if dir_rows:
        sql_dir = f"""
            INSERT INTO {_qualified("crew_directors")}
            (tconst, nconst)
            VALUES %s
            ON CONFLICT (tconst, nconst) DO NOTHING
        """
        ins_dir += _execute_values(cur, sql_dir, dir_rows, page_size=2000)
        conn.commit()

    if wri_rows:
        sql_wri = f"""
            INSERT INTO {_qualified("crew_writers")}
            (tconst, nconst)
            VALUES %s
            ON CONFLICT (tconst, nconst) DO NOTHING
        """
        ins_wri += _execute_values(cur, sql_wri, wri_rows, page_size=2000)
        conn.commit()

    return ins_dir, ins_wri


def _load_title_episode(cur, conn) -> int:
    path = os.path.join(BASE_DIR, "title.episode.tsv")
    ep_rows = []
    ins_ep = 0

    with open(path, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f, delimiter="\t")
        for row in reader:
            tconst = row["tconst"]
            parent = _none(row["parentTconst"])
            season = _to_int(row.get("seasonNumber"))
            epis   = _to_int(row.get("episodeNumber"))
            ep_rows.append((tconst, parent, season, epis))

            if len(ep_rows) >= BATCH_SMALL:
                sql_ep = f"""
                    INSERT INTO {_qualified("episodes")}
                    (tconst, "parentTconst", "seasonNumber", "episodeNumber")
                    VALUES %s
                    ON CONFLICT (tconst) DO NOTHING
                """
                ins_ep += _execute_values(cur, sql_ep, ep_rows, page_size=1000)
                ep_rows.clear()
                conn.commit()

    if ep_rows:
        sql_ep = f"""
            INSERT INTO {_qualified("episodes")}
            (tconst, "parentTconst", "seasonNumber", "episodeNumber")
            VALUES %s
            ON CONFLICT (tconst) DO NOTHING
        """
        ins_ep += _execute_values(cur, sql_ep, ep_rows, page_size=1000)
        conn.commit()

    return ins_ep


def _load_title_principals(cur, conn) -> int:
    path = os.path.join(BASE_DIR, "title.principals.tsv")
    pr_rows = []
    ins_pr = 0

    with open(path, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f, delimiter="\t")
        for row in reader:
            pr_rows.append((
                row["tconst"],
                _to_int(row["ordering"]),
                row["nconst"],
                _none(row["category"]),
                _none(row.get("job")),
                _none(row.get("characters"))
            ))

            if len(pr_rows) >= BATCH_SMALL:
                sql_pr = f"""
                    INSERT INTO {_qualified("principals")}
                    (tconst, ordering, nconst, category, job, characters)
                    VALUES %s
                    ON CONFLICT (tconst, ordering) DO NOTHING
                """
                ins_pr += _execute_values(cur, sql_pr, pr_rows, page_size=1000)
                pr_rows.clear()
                conn.commit()

    if pr_rows:
        sql_pr = f"""
            INSERT INTO {_qualified("principals")}
            (tconst, ordering, nconst, category, job, characters)
            VALUES %s
            ON CONFLICT (tconst, ordering) DO NOTHING
        """
        ins_pr += _execute_values(cur, sql_pr, pr_rows, page_size=1000)
        conn.commit()

    return ins_pr


def _load_title_ratings(cur, conn) -> int:
    path = os.path.join(BASE_DIR, "title.ratings.tsv")
    rt_rows = []
    ins_rt = 0

    with open(path, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f, delimiter="\t")
        for row in reader:
            rt_rows.append((
                row["tconst"],
                _to_float(row["averageRating"]),
                _to_int(row["numVotes"])
            ))

            if len(rt_rows) >= BATCH_MED:
                sql_rt = f"""
                    INSERT INTO {_qualified("ratings")}
                    (tconst, "averageRating", "numVotes")
                    VALUES %s
                    ON CONFLICT (tconst) DO NOTHING
                """
                ins_rt += _execute_values(cur, sql_rt, rt_rows, page_size=2000)
                rt_rows.clear()
                conn.commit()

    if rt_rows:
        sql_rt = f"""
            INSERT INTO {_qualified("ratings")}
            (tconst, "averageRating", "numVotes")
            VALUES %s
            ON CONFLICT (tconst) DO NOTHING
        """
        ins_rt += _execute_values(cur, sql_rt, rt_rows, page_size=2000)
        conn.commit()

    return ins_rt


# --------------------------------------------------------------------
# Orquestador
# --------------------------------------------------------------------
def health_check() -> str:
    try:
        with psycopg2.connect(**DB_CONFIG) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1;")
                _ = cur.fetchone()
        return "Conectada"
    except Exception as e:
        return f"No Conectada: {e}"

def carga_masiva() -> str:
    conn = None
    try:
        print(f"BASE_DIR: {BASE_DIR}")
        print(f"SCHEMA: {SCHEMA}")
        conn = psycopg2.connect(**DB_CONFIG)
        conn.autocommit = False
        with conn.cursor() as cur:
            cur.execute(f"SET search_path TO {SCHEMA}, public;")

            print("Cargando title_basics y basics_genres...")
            ins_tb, ins_bg = _load_title_basics_and_genres(cur, conn)
            print(f"OK title_basics={ins_tb}, basics_genres={ins_bg}")

            print("Cargando name_basics, name_professions y name_known_for...")
            ins_nb, ins_np, ins_nk = _load_name_basics_professions_known(cur, conn)
            print(f"OK name_basics={ins_nb}, name_professions={ins_np}, name_known_for={ins_nk}")

            print("Cargando akas, aka_types y aka_attributes...")
            ins_aka, ins_typ, ins_att = _load_title_akas_and_parts(cur, conn)
            print(f"OK akas={ins_aka}, aka_types={ins_typ}, aka_attributes={ins_att}")

            print("Cargando crew_directors y crew_writers...")
            ins_dir, ins_wri = _load_title_crew(cur, conn)
            print(f"OK crew_directors={ins_dir}, crew_writers={ins_wri}")

            print("Cargando episodes...")
            ins_ep = _load_title_episode(cur, conn)
            print(f"OK episodes={ins_ep}")

            print("Cargando principals...")
            ins_pr = _load_title_principals(cur, conn)
            print(f"OK principals={ins_pr}")

            print("Cargando ratings...")
            ins_rt = _load_title_ratings(cur, conn)
            print(f"OK ratings={ins_rt}")

        conn.commit()
        print("Commit final realizado.")

        resumen = (
            f"title_basics: {ins_tb}, basics_genres: {ins_bg}, "
            f"name_basics: {ins_nb}, name_professions: {ins_np}, name_known_for: {ins_nk}, "
            f"akas: {ins_aka}, aka_types: {ins_typ}, aka_attributes: {ins_att}, "
            f"crew_directors: {ins_dir}, crew_writers: {ins_wri}, "
            f"episodes: {ins_ep}, principals: {ins_pr}, ratings: {ins_rt}"
        )
        print("Resumen inserts (ON CONFLICT DO NOTHING evita duplicados):")
        print(resumen)
        return "datos cargados correctamente"
    except Exception as e:
        if conn:
            conn.rollback()
        print(f"Error al procesar los datos de entrada: {e}")
        return "Error al procesar los datos de entrada"
    finally:
        if conn:
            conn.close()


if __name__ == "__main__":
    print(health_check())
    print(carga_masiva())
