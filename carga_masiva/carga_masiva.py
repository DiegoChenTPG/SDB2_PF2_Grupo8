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

# Usa "imdb" si creaste el schema así; cambia a "public" si fuera el caso.
SCHEMA = os.getenv("PGSCHEMA", "imdb")

# Ruta base a los TSV de IMDb (ajusta si es necesario)
BASE_DIR = os.getenv(
    "IMDB_DATA_DIR",
    os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "data"))
)

# CSV grandes
try:
    csv.field_size_limit(sys.maxsize)
except OverflowError:
    csv.field_size_limit(2**31 - 1)

# Tamaños de lote
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
    return True if _none(x) == "1" else False

def _to_year(x: Optional[str]) -> Optional[int]:
    return _to_int(x)

def _split_csv(x: Optional[str]):
    x = _none(x)
    if x is None:
        return []
    return [p.strip() for p in x.split(",") if p.strip()]

def _qualified(table: str) -> str:
    return f"{SCHEMA}.{table}" if SCHEMA else table

def _execute_values(cur, sql: str, rows: list, page_size: int = 1000) -> int:
    if not rows:
        return 0
    execute_values(cur, sql, rows, page_size=page_size)
    return len(rows)

# Helpers de staging (tabla temporal -> insert filtrando FKs)
def _stage(cur, temp_name: str, cols_ddl: str):
    cur.execute(f"CREATE TEMP TABLE {temp_name} ({cols_ddl}) ON COMMIT DROP;")

def _stage_fill(cur, temp_name: str, rows: list, page_size: int = 1000):
    if not rows:
        return 0
    execute_values(cur, f"INSERT INTO {temp_name} VALUES %s", rows, page_size=page_size)
    return len(rows)

# --------------------------------------------------------------------
# Loaders
# --------------------------------------------------------------------
# Inserta primero title_basics y luego basics_genres; basics_genres se filtra por EXISTS.
# Rellena NOT NULL con defaults si vienen \N.
def _load_title_basics_and_genres(cur, conn) -> Tuple[int, int]:
    path = os.path.join(BASE_DIR, "title.basics.tsv")
    tb_rows, bg_rows = [], []
    ins_tb = ins_bg = 0

    with open(path, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f, delimiter="\t")
        for row in reader:
            tconst = row["tconst"]
            titletype = _none(row["titleType"]) or r"\N"
            primarytitle = _none(row["primaryTitle"]) or r"\N"
            originaltitle = _none(row["originalTitle"]) or r"\N"
            isadult = _to_bool_01(row["isAdult"])
            startyear = _to_year(row["startYear"])
            endyear = _to_year(row["endYear"])
            runtimeminutes = _to_int(row["runtimeMinutes"])

            tb_rows.append((tconst, titletype, primarytitle, originaltitle,
                            isadult, startyear, endyear, runtimeminutes))

            for g in _split_csv(row.get("genres")):
                bg_rows.append((tconst, primarytitle, g))

            if len(tb_rows) >= BATCH_SMALL:
                sql_tb = f"""
                    INSERT INTO {_qualified("title_basics")}
                    (tconst, titletype, primarytitle, originaltitle, isadult,
                     startyear, endyear, runtimeminutes)
                    VALUES %s
                    ON CONFLICT (tconst) DO NOTHING
                """
                ins_tb += _execute_values(cur, sql_tb, tb_rows, page_size=1000)
                tb_rows.clear()
                conn.commit()

            if len(bg_rows) >= BATCH_MED:
                if tb_rows:
                    sql_tb = f"""
                        INSERT INTO {_qualified("title_basics")}
                        (tconst, titletype, primarytitle, originaltitle, isadult,
                         startyear, endyear, runtimeminutes)
                        VALUES %s
                        ON CONFLICT (tconst) DO NOTHING
                    """
                    ins_tb += _execute_values(cur, sql_tb, tb_rows, page_size=1000)
                    tb_rows.clear()
                    conn.commit()

                _stage(cur, "tmp_bg", "tconst varchar(20), primarytitle text, genre varchar(64)")
                _stage_fill(cur, "tmp_bg", bg_rows, page_size=2000)
                cur.execute(f"""
                    INSERT INTO {_qualified("basics_genres")} (tconst, primarytitle, genre)
                    SELECT tconst, primarytitle, genre
                    FROM tmp_bg t
                    WHERE EXISTS (SELECT 1 FROM {_qualified("title_basics")} b WHERE b.tconst = t.tconst)
                    ON CONFLICT (tconst, genre) DO NOTHING
                """)
                ins_bg += len(bg_rows)
                bg_rows.clear()
                conn.commit()

    if tb_rows:
        sql_tb = f"""
            INSERT INTO {_qualified("title_basics")}
            (tconst, titletype, primarytitle, originaltitle, isadult,
             startyear, endyear, runtimeminutes)
            VALUES %s
            ON CONFLICT (tconst) DO NOTHING
        """
        ins_tb += _execute_values(cur, sql_tb, tb_rows, page_size=1000)
        tb_rows.clear()
        conn.commit()

    if bg_rows:
        _stage(cur, "tmp_bg2", "tconst varchar(20), primarytitle text, genre varchar(64)")
        _stage_fill(cur, "tmp_bg2", bg_rows, page_size=2000)
        cur.execute(f"""
            INSERT INTO {_qualified("basics_genres")} (tconst, primarytitle, genre)
            SELECT tconst, primarytitle, genre
            FROM tmp_bg2 t
            WHERE EXISTS (SELECT 1 FROM {_qualified("title_basics")} b WHERE b.tconst = t.tconst)
            ON CONFLICT (tconst, genre) DO NOTHING
        """)
        ins_bg += len(bg_rows)
        bg_rows.clear()
        conn.commit()

    return ins_tb, ins_bg


# name_basics -> inserta siempre; si primaryname viene \N, se rellena con '\N'.
# name_professions -> filtrado por EXISTS en name_basics
# name_known_for -> filtrado por EXISTS en name_basics y title_basics
def _load_name_basics_professions_known(cur, conn) -> Tuple[int, int, int]:
    path = os.path.join(BASE_DIR, "name.basics.tsv")
    nb_rows, np_rows, nk_rows = [], [], []
    ins_nb = ins_np = ins_nk = 0

    with open(path, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f, delimiter="\t")
        for row in reader:
            nconst = row["nconst"]
            primaryname = _none(row["primaryName"]) or r"\N"
            birthyear = _to_year(row["birthYear"])
            deathyear = _to_year(row["deathYear"])

            nb_rows.append((nconst, primaryname, birthyear, deathyear))

            for prof in _split_csv(row.get("primaryProfession")):
                np_rows.append((nconst, prof))
            for tconst in _split_csv(row.get("knownForTitles")):
                nk_rows.append((nconst, tconst))

            if len(nb_rows) >= BATCH_SMALL:
                sql_nb = f"""
                    INSERT INTO {_qualified("name_basics")}
                    (nconst, primaryname, birthyear, deathyear)
                    VALUES %s
                    ON CONFLICT (nconst) DO NOTHING
                """
                ins_nb += _execute_values(cur, sql_nb, nb_rows, page_size=1000)
                nb_rows.clear()
                conn.commit()

            if len(np_rows) >= BATCH_MED:
                if nb_rows:
                    sql_nb = f"""
                        INSERT INTO {_qualified("name_basics")}
                        (nconst, primaryname, birthyear, deathyear)
                        VALUES %s
                        ON CONFLICT (nconst) DO NOTHING
                    """
                    ins_nb += _execute_values(cur, sql_nb, nb_rows, page_size=1000)
                    nb_rows.clear()
                    conn.commit()

                _stage(cur, "tmp_np", "nconst varchar(20), profession varchar(64)")
                _stage_fill(cur, "tmp_np", np_rows, page_size=2000)
                cur.execute(f"""
                    INSERT INTO {_qualified("name_professions")} (nconst, profession)
                    SELECT t.nconst, t.profession
                    FROM tmp_np t
                    WHERE EXISTS (SELECT 1 FROM {_qualified("name_basics")} nb WHERE nb.nconst = t.nconst)
                    ON CONFLICT (nconst, profession) DO NOTHING
                """)
                ins_np += len(np_rows)
                np_rows.clear()
                conn.commit()

            if len(nk_rows) >= BATCH_MED:
                if nb_rows:
                    sql_nb = f"""
                        INSERT INTO {_qualified("name_basics")}
                        (nconst, primaryname, birthyear, deathyear)
                        VALUES %s
                        ON CONFLICT (nconst) DO NOTHING
                    """
                    ins_nb += _execute_values(cur, sql_nb, nb_rows, page_size=1000)
                    nb_rows.clear()
                    conn.commit()

                _stage(cur, "tmp_nk", "nconst varchar(20), tconst varchar(20)")
                _stage_fill(cur, "tmp_nk", nk_rows, page_size=2000)
                cur.execute(f"""
                    INSERT INTO {_qualified("name_known_for")} (nconst, tconst)
                    SELECT t.nconst, t.tconst
                    FROM tmp_nk t
                    WHERE EXISTS (SELECT 1 FROM {_qualified("name_basics")} nb WHERE nb.nconst = t.nconst)
                      AND EXISTS (SELECT 1 FROM {_qualified("title_basics")} tb WHERE tb.tconst = t.tconst)
                    ON CONFLICT (nconst, tconst) DO NOTHING
                """)
                ins_nk += len(nk_rows)
                nk_rows.clear()
                conn.commit()

    if nb_rows:
        sql_nb = f"""
            INSERT INTO {_qualified("name_basics")}
            (nconst, primaryname, birthyear, deathyear)
            VALUES %s
            ON CONFLICT (nconst) DO NOTHING
        """
        ins_nb += _execute_values(cur, sql_nb, nb_rows, page_size=1000)
        nb_rows.clear()
        conn.commit()

    if np_rows:
        _stage(cur, "tmp_np2", "nconst varchar(20), profession varchar(64)")
        _stage_fill(cur, "tmp_np2", np_rows, page_size=2000)
        cur.execute(f"""
            INSERT INTO {_qualified("name_professions")} (nconst, profession)
            SELECT t.nconst, t.profession
            FROM tmp_np2 t
            WHERE EXISTS (SELECT 1 FROM {_qualified("name_basics")} nb WHERE nb.nconst = t.nconst)
            ON CONFLICT (nconst, profession) DO NOTHING
        """)
        ins_np += len(np_rows)
        np_rows.clear()
        conn.commit()

    if nk_rows:
        _stage(cur, "tmp_nk2", "nconst varchar(20), tconst varchar(20)")
        _stage_fill(cur, "tmp_nk2", nk_rows, page_size=2000)
        cur.execute(f"""
            INSERT INTO {_qualified("name_known_for")} (nconst, tconst)
            SELECT t.nconst, t.tconst
            FROM tmp_nk2 t
            WHERE EXISTS (SELECT 1 FROM {_qualified("name_basics")} nb WHERE nb.nconst = t.nconst)
              AND EXISTS (SELECT 1 FROM {_qualified("title_basics")} tb WHERE tb.tconst = t.tconst)
            ON CONFLICT (nconst, tconst) DO NOTHING
        """)
        ins_nk += len(nk_rows)
        nk_rows.clear()
        conn.commit()

    return ins_nb, ins_np, ins_nk


# akas -> filtrado por EXISTS en title_basics
# aka_types/aka_attributes -> filtrado por EXISTS en akas (padre compuesto)
# Rellena NOT NULL de 'title' con '\N' si viene nulo.
def _load_title_akas_and_parts(cur, conn) -> Tuple[int, int, int]:
    path = os.path.join(BASE_DIR, "title.akas.tsv")
    aka_rows, typ_rows, att_rows = [], [], []
    ins_aka = ins_typ = ins_att = 0

    with open(path, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f, delimiter="\t")
        for row in reader:
            titleid = row["titleId"]
            ordering = _to_int(row["ordering"])
            title = _none(row["title"]) or r"\N"
            region = _none(row.get("region"))
            isoriginaltitle = _to_bool_01(row.get("isOriginalTitle"))

            aka_rows.append((titleid, ordering, title, region, isoriginaltitle))
            for t in _split_csv(row.get("types")):
                typ_rows.append((titleid, ordering, t))
            for a in _split_csv(row.get("attributes")):
                att_rows.append((titleid, ordering, a))

            if len(aka_rows) >= BATCH_SMALL:
                _stage(cur, "tmp_aka", "titleid varchar(20), ordering int, title text, region varchar(64), isoriginaltitle boolean")
                _stage_fill(cur, "tmp_aka", aka_rows, page_size=1000)
                cur.execute(f"""
                    INSERT INTO {_qualified("akas")} (titleid, ordering, title, region, isoriginaltitle)
                    SELECT t.titleid, t.ordering, t.title, t.region, t.isoriginaltitle
                    FROM tmp_aka t
                    WHERE EXISTS (SELECT 1 FROM {_qualified("title_basics")} b WHERE b.tconst = t.titleid)
                    ON CONFLICT (titleid, ordering) DO NOTHING
                """)
                ins_aka += len(aka_rows)
                aka_rows.clear()
                conn.commit()

            if len(typ_rows) >= BATCH_MED:
                _stage(cur, "tmp_typ", "titleid varchar(20), ordering int, type text")
                _stage_fill(cur, "tmp_typ", typ_rows, page_size=2000)
                cur.execute(f"""
                    INSERT INTO {_qualified("aka_types")} (titleid, ordering, type)
                    SELECT t.titleid, t.ordering, t.type
                    FROM tmp_typ t
                    WHERE EXISTS (
                      SELECT 1 FROM {_qualified("akas")} a
                      WHERE a.titleid = t.titleid AND a.ordering = t.ordering
                    )
                    ON CONFLICT (titleid, ordering, type) DO NOTHING
                """)
                ins_typ += len(typ_rows)
                typ_rows.clear()
                conn.commit()

            if len(att_rows) >= BATCH_MED:
                _stage(cur, "tmp_att", "titleid varchar(20), ordering int, attribute text")
                _stage_fill(cur, "tmp_att", att_rows, page_size=2000)
                cur.execute(f"""
                    INSERT INTO {_qualified("aka_attributes")} (titleid, ordering, attribute)
                    SELECT t.titleid, t.ordering, t.attribute
                    FROM tmp_att t
                    WHERE EXISTS (
                      SELECT 1 FROM {_qualified("akas")} a
                      WHERE a.titleid = t.titleid AND a.ordering = t.ordering
                    )
                    ON CONFLICT (titleid, ordering, attribute) DO NOTHING
                """)
                ins_att += len(att_rows)
                att_rows.clear()
                conn.commit()

    if aka_rows:
        _stage(cur, "tmp_aka2", "titleid varchar(20), ordering int, title text, region varchar(64), isoriginaltitle boolean")
        _stage_fill(cur, "tmp_aka2", aka_rows, page_size=1000)
        cur.execute(f"""
            INSERT INTO {_qualified("akas")} (titleid, ordering, title, region, isoriginaltitle)
            SELECT t.titleid, t.ordering, t.title, t.region, t.isoriginaltitle
            FROM tmp_aka2 t
            WHERE EXISTS (SELECT 1 FROM {_qualified("title_basics")} b WHERE b.tconst = t.titleid)
            ON CONFLICT (titleid, ordering) DO NOTHING
        """)
        ins_aka += len(aka_rows)
        aka_rows.clear()
        conn.commit()

    if typ_rows:
        _stage(cur, "tmp_typ2", "titleid varchar(20), ordering int, type text")
        _stage_fill(cur, "tmp_typ2", typ_rows, page_size=2000)
        cur.execute(f"""
            INSERT INTO {_qualified("aka_types")} (titleid, ordering, type)
            SELECT t.titleid, t.ordering, t.type
            FROM tmp_typ2 t
            WHERE EXISTS (
              SELECT 1 FROM {_qualified("akas")} a
              WHERE a.titleid = t.titleid AND a.ordering = t.ordering
            )
            ON CONFLICT (titleid, ordering, type) DO NOTHING
        """)
        ins_typ += len(typ_rows)
        typ_rows.clear()
        conn.commit()

    if att_rows:
        _stage(cur, "tmp_att2", "titleid varchar(20), ordering int, attribute text")
        _stage_fill(cur, "tmp_att2", att_rows, page_size=2000)
        cur.execute(f"""
            INSERT INTO {_qualified("aka_attributes")} (titleid, ordering, attribute)
            SELECT t.titleid, t.ordering, t.attribute
            FROM tmp_att2 t
            WHERE EXISTS (
              SELECT 1 FROM {_qualified("akas")} a
              WHERE a.titleid = t.titleid AND a.ordering = t.ordering
            )
            ON CONFLICT (titleid, ordering, attribute) DO NOTHING
        """)
        ins_att += len(att_rows)
        att_rows.clear()
        conn.commit()

    return ins_aka, ins_typ, ins_att


# crew_directors / crew_writers -> filtrado por EXISTS en title_basics y name_basics
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
                _stage(cur, "tmp_cd", "tconst varchar(20), nconst varchar(20)")
                _stage_fill(cur, "tmp_cd", dir_rows, page_size=2000)
                cur.execute(f"""
                    INSERT INTO {_qualified("crew_directors")} (tconst, nconst)
                    SELECT t.tconst, t.nconst
                    FROM tmp_cd t
                    WHERE EXISTS (SELECT 1 FROM {_qualified("title_basics")} tb WHERE tb.tconst = t.tconst)
                      AND EXISTS (SELECT 1 FROM {_qualified("name_basics")} nb WHERE nb.nconst = t.nconst)
                    ON CONFLICT (tconst, nconst) DO NOTHING
                """)
                ins_dir += len(dir_rows)
                dir_rows.clear()
                conn.commit()

            if len(wri_rows) >= BATCH_MED:
                _stage(cur, "tmp_cw", "tconst varchar(20), nconst varchar(20)")
                _stage_fill(cur, "tmp_cw", wri_rows, page_size=2000)
                cur.execute(f"""
                    INSERT INTO {_qualified("crew_writers")} (tconst, nconst)
                    SELECT t.tconst, t.nconst
                    FROM tmp_cw t
                    WHERE EXISTS (SELECT 1 FROM {_qualified("title_basics")} tb WHERE tb.tconst = t.tconst)
                      AND EXISTS (SELECT 1 FROM {_qualified("name_basics")} nb WHERE nb.nconst = t.nconst)
                    ON CONFLICT (tconst, nconst) DO NOTHING
                """)
                ins_wri += len(wri_rows)
                wri_rows.clear()
                conn.commit()

    if dir_rows:
        _stage(cur, "tmp_cd2", "tconst varchar(20), nconst varchar(20)")
        _stage_fill(cur, "tmp_cd2", dir_rows, page_size=2000)
        cur.execute(f"""
            INSERT INTO {_qualified("crew_directors")} (tconst, nconst)
            SELECT t.tconst, t.nconst
            FROM tmp_cd2 t
            WHERE EXISTS (SELECT 1 FROM {_qualified("title_basics")} tb WHERE tb.tconst = t.tconst)
              AND EXISTS (SELECT 1 FROM {_qualified("name_basics")} nb WHERE nb.nconst = t.nconst)
            ON CONFLICT (tconst, nconst) DO NOTHING
        """)
        ins_dir += len(dir_rows)
        dir_rows.clear()
        conn.commit()

    if wri_rows:
        _stage(cur, "tmp_cw2", "tconst varchar(20), nconst varchar(20)")
        _stage_fill(cur, "tmp_cw2", wri_rows, page_size=2000)
        cur.execute(f"""
            INSERT INTO {_qualified("crew_writers")} (tconst, nconst)
            SELECT t.tconst, t.nconst
            FROM tmp_cw2 t
            WHERE EXISTS (SELECT 1 FROM {_qualified("title_basics")} tb WHERE tb.tconst = t.tconst)
              AND EXISTS (SELECT 1 FROM {_qualified("name_basics")} nb WHERE nb.nconst = t.nconst)
            ON CONFLICT (tconst, nconst) DO NOTHING
        """)
        ins_wri += len(wri_rows)
        wri_rows.clear()
        conn.commit()

    return ins_dir, ins_wri


# episodes -> filtrado por EXISTS en title_basics para tconst y parenttconst
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
                _stage(cur, "tmp_ep", "tconst varchar(20), parenttconst varchar(20), seasonnumber int, episodenumber int")
                _stage_fill(cur, "tmp_ep", ep_rows, page_size=1000)
                cur.execute(f"""
                    INSERT INTO {_qualified("episodes")} (tconst, parenttconst, seasonnumber, episodenumber)
                    SELECT t.tconst, t.parenttconst, t.seasonnumber, t.episodenumber
                    FROM tmp_ep t
                    WHERE EXISTS (SELECT 1 FROM {_qualified("title_basics")} b WHERE b.tconst = t.tconst)
                      AND (t.parenttconst IS NULL OR EXISTS (SELECT 1 FROM {_qualified("title_basics")} b2 WHERE b2.tconst = t.parenttconst))
                    ON CONFLICT (tconst) DO NOTHING
                """)
                ins_ep += len(ep_rows)
                ep_rows.clear()
                conn.commit()

    if ep_rows:
        _stage(cur, "tmp_ep2", "tconst varchar(20), parenttconst varchar(20), seasonnumber int, episodenumber int")
        _stage_fill(cur, "tmp_ep2", ep_rows, page_size=1000)
        cur.execute(f"""
            INSERT INTO {_qualified("episodes")} (tconst, parenttconst, seasonnumber, episodenumber)
            SELECT t.tconst, t.parenttconst, t.seasonnumber, t.episodenumber
            FROM tmp_ep2 t
            WHERE EXISTS (SELECT 1 FROM {_qualified("title_basics")} b WHERE b.tconst = t.tconst)
              AND (t.parenttconst IS NULL OR EXISTS (SELECT 1 FROM {_qualified("title_basics")} b2 WHERE b2.tconst = t.parenttconst))
            ON CONFLICT (tconst) DO NOTHING
        """)
        ins_ep += len(ep_rows)
        ep_rows.clear()
        conn.commit()

    return ins_ep


# principals -> filtrado por EXISTS en title_basics y name_basics.
# category es NOT NULL: si viene \N, se rellena '\N'.
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
                _none(row["category"]) or r"\N",
                _none(row.get("job")),
                _none(row.get("characters"))
            ))

            if len(pr_rows) >= BATCH_SMALL:
                _stage(cur, "tmp_pr", "tconst varchar(20), ordering int, nconst varchar(20), category varchar(64), job varchar(512), characters text")
                _stage_fill(cur, "tmp_pr", pr_rows, page_size=1000)
                cur.execute(f"""
                    INSERT INTO {_qualified("principals")} (tconst, ordering, nconst, category, job, characters)
                    SELECT t.tconst, t.ordering, t.nconst, t.category, t.job, t.characters
                    FROM tmp_pr t
                    WHERE EXISTS (SELECT 1 FROM {_qualified("title_basics")} tb WHERE tb.tconst = t.tconst)
                      AND EXISTS (SELECT 1 FROM {_qualified("name_basics")} nb WHERE nb.nconst = t.nconst)
                    ON CONFLICT (tconst, ordering) DO NOTHING
                """)
                ins_pr += len(pr_rows)
                pr_rows.clear()
                conn.commit()

    if pr_rows:
        _stage(cur, "tmp_pr2", "tconst varchar(20), ordering int, nconst varchar(20), category varchar(64), job varchar(512), characters text")
        _stage_fill(cur, "tmp_pr2", pr_rows, page_size=1000)
        cur.execute(f"""
            INSERT INTO {_qualified("principals")} (tconst, ordering, nconst, category, job, characters)
            SELECT t.tconst, t.ordering, t.nconst, t.category, t.job, t.characters
            FROM tmp_pr2 t
            WHERE EXISTS (SELECT 1 FROM {_qualified("title_basics")} tb WHERE tb.tconst = t.tconst)
              AND EXISTS (SELECT 1 FROM {_qualified("name_basics")} nb WHERE nb.nconst = t.nconst)
            ON CONFLICT (tconst, ordering) DO NOTHING
        """)
        ins_pr += len(pr_rows)
        pr_rows.clear()
        conn.commit()

    return ins_pr


# ratings -> filtrado por EXISTS en title_basics.
# NOT NULL: averagerating y numvotes -> defaults si vienen nulos.
def _load_title_ratings(cur, conn) -> int:
    path = os.path.join(BASE_DIR, "title.ratings.tsv")
    rt_rows = []
    ins_rt = 0

    with open(path, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f, delimiter="\t")
        for row in reader:
            avg = _to_float(row["averageRating"])
            votes = _to_int(row["numVotes"])
            rt_rows.append((
                row["tconst"],
                0.0 if avg is None else avg,
                0   if votes is None else votes
            ))

            if len(rt_rows) >= BATCH_MED:
                _stage(cur, "tmp_rt", "tconst varchar(20), averagerating numeric, numvotes int")
                _stage_fill(cur, "tmp_rt", rt_rows, page_size=2000)
                cur.execute(f"""
                    INSERT INTO {_qualified("ratings")} (tconst, averagerating, numvotes)
                    SELECT t.tconst, t.averagerating, t.numvotes
                    FROM tmp_rt t
                    WHERE EXISTS (SELECT 1 FROM {_qualified("title_basics")} b WHERE b.tconst = t.tconst)
                    ON CONFLICT (tconst) DO NOTHING
                """)
                ins_rt += len(rt_rows)
                rt_rows.clear()
                conn.commit()

    if rt_rows:
        _stage(cur, "tmp_rt2", "tconst varchar(20), averagerating numeric, numvotes int")
        _stage_fill(cur, "tmp_rt2", rt_rows, page_size=2000)
        cur.execute(f"""
            INSERT INTO {_qualified("ratings")} (tconst, averagerating, numvotes)
            SELECT t.tconst, t.averagerating, t.numvotes
            FROM tmp_rt2 t
            WHERE EXISTS (SELECT 1 FROM {_qualified("title_basics")} b WHERE b.tconst = t.tconst)
            ON CONFLICT (tconst) DO NOTHING
        """)
        ins_rt += len(rt_rows)
        rt_rows.clear()
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

            print("Cargando title_basics y basics_genres...") # los comentamos porque se supone que estos ya estan hechos completamnete
            #ins_tb, ins_bg = _load_title_basics_and_genres(cur, conn)
            #print(f"OK title_basics={ins_tb}, basics_genres={ins_bg}")

            print("Cargando name_basics, name_professions y name_known_for...")
            #ins_nb, ins_np, ins_nk = _load_name_basics_professions_known(cur, conn)
            #print(f"OK name_basics={ins_nb}, name_professions={ins_np}, name_known_for={ins_nk}")

            print("Cargando akas, aka_types y aka_attributes...")
            #ins_aka, ins_typ, ins_att = _load_title_akas_and_parts(cur, conn)
            #print(f"OK akas={ins_aka}, aka_types={ins_typ}, aka_attributes={ins_att}")

            print("Cargando crew_directors y crew_writers...")
            #ins_dir, ins_wri = _load_title_crew(cur, conn)
            #print(f"OK crew_directors={ins_dir}, crew_writers={ins_wri}")

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
            #f"title_basics: {ins_tb}, basics_genres: {ins_bg}, "
            #f"name_basics: {ins_nb}, name_professions: {ins_np}, name_known_for: {ins_nk}, "
            #f"akas: {ins_aka}, aka_types: {ins_typ}, aka_attributes: {ins_att}, "
            #f"crew_directors: {ins_dir}, crew_writers: {ins_wri}, "
            f"episodes: {ins_ep}, principals: {ins_pr}, ratings: {ins_rt}"
        )
        print("Resumen inserts (ON CONFLICT DO NOTHING + filtros EXISTS):")
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
