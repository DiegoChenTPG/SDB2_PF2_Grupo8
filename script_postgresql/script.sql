-- Opcional: trabajar en un schema propio
CREATE SCHEMA IF NOT EXISTS imdb;
SET search_path TO imdb, public;

-- ====================================================================
-- TITLE BASICS
-- ====================================================================
CREATE TABLE IF NOT EXISTS title_basics (
    tconst         varchar(20) PRIMARY KEY,
    titleType      varchar(64)   NOT NULL,
    primaryTitle   varchar(1024) NOT NULL,
    originalTitle  varchar(1024) NOT NULL,
    isAdult        boolean       NOT NULL,    -- true/false
    startYear      smallint      NULL,        -- YYYY
    endYear        smallint      NULL,        -- YYYY
    runtimeMinutes integer       NULL         -- NULL si "\N"
);

-- ====================================================================
-- BASICS GENRES
-- ====================================================================
CREATE TABLE IF NOT EXISTS basics_genres (
    tconst       varchar(20)   NOT NULL,
    primaryTitle varchar(1024) NOT NULL,
    genre        varchar(64)   NOT NULL,
    PRIMARY KEY (tconst, genre),
    FOREIGN KEY (tconst) REFERENCES title_basics(tconst)
);

-- ====================================================================
-- AKAS
-- ====================================================================
CREATE TABLE IF NOT EXISTS akas (
    titleId         varchar(20)   NOT NULL,
    ordering        integer       NOT NULL,
    title           varchar(1024) NOT NULL,
    region          varchar(64)   NULL,
    isOriginalTitle boolean       NOT NULL,
    PRIMARY KEY (titleId, ordering),
    FOREIGN KEY (titleId) REFERENCES title_basics(tconst)
);

-- ====================================================================
-- AKA TYPES
-- ====================================================================
CREATE TABLE IF NOT EXISTS aka_types (
    titleId  varchar(20) NOT NULL,
    ordering integer     NOT NULL,
    type     varchar(64) NOT NULL,
    PRIMARY KEY (titleId, ordering, type),
    FOREIGN KEY (titleId, ordering) REFERENCES akas(titleId, ordering)
);

-- ====================================================================
-- AKA ATTRIBUTES
-- ====================================================================
CREATE TABLE IF NOT EXISTS aka_attributes (
    titleId   varchar(20)  NOT NULL,
    ordering  integer      NOT NULL,
    attribute varchar(256) NOT NULL,
    PRIMARY KEY (titleId, ordering, attribute),
    FOREIGN KEY (titleId, ordering) REFERENCES akas(titleId, ordering)
);

-- ====================================================================
-- NAME BASICS
-- ====================================================================
CREATE TABLE IF NOT EXISTS name_basics (
    nconst      varchar(20)  PRIMARY KEY,
    primaryName varchar(512) NOT NULL,
    birthYear   smallint     NULL,   -- YYYY
    deathYear   smallint     NULL    -- YYYY
);

-- ====================================================================
-- CREW - DIRECTORS
-- ====================================================================
CREATE TABLE IF NOT EXISTS crew_directors (
    tconst varchar(20) NOT NULL,
    nconst varchar(20) NOT NULL,
    PRIMARY KEY (tconst, nconst),
    FOREIGN KEY (tconst) REFERENCES title_basics(tconst),
    FOREIGN KEY (nconst) REFERENCES name_basics(nconst)
);

-- ====================================================================
-- CREW - WRITERS
-- ====================================================================
CREATE TABLE IF NOT EXISTS crew_writers (
    tconst varchar(20) NOT NULL,
    nconst varchar(20) NOT NULL,
    PRIMARY KEY (tconst, nconst),
    FOREIGN KEY (tconst) REFERENCES title_basics(tconst),
    FOREIGN KEY (nconst) REFERENCES name_basics(nconst)
);

-- ====================================================================
-- EPISODES
-- ====================================================================
CREATE TABLE IF NOT EXISTS episodes (
    tconst        varchar(20) PRIMARY KEY,
    parentTconst  varchar(20) NOT NULL,
    seasonNumber  integer     NULL,
    episodeNumber integer     NULL,
    FOREIGN KEY (tconst)       REFERENCES title_basics(tconst),
    FOREIGN KEY (parentTconst) REFERENCES title_basics(tconst)
);

-- ====================================================================
-- PRINCIPALS
-- ====================================================================
CREATE TABLE IF NOT EXISTS principals (
    tconst    varchar(20) NOT NULL,
    ordering  integer     NOT NULL,
    nconst    varchar(20) NOT NULL,
    category  varchar(64) NOT NULL,
    job       varchar(512) NULL,
    characters text        NULL,
    PRIMARY KEY (tconst, ordering),
    FOREIGN KEY (tconst) REFERENCES title_basics(tconst),
    FOREIGN KEY (nconst) REFERENCES name_basics(nconst)
);

-- ====================================================================
-- RATINGS
-- ====================================================================
CREATE TABLE IF NOT EXISTS ratings (
    tconst        varchar(20)  PRIMARY KEY,
    averageRating numeric(3,1) NOT NULL,
    numVotes      integer      NOT NULL,
    FOREIGN KEY (tconst) REFERENCES title_basics(tconst)
);

-- ====================================================================
-- NAME PROFESSIONS
-- ====================================================================
CREATE TABLE IF NOT EXISTS name_professions (
    nconst     varchar(20) NOT NULL,
    profession varchar(64) NOT NULL,
    PRIMARY KEY (nconst, profession),
    FOREIGN KEY (nconst) REFERENCES name_basics(nconst)
);

-- ====================================================================
-- NAME KNOWN FOR
-- ====================================================================
CREATE TABLE IF NOT EXISTS name_known_for (
    nconst varchar(20) NOT NULL,
    tconst varchar(20) NOT NULL,
    PRIMARY KEY (nconst, tconst),
    FOREIGN KEY (nconst) REFERENCES name_basics(nconst),
    FOREIGN KEY (tconst) REFERENCES title_basics(tconst)
);

-- Recomendación de índices para acelerar FKs comunes en carga/consultas, revisa luego 
CREATE INDEX IF NOT EXISTS idx_bg_tconst        ON basics_genres(tconst);
CREATE INDEX IF NOT EXISTS idx_aka_titleId      ON akas(titleId);
CREATE INDEX IF NOT EXISTS idx_types_titleId    ON aka_types(titleId);
CREATE INDEX IF NOT EXISTS idx_attrs_titleId    ON aka_attributes(titleId);
CREATE INDEX IF NOT EXISTS idx_cd_tconst        ON crew_directors(tconst);
CREATE INDEX IF NOT EXISTS idx_cd_nconst        ON crew_directors(nconst);
CREATE INDEX IF NOT EXISTS idx_cw_tconst        ON crew_writers(tconst);
CREATE INDEX IF NOT EXISTS idx_cw_nconst        ON crew_writers(nconst);
CREATE INDEX IF NOT EXISTS idx_ep_parent        ON episodes(parentTconst);
CREATE INDEX IF NOT EXISTS idx_pr_tconst        ON principals(tconst);
CREATE INDEX IF NOT EXISTS idx_pr_nconst        ON principals(nconst);
CREATE INDEX IF NOT EXISTS idx_nf_tconst        ON name_known_for(tconst);
