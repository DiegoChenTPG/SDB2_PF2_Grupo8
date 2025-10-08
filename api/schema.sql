CREATE TABLE IF NOT EXISTS name_basics (
    nconst      varchar(20)  PRIMARY KEY,
    primaryName varchar(512) NOT NULL,
    birthYear   smallint     NULL,   -- YYYY
    deathYear   smallint     NULL    -- YYYY
);
