-- títulos alternativos: puede exceder 1024
ALTER TABLE imdb.akas           ALTER COLUMN title          TYPE text;

-- por consistencia
ALTER TABLE imdb.basics_genres  ALTER COLUMN primarytitle   TYPE text;
ALTER TABLE imdb.title_basics   ALTER COLUMN primarytitle   TYPE text;
ALTER TABLE imdb.title_basics   ALTER COLUMN originaltitle  TYPE text;

-- atributos/tipos de AKA
ALTER TABLE imdb.aka_attributes ALTER COLUMN attribute      TYPE text;
ALTER TABLE imdb.aka_types      ALTER COLUMN type           TYPE text;
