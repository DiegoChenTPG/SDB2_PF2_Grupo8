-- === Contexto general del nodo ===
SELECT now() AS ts,
    inet_server_addr() AS server_ip,
    inet_server_port() AS server_port,
    inet_client_addr() AS client_ip,
    version();

-- Rol del nodo (primaria vs standby) y modo de transacción
SELECT pg_is_in_recovery() AS in_recovery;          -- f = primaria, t = standby
SHOW transaction_read_only;                         -- on = solo lectura efectivo en standby
SHOW default_transaction_read_only;                 -- preferencia por defecto (informativo)

-- Instancia/DB/usuario y parámetros de replicación útiles
SELECT current_database() AS db,
    current_user      AS usr,
    current_setting('synchronous_standby_names', true) AS synchronous_standby_names,
    current_setting('primary_conninfo', true)           AS primary_conninfo;

-- LSNs (en primaria y standby)
SELECT pg_current_wal_lsn()   AS current_wal_lsn;   -- válido siempre
SELECT pg_last_wal_replay_lsn() AS last_replay_lsn; -- NULL en primaria; valor en standby

-- Si el nodo es PRIMARIA: conexiones de replicación
SELECT application_name, state, sync_state, client_addr,
    sent_lsn, write_lsn, flush_lsn, replay_lsn
FROM pg_stat_replication
ORDER BY application_name;

-- Si el nodo es STANDBY: receptor de WAL
SELECT status, received_lsn, last_msg_send_time, last_msg_receipt_time, conninfo
FROM pg_stat_wal_receiver;

-- Slots de replicación (informativo)
SELECT slot_name, slot_type, active, restart_lsn
FROM pg_replication_slots
ORDER BY slot_name;

-- Comprobación de datos (ajusta a tus tablas/esquema)
SELECT
    (SELECT COUNT(*) FROM imdb.title_basics)     AS cnt_title_basics,
    SELECT COUNT(*) FROM imdb.ratings)          AS cnt_ratings,
    (SELECT COUNT(*) FROM imdb.basics_genres)    AS cnt_basics_genres;
