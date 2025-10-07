# Esto es la documentacion sin formato


# Guía de Replicación PostgreSQL con Docker 
## ya usando mi docker file
## 1. Archivos de Configuración

### 1.1 `./pg_hba.conf`

```conf
# TYPE  DATABASE     USER         ADDRESS         METHOD
local   all          all                          trust
host    all          all          0.0.0.0/0       scram-sha-256
host    all          all          ::/0            scram-sha-256

# Permitir replicación desde la red docker al rol 'replicator'
host    replication  replicator   0.0.0.0/0       scram-sha-256
host    replication  replicator   ::/0            scram-sha-256
```

### 1.2 `./initdb/01_init_replication.sql`

```sql
-- rol para streaming replication
CREATE ROLE replicator WITH REPLICATION LOGIN PASSWORD 'replica_pass';
```

## 2. Levantar el Primario y Preparar el Slot

### 2.1 Iniciar el contenedor primario

```bash
docker compose up -d pg-bases2
```

### 2.2 Verificar parámetros

```bash
docker exec -it pg-bases2 psql -U postgres -d bases2_proyectos -c "SHOW wal_level;"
docker exec -it pg-bases2 psql -U postgres -d bases2_proyectos -c "SHOW max_wal_senders;"
docker exec -it pg-bases2 psql -U postgres -d bases2_proyectos -c "SHOW max_replication_slots;"
docker exec -it pg-bases2 psql -U postgres -d bases2_proyectos -c "\du+ replicator"
```

### 2.3 Crear el slot físico para la réplica

```bash
docker exec -it pg-bases2 psql -U postgres -d bases2_proyectos -c "SELECT pg_create_physical_replication_slot('replica1');"
```

## 3. Basebackup y Arranque de la Réplica

### 3.1 Realizar basebackup

```bash
docker compose run --rm -u postgres --entrypoint bash pg-replica -lc "
set -e
rm -rf /var/lib/postgresql/data/*
export PGPASSWORD='replica_pass'
pg_basebackup -h pg-bases2 -U replicator -D /var/lib/postgresql/data -X stream -R -S replica1 -v
echo \"primary_conninfo = 'host=pg-bases2 port=5432 user=replicator password=replica_pass application_name=replica1'\" >> /var/lib/postgresql/data/postgresql.auto.conf
"
```

### 3.2 Arrancar la réplica

```bash
docker compose up -d pg-replica
```

## 4. Verificación

### 4.1 Verificar que la réplica está en recuperación

```bash
docker exec -it pg-replica psql -U postgres -d postgres -c "SELECT pg_is_in_recovery();"
```

### 4.2 Verificar estado del wal receiver

```bash
docker exec -it pg-replica psql -U postgres -d postgres -c "SELECT status FROM pg_stat_wal_receiver;"
```

### 4.3 Verificar replicación en el primario

```bash
docker exec -it pg-bases2 psql -U postgres -d bases2_proyectos -c "SELECT application_name, state, sync_state FROM pg_stat_replication;"
```

### 4.4 Verificar slot activo

```bash
docker exec -it pg-bases2 psql -U postgres -d bases2_proyectos -c "SELECT slot_name, active FROM pg_replication_slots WHERE slot_name='replica1';"
```

## 5. Prueba de Replicación

### 5.1 Insertar datos en el primario

```bash
docker exec -it pg-bases2 psql -U postgres -d bases2_proyectos -c "CREATE TABLE IF NOT EXISTS public.replica_test(id serial primary key, val text);"
docker exec -it pg-bases2 psql -U postgres -d bases2_proyectos -c "INSERT INTO public.replica_test(val) VALUES ('hola'),('replica-ok');"
```

### 5.2 Verificar datos en la réplica

```bash
docker exec -it pg-replica psql -U postgres -d bases2_proyectos -c "TABLE public.replica_test;"
```

## 6. Failover y Failback

### 6.1 Failover (Promover la Réplica)

#### Detener el primario

```bash
docker stop pg-bases2
```

#### Promover la réplica

```bash
docker exec -it pg-replica psql -U postgres -d postgres -c "SELECT pg_promote(wait => true);"
docker exec -it pg-replica psql -U postgres -d postgres -c "SELECT pg_is_in_recovery();"  # debe ser 'f'
```

#### Verificar escritura en la nueva primaria

```bash
docker exec -it pg-replica psql -U postgres -d bases2_proyectos -c "INSERT INTO public.replica_test(val) VALUES ('failover-ok');"
```

### 6.2 Failback (Reintegrar el Viejo Primario como Standby)

#### Detener el viejo primario

```bash
docker stop pg-bases2
```

#### Vaciar el directorio de datos

```bash
docker compose run --rm -u postgres --entrypoint bash pg-bases2 -lc "rm -rf /var/lib/postgresql/data/*"
```

#### Crear slot en el nuevo primario

```bash
docker exec -it pg-replica psql -U postgres -d postgres -c "SELECT pg_create_physical_replication_slot('primary1');"
```

#### Re-clonar el viejo primario como standby

```bash
docker compose run --rm -u postgres --entrypoint bash pg-bases2 -lc "
  set -e
  export PGPASSWORD='replica_pass'
  pg_basebackup -h pg-replica -U replicator -D /var/lib/postgresql/data -X stream -R -S primary1 -v
  echo \"primary_conninfo = 'host=pg-replica port=5432 user=replicator password=replica_pass application_name=primary1'\" >> /var/lib/postgresql/data/postgresql.auto.conf
"
```

#### Arrancar el viejo primario como standby

```bash
docker compose up -d pg-bases2
```