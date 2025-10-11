# Capturas de backups, hay las ordenas dukin

Validacion de que la api esta funcionando correctamente y conectada a la DB
![alt text](images/api_conectada_correctamente.png)

Ejecucion del script de automatizacion de los backups
![alt text](images/script_corriendo.png)


docker exec -u postgres pg-bases2 pgbackrest --stanza=bases2-db info

## Primer Backup (Dia 1 - Completo)
En este caso se hizo un completo sin ningun cambio, osea el apartado de locust esta apagado, cosa que no manda nuevos datos

Informacion de Redis
![alt text](images/dia1.png)

Informacion dentro del contenedor
![alt text](images/dia1d.png)

## Segundo Backup (Dia 2 - Incremental)
![alt text](images/locust.png)
![alt text](images/locust_encendido.png)
![alt text](images/api_datos.png)
En este caso ya encendemos Locust para generar datos, buscando que ya vean diferencias en los backups, a partir de aca Locust siempre estara encendido mandando datos con los mismos parametros, los siguientes backups seran asi.

Informacion de Redis
![alt text](images/dia2.png)

Informacion dentro del contenedor
![alt text](images/dia2d.png)


## Tercer Backup (Dia 3 - Incremental, Diferencial)

Informacion de Redis
![alt text](images/dia3.png)

Informacion dentro del contenedor
![alt text](images/dia3d.png)

## Cuarto Backup (Dia 4 - Incremental)

Informacion de Redis
![alt text](images/dia4.png)

Informacion dentro del contenedor
![alt text](images/dia4d.png)

## Quinto Backup (Dia 5 - Incremental, Diferencial)

Informacion de Redis
![alt text](images/dia5.png)

Informacion dentro del contenedor
![alt text](images/dia5d.png)

## Sexto Backup (Dia 6 - Diferencial, Completo)
En este caso aqui, finalmente si se apaga el locust al momento de hacer el ultimo completo

Informacion de Redis
![alt text](images/dia6.png)

Informacion dentro del contenedor
![alt text](images/dia6d.png)



### Script finalizado
![alt text](images/script_final.png)