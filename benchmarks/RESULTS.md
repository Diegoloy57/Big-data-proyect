# Benchmark Dask vs Spark

## Operación evaluada

Se utilizó el mismo dataset Parquet y la misma operación geoespacial en ambos motores:

1. Lectura de coordenadas LAT y LON.
2. Eliminación de coordenadas nulas, fuera de rango y valores (0,0).
3. División espacial en celdas de 0.01 grados.
4. Agrupación de registros por celda geográfica.
5. Conteo de registros por celda.

Todos los escenarios produjeron 1,279 celdas geográficas.

## Resultados

| Motor | Workers | Tiempo | Memoria pico aprox. | Celdas |
|---|---:|---:|---:|---:|
| Dask | 1 | 1.0417 s | 351.0 MiB | 1,279 |
| Dask | 2 | 0.9003 s | 520.4 MiB | 1,279 |
| Spark | 1 | 52.2201 s | 1.38 GiB | 1,279 |
| Spark | 2 | 40.1406 s | 2.06 GiB | 1,279 |

## Metodología

Para Dask se utilizó el dataset reparticionado en 8 particiones para permitir trabajo distribuido entre varios workers.

Los tiempos de Dask corresponden a la mediana de tres ejecuciones por configuración.

En Spark se utilizaron ejecutores de 512 MB y 1 core por worker.

La memoria se estimó mediante muestreo con `docker stats` durante la ejecución.

## Conclusión

Para este dataset y esta operación, Dask presentó menor tiempo de ejecución y menor consumo de memoria que Spark.

Dask redujo el tiempo aproximadamente 13.6% al pasar de uno a dos workers.

Spark redujo el tiempo aproximadamente 23.1% al pasar de uno a dos workers.

El resultado muestra que aumentar el número de workers puede mejorar el procesamiento, pero también incrementa el consumo de memoria. En cargas de tamaño moderado, el overhead de inicialización, planificación y coordinación de Spark puede representar una parte significativa del tiempo total.