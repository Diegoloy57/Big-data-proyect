import time
import numpy as np
import dask.dataframe as dd
from dask.distributed import Client

DATASET = "/app/data/raw/Crime_Data_from_2020_to_Present.parquet"
SCHEDULER = "tcp://dask-scheduler:8786"
GRID_SIZE = 0.01

print("=" * 55)
print("BENCHMARK DASK - HOTSPOTS GEOESPACIALES")
print("=" * 55)

client = Client(SCHEDULER)

info = client.scheduler_info()
workers = len(info["workers"])

print(f"Workers conectados: {workers}")
print(f"Dataset: {DATASET}")

inicio = time.perf_counter()

df = dd.read_parquet(
    DATASET,
    columns=["LAT", "LON"]
)

df = df.repartition(npartitions=8)

# Misma limpieza aplicada antes de almacenar en MongoDB
df = df[
    df["LAT"].notnull()
    & df["LON"].notnull()
    & df["LAT"].between(-90, 90)
    & df["LON"].between(-180, 180)
    & ~((df["LAT"] == 0) & (df["LON"] == 0))
]

# Misma cuadrícula usada por Spark:
# floor(coordenada / 0.01) * 0.01
df = df.assign(
    grid_lat=(
        np.floor(df["LAT"] / GRID_SIZE) * GRID_SIZE
    ).round(4),

    grid_lon=(
        np.floor(df["LON"] / GRID_SIZE) * GRID_SIZE
    ).round(4)
)

resultado = (
    df.groupby(["grid_lat", "grid_lon"])
      .size()
      .compute()
)

fin = time.perf_counter()

tiempo = fin - inicio

print()
print("=" * 55)
print("RESULTADO")
print("=" * 55)
print(f"Workers Dask     : {workers}")
print(f"Celdas generadas : {len(resultado):,}")
print(f"Tiempo segundos  : {tiempo:.4f}")
print("=" * 55)

client.close()