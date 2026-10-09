import time

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, floor, count, round as spark_round
from pyspark.sql.types import StructType, StructField, DoubleType

DATASET = "/app/data/raw/Crime_Data_from_2020_to_Present.parquet"
GRID_SIZE = 0.01

print("=" * 55)
print("BENCHMARK SPARK - HOTSPOTS GEOESPACIALES")
print("=" * 55)

spark = (
    SparkSession.builder
    .appName("BenchmarkSparkHotspots")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

print(f"Dataset: {DATASET}")
print(f"Spark master: {spark.sparkContext.master}")

inicio = time.perf_counter()

schema = StructType([
    StructField("LAT", DoubleType(), True),
    StructField("LON", DoubleType(), True),
])

df = (
    spark.read
    .schema(schema)
    .parquet(DATASET)
    .select("LAT", "LON")
)

df = (
    df
    .filter(
        col("LAT").isNotNull()
        & col("LON").isNotNull()
        & col("LAT").between(-90, 90)
        & col("LON").between(-180, 180)
        & ~((col("LAT") == 0) & (col("LON") == 0))
    )
)

grid_df = (
    df
    .withColumn(
        "grid_lat",
        floor(col("LAT") / GRID_SIZE) * GRID_SIZE
    )
    .withColumn(
        "grid_lon",
        floor(col("LON") / GRID_SIZE) * GRID_SIZE
    )
)

resultado = (
    grid_df
    .groupBy("grid_lat", "grid_lon")
    .agg(count("*").alias("total_crimes"))
    .withColumn("grid_lat", spark_round(col("grid_lat"), 4))
    .withColumn("grid_lon", spark_round(col("grid_lon"), 4))
)

celdas = resultado.count()

fin = time.perf_counter()

print()
print("=" * 55)
print("RESULTADO")
print("=" * 55)
print(f"Celdas generadas : {celdas:,}")
print(f"Tiempo segundos  : {fin - inicio:.4f}")
print("=" * 55)

spark.stop()