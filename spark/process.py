from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    count,
    floor,
    round as spark_round
)


# ============================================================
# CONFIGURACIÓN
# ============================================================

MONGO_URI = "mongodb://mongo:27017"

DATABASE_NAME = "crime_db"
SOURCE_COLLECTION = "crimes"

MONTHLY_COLLECTION = "crime_monthly_area_stats"
GRID_COLLECTION = "crime_hotspots_grid"


# ============================================================
# CREAR SPARK SESSION
# ============================================================

spark = (
    SparkSession.builder
    .appName("CrimeDataProcessing")
    .master("spark://spark-master:7077")
    .config(
        "spark.mongodb.read.connection.uri",
        f"{MONGO_URI}/{DATABASE_NAME}.{SOURCE_COLLECTION}"
    )
    .config(
        "spark.mongodb.write.connection.uri",
        f"{MONGO_URI}/{DATABASE_NAME}"
    )
    .getOrCreate()
)

# Reducimos ruido en consola
spark.sparkContext.setLogLevel("WARN")


print("========================================")
print("     PROCESAMIENTO DISTRIBUIDO SPARK")
print("========================================")


# ============================================================
# 1. LEER DESDE MONGODB
# ============================================================

print("\n1. Leyendo datos desde MongoDB...")

df = (
    spark.read
    .format("mongodb")
    .option("database", DATABASE_NAME)
    .option("collection", SOURCE_COLLECTION)
    .load()
)

total = df.count()

print(f"Registros leídos: {total:,}")


# ============================================================
# 2. AGREGACIÓN TEMPORAL
#    DELITOS POR AÑO, MES Y ÁREA
# ============================================================

print("\n2. Calculando agregación temporal...")

monthly_stats = (
    df
    .filter(
        col("occ_year").isNotNull()
        & col("occ_month").isNotNull()
        & col("area_name").isNotNull()
    )
    .groupBy(
        "occ_year",
        "occ_month",
        "area_name"
    )
    .agg(
        count("*").alias("total_crimes")
    )
    .orderBy(
        "occ_year",
        "occ_month",
        col("total_crimes").desc()
    )
)

print("\nMuestra de agregación temporal:")

monthly_stats.show(
    20,
    truncate=False
)


# ============================================================
# 3. GUARDAR AGREGACIÓN TEMPORAL EN MONGODB
# ============================================================

print(
    f"\n3. Guardando resultados en "
    f"{DATABASE_NAME}.{MONTHLY_COLLECTION}..."
)

(
    monthly_stats
    .write
    .format("mongodb")
    .mode("overwrite")
    .option("database", DATABASE_NAME)
    .option("collection", MONTHLY_COLLECTION)
    .save()
)

print("Agregación temporal guardada correctamente.")


# ============================================================
# 4. AGREGACIÓN ESPACIAL POR CUADRÍCULA
# ============================================================

print("\n4. Calculando hotspots por cuadrícula...")

# Tamaño aproximado de celda:
# 0.01 grados ≈ 1 km en latitud.
#
# La idea es agrupar delitos cercanos dentro
# de una misma celda geográfica.
GRID_SIZE = 0.01

grid_df = (
    df
    .filter(
        col("latitude").isNotNull()
        & col("longitude").isNotNull()
    )
    .withColumn(
        "grid_lat",
        floor(
            col("latitude") / GRID_SIZE
        ) * GRID_SIZE
    )
    .withColumn(
        "grid_lon",
        floor(
            col("longitude") / GRID_SIZE
        ) * GRID_SIZE
    )
)


grid_stats = (
    grid_df
    .groupBy(
        "grid_lat",
        "grid_lon"
    )
    .agg(
        count("*").alias("total_crimes")
    )
    .withColumn(
        "grid_lat",
        spark_round(
            col("grid_lat"),
            4
        )
    )
    .withColumn(
        "grid_lon",
        spark_round(
            col("grid_lon"),
            4
        )
    )
    .orderBy(
        col("total_crimes").desc()
    )
)

print("\nTop hotspots:")

grid_stats.show(
    20,
    truncate=False
)


# ============================================================
# 5. GUARDAR HOTSPOTS EN MONGODB
# ============================================================

print(
    f"\n5. Guardando hotspots en "
    f"{DATABASE_NAME}.{GRID_COLLECTION}..."
)

(
    grid_stats
    .write
    .format("mongodb")
    .mode("overwrite")
    .option("database", DATABASE_NAME)
    .option("collection", GRID_COLLECTION)
    .save()
)

print("Hotspots guardados correctamente.")


# ============================================================
# 6. VALIDACIÓN
# ============================================================

monthly_count = monthly_stats.count()
grid_count = grid_stats.count()

print("\n========================================")
print("            RESULTADO FINAL")
print("========================================")

print(f"Registros fuente       : {total:,}")
print(f"Filas temporal         : {monthly_count:,}")
print(f"Celdas geográficas     : {grid_count:,}")

print(
    f"Colección temporal     : "
    f"{DATABASE_NAME}.{MONTHLY_COLLECTION}"
)

print(
    f"Colección espacial     : "
    f"{DATABASE_NAME}.{GRID_COLLECTION}"
)

print("========================================")


spark.stop()