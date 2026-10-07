from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count

MONGO_URI = "mongodb://mongo:27017"

spark = (
    SparkSession.builder
    .appName("CrimeDataProcessing")
    .master("spark://spark-master:7077")
    .config(
        "spark.mongodb.read.connection.uri",
        f"{MONGO_URI}/crime_db.crimes"
    )
    .config(
        "spark.mongodb.write.connection.uri",
        f"{MONGO_URI}/crime_db"
    )
    .getOrCreate()
)

print("=== SPARK INICIADO ===")

df = (
    spark.read
    .format("mongodb")
    .option("database", "crime_db")
    .option("collection", "crimes")
    .load()
)

print("Esquema leído desde MongoDB:")
df.printSchema()

total = df.count()

print(f"Total de registros leídos: {total:,}")

print("\nTop 10 tipos de crimen:")

crime_counts = (
    df.groupBy("crime_description")
    .agg(count("*").alias("total"))
    .orderBy(col("total").desc())
)

crime_counts.show(10, truncate=False)

spark.stop()