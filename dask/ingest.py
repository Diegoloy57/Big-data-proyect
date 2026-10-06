from pathlib import Path
from datetime import time

import dask.dataframe as dd
import numpy as np
import pandas as pd
from pymongo import MongoClient


# --------------------------------------------------
# CONFIGURACIÓN
# --------------------------------------------------

BASE_DIR = Path(__file__).resolve().parent.parent

DATASET_PATH = (
    BASE_DIR
    / "data"
    / "raw"
    / "Crime_Data_from_2020_to_Present.parquet"
)

MONGO_URI = "mongodb://localhost:27017"
DATABASE_NAME = "crime_db"
COLLECTION_NAME = "crimes"

BATCH_SIZE = 5000


# --------------------------------------------------
# CONVERSIÓN DE TIPOS
# --------------------------------------------------

def to_python(value):
    """
    Convierte valores de Pandas/Numpy a tipos compatibles con MongoDB.
    """

    if pd.isna(value):
        return None

    if isinstance(value, pd.Timestamp):
        return value.to_pydatetime()

    if isinstance(value, time):
        return value.strftime("%H:%M:%S")

    if isinstance(value, np.generic):
        return value.item()

    return value

# --------------------------------------------------
# TRANSFORMACIÓN A DOCUMENTO MONGODB + GEOJSON
# --------------------------------------------------

def create_document(row):
    lat = float(row["LAT"])
    lon = float(row["LON"])

    return {
        "dr_no": to_python(row["DR_NO"]),
        "date_occ": to_python(row["DATE OCC"]),
        "time_occ": to_python(row["TIME OCC"]),

        "area": to_python(row["AREA"]),
        "area_name": to_python(row["AREA NAME"]),

        "crime_code": to_python(row["Crm Cd"]),
        "crime_description": to_python(row["Crm Cd Desc"]),

        "victim_age": to_python(row["Vict Age"]),
        "victim_sex": to_python(row["Vict Sex"]),

        "premise_description": to_python(row["Premis Desc"]),
        "weapon_description": to_python(row["Weapon Desc"]),

        "status_description": to_python(row["Status Desc"]),
        "address": to_python(row["LOCATION"]),

        "latitude": lat,
        "longitude": lon,

        "occ_year": to_python(row["occ_year"]),
        "occ_month": to_python(row["occ_month"]),
        "occ_day": to_python(row["occ_day"]),

        # GEOJSON
        # IMPORTANTE: primero longitud y luego latitud
        "location": {
            "type": "Point",
            "coordinates": [lon, lat]
        }
    }


# --------------------------------------------------
# PROCESO PRINCIPAL
# --------------------------------------------------

def main():

    print("========================================")
    print("   INGESTA DASK -> MONGODB")
    print("========================================")

    print("\n1. Leyendo dataset con Dask...")

    df = dd.read_parquet(
        DATASET_PATH,
        split_row_groups="adaptive",
        blocksize="64MB"
    )

    df = df.repartition(npartitions=8)

    print(f"Particiones detectadas: {df.npartitions}")

    total_original = df.shape[0].compute()

    print(f"Registros originales: {total_original:,}")

    # --------------------------------------------------
    # LIMPIEZA GEOESPACIAL
    # --------------------------------------------------

    print("\n2. Aplicando limpieza geoespacial...")

    df_clean = df[
        df["LAT"].notnull()
        & df["LON"].notnull()

        & (df["LAT"] >= -90)
        & (df["LAT"] <= 90)

        & (df["LON"] >= -180)
        & (df["LON"] <= 180)

        & ~(
            (df["LAT"] == 0)
            & (df["LON"] == 0)
        )
    ]

    total_clean = df_clean.shape[0].compute()

    print(f"Registros válidos: {total_clean:,}")
    print(
        f"Registros descartados: "
        f"{total_original - total_clean:,}"
    )

    # --------------------------------------------------
    # CONEXIÓN MONGODB
    # --------------------------------------------------

    print("\n3. Conectando con MongoDB...")

    client = MongoClient(MONGO_URI)

    db = client[DATABASE_NAME]

    collection = db[COLLECTION_NAME]

    # Comprobar conexión
    client.admin.command("ping")

    print("MongoDB conectado correctamente.")

    # --------------------------------------------------
    # LIMPIAR COLECCIÓN PARA EVITAR DUPLICADOS
    # --------------------------------------------------

    print("\n4. Preparando colección...")

    collection.drop()

    print("Colección limpia.")

    # --------------------------------------------------
    # PROCESAR PARTICIONES
    # --------------------------------------------------

    print("\n5. Cargando registros por lotes...")

    inserted = 0

    partitions = df_clean.to_delayed()

    total_partitions = len(partitions)

    for partition_number, delayed_partition in enumerate(
        partitions,
        start=1
    ):

        print(
            f"\nProcesando partición "
            f"{partition_number}/{total_partitions}"
        )

        partition = delayed_partition.compute()

        documents = []

        for _, row in partition.iterrows():

            document = create_document(row)

            documents.append(document)

            if len(documents) >= BATCH_SIZE:

                collection.insert_many(
                    documents,
                    ordered=False
                )

                inserted += len(documents)

                print(
                    f"Insertados: {inserted:,}",
                    end="\r"
                )

                documents = []

        # Insertar registros sobrantes
        if documents:

            collection.insert_many(
                documents,
                ordered=False
            )

            inserted += len(documents)

    print(f"\n\nTotal insertado: {inserted:,}")

    # --------------------------------------------------
    # ÍNDICE GEOESPACIAL
    # --------------------------------------------------

    print("\n6. Creando índice 2dsphere...")

    index_name = collection.create_index(
        [("location", "2dsphere")]
    )

    print(f"Índice creado: {index_name}")

    # --------------------------------------------------
    # VALIDACIÓN
    # --------------------------------------------------

    mongo_count = collection.count_documents({})

    print("\n========================================")
    print("          RESULTADO FINAL")
    print("========================================")

    print(f"Originales : {total_original:,}")
    print(f"Válidos    : {total_clean:,}")
    print(f"MongoDB    : {mongo_count:,}")

    print("========================================")

    client.close()


if __name__ == "__main__":
    main()
