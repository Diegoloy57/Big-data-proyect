import os
from pathlib import Path
from datetime import datetime, date, time

import dask.dataframe as dd
import numpy as np
import pandas as pd
import pyarrow.parquet as pq

from dask.distributed import Client
from pymongo import MongoClient


# ============================================================
# CONFIGURACIÓN GENERAL
# ============================================================

BASE_DIR = Path(__file__).resolve().parent.parent

DATASET_PATH = (
    BASE_DIR
    / "data"
    / "raw"
    / "Crime_Data_from_2020_to_Present.parquet"
)

# Dentro de Docker estos nombres corresponden a los servicios
# definidos en docker-compose.yml
DASK_SCHEDULER = os.getenv(
    "DASK_SCHEDULER_ADDRESS",
    "tcp://dask-scheduler:8786"
)

MONGO_URI = os.getenv(
    "MONGO_URI",
    "mongodb://mongo:27017"
)

DATABASE_NAME = "crime_db"
COLLECTION_NAME = "crimes"

# Cantidad de documentos enviados a MongoDB por operación
BATCH_SIZE = 5000

# Más particiones = bloques de trabajo más pequeños
# para reducir consumo de memoria por worker.
N_PARTITIONS = 16


# Columnas que realmente necesitamos almacenar.
# No cargamos las 28 columnas si no son necesarias.
COLUMNS_TO_LOAD = [
    "DR_NO",
    "DATE OCC",
    "TIME OCC",
    "AREA",
    "AREA NAME",
    "Crm Cd",
    "Crm Cd Desc",
    "Vict Age",
    "Vict Sex",
    "Premis Desc",
    "Weapon Desc",
    "Status Desc",
    "LOCATION",
    "LAT",
    "LON",
    "occ_year",
    "occ_month",
    "occ_day",
]


# ============================================================
# CONVERSIÓN DE TIPOS PARA MONGODB
# ============================================================

def to_python(value):
    """
    Convierte valores de Pandas/Numpy a tipos compatibles
    con BSON/MongoDB.
    """

    if value is None:
        return None

    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass

    # Timestamp de Pandas -> datetime de Python
    if isinstance(value, pd.Timestamp):
        return value.to_pydatetime()

    # datetime ya es compatible con MongoDB
    if isinstance(value, datetime):
        return value

    # datetime.date no es directamente compatible.
    # Se convierte a datetime a medianoche.
    if isinstance(value, date) and not isinstance(value, datetime):
        return datetime.combine(value, time.min)

    # datetime.time no es soportado directamente por BSON.
    # Lo guardamos como texto HH:MM:SS.
    if isinstance(value, time):
        return value.strftime("%H:%M:%S")

    # Tipos numpy -> tipos nativos de Python
    if isinstance(value, np.generic):
        return value.item()

    return value


# ============================================================
# TRANSFORMACIÓN A DOCUMENTO MONGODB + GEOJSON
# ============================================================

def create_document(row):
    """
    Convierte una fila del dataset en un documento MongoDB.

    GeoJSON exige el orden:
        [longitud, latitud]
    """

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

        # Punto GeoJSON
        "location": {
            "type": "Point",
            "coordinates": [lon, lat]
        }
    }


# ============================================================
# FUNCIÓN DE LIMPIEZA GEOESPACIAL
# ============================================================

def clean_coordinates(df):
    """
    Elimina coordenadas inválidas.

    Reglas:
    - LAT y LON no pueden ser nulos.
    - LAT debe estar entre -90 y 90.
    - LON debe estar entre -180 y 180.
    - Se elimina (0, 0), porque no representa
      una ubicación válida para los delitos de Los Ángeles.
    """

    return df[
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


# ============================================================
# PROCESO PRINCIPAL
# ============================================================

def main():

    print("========================================")
    print("   INGESTA DISTRIBUIDA DASK -> MONGODB")
    print("========================================")

    # --------------------------------------------------------
    # 1. VALIDAR DATASET
    # --------------------------------------------------------

    if not DATASET_PATH.exists():
        raise FileNotFoundError(
            f"No se encontró el dataset: {DATASET_PATH}"
        )

    print(f"\nDataset: {DATASET_PATH}")

    # --------------------------------------------------------
    # 2. CONECTAR AL CLUSTER DASK
    # --------------------------------------------------------

    print("\n1. Conectando al cluster Dask...")

    client = Client(DASK_SCHEDULER)

    print("Cluster Dask conectado correctamente.")
    print(f"Scheduler: {DASK_SCHEDULER}")

    scheduler_info = client.scheduler_info()

    workers = scheduler_info.get("workers", {})

    print(f"Workers conectados: {len(workers)}")

    if len(workers) < 2:
        print(
            "ADVERTENCIA: se esperaban al menos "
            "2 workers Dask."
        )

    # --------------------------------------------------------
    # 3. TOTAL ORIGINAL DESDE METADATOS PARQUET
    # --------------------------------------------------------

    print("\n2. Leyendo metadatos del dataset...")

    parquet_file = pq.ParquetFile(DATASET_PATH)

    total_original = parquet_file.metadata.num_rows

    print(f"Registros originales: {total_original:,}")

    # --------------------------------------------------------
    # 4. LEER SOLO COORDENADAS PARA VALIDACIÓN
    # --------------------------------------------------------

    print("\n3. Aplicando limpieza geoespacial...")

    # Primero cargamos solamente LAT y LON.
    # Esto evita mover todas las columnas por el cluster
    # únicamente para calcular cuántos registros son válidos.
    coords_df = dd.read_parquet(
        DATASET_PATH,
        columns=["LAT", "LON"]
    )

    coords_df = coords_df.repartition(
        npartitions=N_PARTITIONS
    )

    coords_clean = clean_coordinates(coords_df)

    total_clean = coords_clean.shape[0].compute()

    total_discarded = total_original - total_clean

    print(f"Particiones Dask: {N_PARTITIONS}")
    print(f"Registros válidos: {total_clean:,}")
    print(f"Registros descartados: {total_discarded:,}")

    # Ya no necesitamos este dataframe
    del coords_df
    del coords_clean

    # --------------------------------------------------------
    # 5. LEER COLUMNAS NECESARIAS
    # --------------------------------------------------------

    print("\n4. Preparando datos para la ingesta...")

    df = dd.read_parquet(
        DATASET_PATH,
        columns=COLUMNS_TO_LOAD
    )

    df = df.repartition(
        npartitions=N_PARTITIONS
    )

    df_clean = clean_coordinates(df)

    print(
        f"Dataset preparado en "
        f"{df_clean.npartitions} particiones."
    )

    # --------------------------------------------------------
    # 6. CONECTAR A MONGODB
    # --------------------------------------------------------

    print("\n5. Conectando con MongoDB...")

    mongo_client = MongoClient(
        MONGO_URI,
        serverSelectionTimeoutMS=10000
    )

    mongo_client.admin.command("ping")

    db = mongo_client[DATABASE_NAME]

    collection = db[COLLECTION_NAME]

    print("MongoDB conectado correctamente.")

    # --------------------------------------------------------
    # 7. LIMPIAR COLECCIÓN
    # --------------------------------------------------------

    print("\n6. Preparando colección...")

    collection.drop()

    print(
        f"Colección {DATABASE_NAME}."
        f"{COLLECTION_NAME} preparada."
    )

    # --------------------------------------------------------
    # 8. PROCESAR PARTICIONES
    # --------------------------------------------------------

    print("\n7. Cargando registros por lotes...")

    inserted = 0

    delayed_partitions = df_clean.to_delayed()

    total_partitions = len(delayed_partitions)

    for partition_number, delayed_partition in enumerate(
        delayed_partitions,
        start=1
    ):

        print(
            f"\nProcesando partición "
            f"{partition_number}/{total_partitions}"
        )

        # La computación de la partición es enviada
        # al cluster Dask.
        partition = delayed_partition.compute()

        documents = []

        for _, row in partition.iterrows():

            documents.append(
                create_document(row)
            )

            if len(documents) >= BATCH_SIZE:

                collection.insert_many(
                    documents,
                    ordered=False
                )

                inserted += len(documents)

                print(
                    f"Insertados: {inserted:,}",
                    end="\r",
                    flush=True
                )

                documents = []

        # Último lote de la partición
        if documents:

            collection.insert_many(
                documents,
                ordered=False
            )

            inserted += len(documents)

            print(
                f"Insertados: {inserted:,}",
                end="\r",
                flush=True
            )

        # Liberamos explícitamente la partición
        del partition
        del documents

    print(f"\n\nTotal insertado: {inserted:,}")

    # --------------------------------------------------------
    # 9. CREAR ÍNDICE GEOESPACIAL
    # --------------------------------------------------------

    print("\n8. Creando índice geoespacial 2dsphere...")

    index_name = collection.create_index(
        [("location", "2dsphere")]
    )

    print(f"Índice creado: {index_name}")

    # Índices adicionales útiles para Spark/API
    print("\nCreando índices auxiliares...")

    collection.create_index("area_name")
    collection.create_index("crime_description")
    collection.create_index("occ_year")
    collection.create_index("occ_month")

    print("Índices auxiliares creados.")

    # --------------------------------------------------------
    # 10. VALIDACIÓN FINAL
    # --------------------------------------------------------

    mongo_count = collection.count_documents({})

    print("\n========================================")
    print("             RESULTADO FINAL")
    print("========================================")

    print(f"Originales   : {total_original:,}")
    print(f"Válidos      : {total_clean:,}")
    print(f"Descartados  : {total_discarded:,}")
    print(f"MongoDB      : {mongo_count:,}")
    print(f"Particiones  : {N_PARTITIONS}")
    print(f"Workers Dask : {len(workers)}")
    print(f"Índice       : {index_name}")

    if mongo_count == total_clean:
        print("\nESTADO: INGESTA CORRECTA")
    else:
        print("\nADVERTENCIA:")
        print(
            "La cantidad almacenada en MongoDB "
            "no coincide con los registros válidos."
        )

    print("========================================")

    # --------------------------------------------------------
    # 11. CERRAR CONEXIONES
    # --------------------------------------------------------

    mongo_client.close()
    client.close()


# ============================================================
# ENTRYPOINT
# ============================================================

if __name__ == "__main__":
    main()