from pathlib import Path
import os
import subprocess
import sys


DATASET_SLUG = "utkarsh1093/crime-data-from-2020-to-nov2025"

DATA_DIR = Path("/app/data/raw")

DATASET_FILE = DATA_DIR / "Crime_Data_from_2020_to_Present.parquet"

FORCE_DOWNLOAD = (
    os.getenv("FORCE_DOWNLOAD", "false").lower()
    == "true"
)


def main():

    print("========================================")
    print("       DESCARGA DATASET KAGGLE")
    print("========================================")

    DATA_DIR.mkdir(
        parents=True,
        exist_ok=True
    )

    if DATASET_FILE.exists() and not FORCE_DOWNLOAD:

        size_mb = DATASET_FILE.stat().st_size / (1024 * 1024)

        print("Dataset ya disponible.")
        print(f"Archivo : {DATASET_FILE}")
        print(f"Tamaño  : {size_mb:.2f} MB")
        print("No se requiere nueva descarga.")

        return

    print(f"Dataset Kaggle: {DATASET_SLUG}")
    print("Iniciando descarga...")

    command = [
        sys.executable,
        "-m",
        "kaggle",
        "datasets",
        "download",
        "-d",
        DATASET_SLUG,
        "-p",
        str(DATA_DIR),
        "--unzip"
    ]

    try:

        subprocess.run(
            command,
            check=True
        )

    except subprocess.CalledProcessError as error:

        print("ERROR: no fue posible descargar el dataset.")
        print(
            "Verifique las credenciales de Kaggle."
        )

        raise SystemExit(
            error.returncode
        )

    if not DATASET_FILE.exists():

        print(
            "ERROR: Kaggle terminó la descarga, "
            "pero no se encontró el archivo esperado."
        )

        raise SystemExit(1)

    size_mb = DATASET_FILE.stat().st_size / (1024 * 1024)

    print("----------------------------------------")
    print("DESCARGA COMPLETADA")
    print(f"Archivo : {DATASET_FILE}")
    print(f"Tamaño  : {size_mb:.2f} MB")
    print("----------------------------------------")


if __name__ == "__main__":
    main()