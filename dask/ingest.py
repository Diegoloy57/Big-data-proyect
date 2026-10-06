import dask.dataframe as dd

DATASET_PATH = "data/raw/Crime_Data_from_2020_to_Present.parquet"

def main ():
    print ("=== INICIO PROCESO DASK ===")

    #Leer archivo Parquet con Dask 
    df = dd.read_parquet(DATASET_PATH)

    print (f"Particiones Dask: {df.npartitions}")

    total = df.shape [0].compute()
    print (f"Registros originales: {total}")

    #Validar coordendas

    df_clean = df [
        df["LAT"].notnull()
        & df ["LON"].notnull()
        & (df["LAT"] >=-90)
        & (df["LAT"] <=90)
        & (df["LON"] >= -180)
        & (df["LON"] <= 180)
         & ~((df["LAT"] == 0) & (df["LON"] == 0))
    ]

    total_clean = df_clean.shape[0].compute()

    print(f"Registors validos : {total_clean}")
    print (f"Registros descartados: {total - total_clean}")

    print ("=== FIN DEL PROCESO DASK ===")

if __name__ == "__main__":
    main()


