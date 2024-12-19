# Ce fichier contient les fonctions commun pour l'écriture des fichiers parquet

from pyspark.sql import DataFrame

def write_to_parquet(df: DataFrame, output_file_path: str) -> str:
    df.write.mode("overwrite").option("header", "true").csv(output_file_path)
    return output_file_path
