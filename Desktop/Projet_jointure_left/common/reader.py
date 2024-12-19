# Ce fichier contient les fonctions commun pour la lecture des fichiers à utiliser 

from Projet_jointure_left.context.context import spark



def read_from_parquet(parquet_file_path: str):
    return spark.read.parquet(parquet_file_path)
