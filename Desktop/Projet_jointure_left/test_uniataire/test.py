#test unitaire pour les lectures et ecritures

import os
import sys
import pytest
from pyspark.sql import DataFrame, SparkSession
from common.reader import read_from_parquet
from common.writer import write_to_parquet


# Déterminer le répertoire actuel du scriptos
current_dir = os.path.dirname(os.path.realpath(__file__))

# Ajouter dans le reptoire parent 
parent_dir = os.path.abspath(os.path.join(current_dir, "../.."))
sys.path.insert(0, parent_dir)


@pytest.fixture(scope="session")
def spark():
    spark_session = SparkSession.builder.appName("pytest-pyspark").getOrCreate()
    yield spark_session
    spark_session.stop()

# test pour la lecture d'un parquet
def test_read_from_parquet(spark):
    test_data_path = "C:/Users/fchaouachi/Desktop/tables/maag_master_agrem"
    df = read_from_parquet(test_data_path)

    assert df is not None
    assert isinstance(df, DataFrame)
    assert df.count() > 0


#test pour la lecture d'un parquet
def test_write_to_parquet():
    test_data_path = "C:/Users/fchaouachi/Desktop/tables/maag_master_agrem"
    df = read_from_parquet(test_data_path)
    output_file_path = "C:/Users/fchaouachi/Desktop/Projet_jointure_left/test_uniataire/resultat_test"
    result_path = write_to_parquet(df, output_file_path)

    assert os.path.exists(result_path)
    assert result_path == output_file_path

 

