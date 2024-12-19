# Ce fichier contient la classe qui va permettre la lecture des données 

import os
from Projet_jointure_left.common.reader import read_from_parquet
from pyspark.sql import DataFrame


class Data_Frame_Reader:
    def __init__(self,path: str, df: DataFrame = None, spark= None):
        self.df=df
        self.spark = spark
        self.path = path
    def read(self):
        print(self.spark)


        try:
            # Use Spark to read all Parquet files in the S3 path
            self.df  = self.spark.read.parquet(self.path)  # Spark will automatically handle the files in the prefix
            self.df.show()
            # Opt   ionally, if you need to filter only parquet files, you can use .filter here
            # new_df = self.spark.read.parquet(f"s3://{bucket_name}/{prefix}/*.parquet")

          # Set self.df to the DataFrame containing the data

            print(f"Data successfully read from: {self.path}")
            return self.df
        except Exception as e:
            print(f"Error reading from S3 path {self.path}: {e}")
        