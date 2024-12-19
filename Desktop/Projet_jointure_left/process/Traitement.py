# Ce fichier permet de faire les transformations necessaire 

from Projet_jointure_left.common.writer import write_to_parquet
from Projet_jointure_left.config.config import app_config



def jointure_left(df1, df2, keysel=None, keysel_other=None, table='', app=app_config):
    """
    Effectue une jointure LEFT entre deux DataFrames avec une flexibilité pour gérer jusqu'à trois clés de jointure.
    
    :param df1: Premier DataFrame (source).
    :param df2: Deuxieme DataFrame (source).
    :keysel: La colonne de jointure numero 1 de DF1.
    :keysel_other: La colonne de jointure numero 2 de DF1.
    :keysel_other_other: La colonne de jointure numero 1 de DF2.
    :keysel_other_final: La colonne de jointure de numero 2 de DF2.

    
    :return: DataFrame résultant de la jointure LEFT.
    """
    
    if (keysel is not None) and (keysel_other is None) :
        # le cas ou on a une seule colonne de jointure 
        df_joined = df1.join(df2, on=keysel, how="left")

    elif(keysel is not None) and (keysel_other is not None) : 
        # le cas ou on a deux colonnes de jointure 
        df_joined = df1.join(df2, on=[keysel,keysel_other], how="left")

    sauvgarder(df_joined,f'{getattr(app_config, table)}')

    return df_joined


def change_nom(df,ancien,mot): 
    df=df.withColumnRenamed(ancien,mot)
    return(df)

def sauvgarder(df,path):
    write_to_parquet(df,path)

