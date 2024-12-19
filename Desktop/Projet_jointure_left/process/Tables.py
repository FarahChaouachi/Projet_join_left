# Ce fichier contient la class qui permet de faire notre ETL 

from datetime import datetime
from typing import Any
from pyspark.sql import DataFrame
from pyspark.sql import functions as f
from Projet_jointure_left.data.Data_Frame_Reader import *
from Projet_jointure_left.process.Traitement import jointure_left,change_nom
from Projet_jointure_left.config.config import app_config 
from Projet_jointure_left.context.context import spark 


class Tables:

    def __init__(self,app_config) -> None:
        
        #initialisation par les paths des données 
        self.maag_master_agrem_input_path: str = app_config.maag_master_agrem_input_path
        self.maag_raty_linked_input_path: str = app_config.maag_raty_linked_input_path
        self.reac_ref_act_type_input_path: str = app_config.reac_ref_act_type_input_path
        self.rtpa_ref_third_party_input_path: str = app_config.rtpa_ref_third_party_input_path
        self.maag_repa_rrol_linked_input_path: str = app_config.maag_repa_rrol_linked_input_path
        self.maag_master_agrem_df=None
        self.maag_raty_linked_df=None
        self.reac_ref_act_type_df=None
        self.rtpa_ref_third_party_df=None
        self.maag_repa_rrol_linked_df=None


    def run(self) -> None:
        print("coucou")
        #Transformation des des liens en data frame
        self.maag_master_agrem_df: DataFrame = self._get_data_from_parquet(
            self.maag_master_agrem_input_path
        )

        self.maag_raty_linked_df: DataFrame = self._get_data_from_parquet(
            self.maag_raty_linked_input_path
        )

        self.reac_ref_act_type_df: DataFrame = self._get_data_from_parquet(
            self.reac_ref_act_type_input_path
        )

        self.rtpa_ref_third_party_df: DataFrame = self._get_data_from_parquet(
            self.rtpa_ref_third_party_input_path
        )

        self.maag_repa_rrol_linked_df: DataFrame = self._get_data_from_parquet(
            self.maag_repa_rrol_linked_input_path 
        )

        # changement de la colonne event date qui est en commun entre la plus part des tables mais qui ne sera pas utiliser comme une jointure 
        self.maag_master_agrem_df=change_nom(self.maag_master_agrem_df,'eventdate','eventdate_maag_master_agrem_df')
        self.reac_ref_act_type_df=change_nom(self.reac_ref_act_type_df,'eventdate','eventdate_reac_ref_act_type_df')
        self.maag_repa_rrol_linked_df=change_nom(self.maag_repa_rrol_linked_df,'eventdate','eventdate_maag_repa_rrol_linked_df')
        self.rtpa_ref_third_party_df=change_nom(self.rtpa_ref_third_party_df,'eventdate','eventdate_rtpa_ref_third_party_df')
        self.maag_raty_linked_df==change_nom(self.maag_raty_linked_df,'eventdate','eventdate_maag_raty_linked_df')

        #2. Effectuez une jointure LEFT entre les tables `maag_master_agrem` et `reac_ref_act_type` en utilisant les colonnes `c_act_type` et `n_applic_infq` comme clés de jointure.
        DF2=jointure_left(self.maag_master_agrem_df, self.reac_ref_act_type_df, 'c_act_type', 'n_applic_infq', "maag_reac")

        #3. Effectuez une jointure LEFT entre le résultat précédent et la table `maag_repa_rrol_linked` en utilisant les colonnes `N_APPLIC_INFQ` et `C_MAST_AGREM_REFER` comme clés de jointure.
        DF3=jointure_left(DF2, self.maag_repa_rrol_linked_df, 'C_MAST_AGREM_REFER', 'N_APPLIC_INFQ',"maag_reac_repa")

        #4. Effectuez une jointure LEFT entre le résultat précédent et la table `maag_raty_linked` en utilisant la colonne `c_mast_agrem_refer` comme clé de jointure.
        # changement de la colonne N_APPLIC_INFQ_  qui est en commun entre les deux colonnes mais qui sera pas utiliser pour la jointure 
        DF3=change_nom(DF3,'N_APPLIC_INFQ','N_APPLIC_INFQ_maag_reac_repa')

        DF4=jointure_left(DF3, self.maag_raty_linked_df, 'c_mast_agrem_refer',None,'maag_reac_repa_raty')

        #5. Effectuez une jointure LEFT entre le résultat précédent et la table `rtpa_ref_third_party` en utilisant les colonnes `N_APPLIC_INFQ` et `C_THIR_PART_REFER` comme clés de jointure. (Notez que le champ `C_PART_REFER` est le même que le champ `C_THIR_PART_REFER`.)
        # changement de la colonne C_THIR_PART_REFER  qui est en commun entre les deux colonnes mais qui sera pas utiliser pour la jointure 
        self.rtpa_ref_third_party_df=change_nom(self.rtpa_ref_third_party_df,'C_THIR_PART_REFER','C_PART_REFER')
        DF5=jointure_left(DF4, self.rtpa_ref_third_party_df, 'C_PART_REFER','n_applic_infq','maag_reac_repa_raty_party')
        
        #6. Affichez les résultats finaux obtenus après les jointures.
        DF2.show()  
        DF3.show()  
        DF4.show() 
        DF5.show()  



    def _get_data_from_parquet(self,path)-> DataFrame:

        # converir le parquet en DF
        df_read=Data_Frame_Reader(path,spark = spark)
        df = df_read.read()
        df.show()
        df.printSchema()
        return (df)
    

TTable = Tables(app_config)

