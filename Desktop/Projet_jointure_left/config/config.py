class AppConfig:
    def __init__(self) -> None:
        self.role_arn_datalake = "arn:aws:iam::503561446638:user/Farah"
        self.maag_master_agrem_input_path = "s3://projetjointureleft/maag_master_agrem/"
        self.maag_raty_linked_input_path = "s3://projetjointureleft/maag_raty_linked/"
        self.maag_repa_rrol_linked_input_path = "s3://projetjointureleft/maag_repa_rrol_linked/"
        self.reac_ref_act_type_input_path = "s3://projetjointureleft/reac_ref_act_type/"
        self.rtpa_ref_third_party_input_path = "s3://projetjointureleft/rtpa_ref_third_party/"
        self.maag_reac = "s3://projetjointureleft/output/maag_reac/maag_reac.parquet"
        self.maag_reac_repa = "s3://projetjointureleft/output/maag_reac_repa/maag_reac_repa.parquet"
        self.maag_reac_repa_raty = "s3://projetjointureleft/output/maag_reac_repa_raty/maag_reac_repa_raty.parquet"
        self.maag_reac_repa_raty_party = "s3://projetjointureleft/output/maag_reac_repa_raty_party/maag_reac_repa_raty_party.parquet"


app_config = AppConfig()


