# © Copyright 2020-21 Mayo Foundation For Medical Education and Research (MFMER). All rights reserved. V 1.0.0 
# This software is provided as-is, without warranty or representation for any use or purpose.
# Your use of it is subject to your agreement with Mayo.

import pandas as pd
import filter, query, config


def get_lab_result_for_cohort(patients: pd.DataFrame, inclusion: list=None, split: int=5000)->pd.DataFrame:
    """
        This function returns the dataframe of lab result from epic EHR for matching lab test description
        Args: 
            patients: dataframe that contains PATIENT_DK
            inclusion: list of lab test description
                
    """
    if inclusion:
        filter_string=filter.get_filter_string(inclusion, "LOWER(TEST.LAB_TEST_DESCRIPTION) LIKE", "OR")
    else:
        filter_string=None
        
    sql=f""" SELECT TEST.LAB_TEST_DESCRIPTION, LAB.*
             FROM `{config.project_id}.{config.dataset_id}.FACT_LAB_TEST` LAB
             INNER JOIN `{config.project_id}.{config.dataset_id}.DIM_LAB_TEST` TEST ON TEST.LAB_TEST_DK=LAB.LAB_TEST_DK AND TEST.ROW_SOURCE_ID=LAB.ROW_SOURCE_ID
             """
    if patients.empty:
        return pd.DataFrame()
    else:
        result=query.run_loop_query(patients, "PATIENT_DK","PATIENT_DK", split, sql,filter_string)                         
        return result
    
def get_lab_result_by_encounters(encounters: pd.DataFrame, key:str, inclusion: list=None, split: int=5000)->pd.DataFrame:
    """
        This function returns the dataframe of lab result from epic EHR for matching lab test description
        Args: 
            encounters: dataframe that contains encounters
            key: the column name of the df that will be used to filter the chunks e.g. APPOINTMENT_ID, EHR_ENCOUNTER_NUMBER
            search_key: column name of the queried table E.G. ENCOUNTER_NUMBER
            inclusion: list of lab test description
                
    """
    if inclusion:
        filter_string=filter.get_filter_string(inclusion, "LOWER(TEST.LAB_TEST_DESCRIPTION) LIKE", "OR")
    else:
        filter_string=None
        
    sql=f""" SELECT TEST.LAB_TEST_DESCRIPTION, LAB.*
             FROM `{config.project_id}.{config.dataset_id}.FACT_LAB_TEST` LAB
             INNER JOIN `{config.project_id}.{config.dataset_id}.DIM_LAB_TEST` TEST ON TEST.LAB_TEST_DK=LAB.LAB_TEST_DK AND TEST.ROW_SOURCE_ID=LAB.ROW_SOURCE_ID
             """
    if encounters.empty:
        return pd.DataFrame()
    else:
        result=query.run_loop_query(encounters, key,"ENCOUNTER_NUMBER", split, sql,filter_string)                         
        return result