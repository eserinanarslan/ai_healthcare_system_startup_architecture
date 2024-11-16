# © Copyright 2020-21 Mayo Foundation For Medical Education and Research (MFMER). All rights reserved. V 1.0.0 
# This software is provided as-is, without warranty or representation for any use or purpose.
# Your use of it is subject to your agreement with Mayo.

import pandas as pd
import filter, query, config


def get_epic_outpatient_encounters_for_cohort(patients: pd.DataFrame, inclusion: list=None, split: int=5000)->pd.DataFrame:
    """
        This function returns the dataframe of outpatient encounter from epic EHR for matching visit types
        Args: 
            patients: dataframe that contains PATIENT_DK
                
    """
    if inclusion:
        filter_string=filter.get_filter_string(inclusion, "LOWER(APPT.APPOINTMENT_DESCRIPTION) LIKE", "OR")
        filter_string=f"{filter_string}  AND APPT.SOURCE_SYSTEM_DK='47918'"
    else:
        filter_string=f"APPT.SOURCE_SYSTEM_DK='47918'"
        
    sql=f""" SELECT APPT.*
             FROM `{config.project_id}.{config.dataset_id}.FACT_APPOINTMENT` APPT
             """
    if patients.empty:
        return pd.DataFrame()
    else:
        result=query.run_loop_query(patients, "PATIENT_DK","PATIENT_DK", split, sql,filter_string)                         
        return result
    
def get_outpatient_encounters_for_cohort(patients: pd.DataFrame, inclusion: list=None, split: int=5000)->pd.DataFrame:
    """
        This function returns the dataframe of outpatient encounter from epic EHR for matching visit types
        Args: 
            patients: dataframe that contains PATIENT_DK
                
    """
    if inclusion:
        filter_string=filter.get_filter_string(inclusion, "LOWER(APPT.APPOINTMENT_DESCRIPTION) LIKE", "OR")
    else:
        filter_string=None
        
    sql=f""" SELECT APPT.*
             FROM `{config.project_id}.{config.dataset_id}.FACT_APPOINTMENT` APPT
             """
    if patients.empty:
        return pd.DataFrame()
    else:
        result=query.run_parallel_query(patients, "PATIENT_DK","PATIENT_DK", split, sql,filter_string)                         
        return result

def get_encounters_for_cohort(patients: pd.DataFrame, inclusion: list=None, split: int=5000)->pd.DataFrame:
    """
        This function returns the dataframe of outpatient encounter from epic EHR for matching ENCOUNTER_TYPE
        Args: 
            patients: dataframe that contains PATIENT_DK
                
    """
    if inclusion:
        filter_string=filter.get_filter_string(inclusion, "LOWER(ENC.ENCOUNTER_TYPE) LIKE", "OR")
    else:
        filter_string=None
        
    sql=f""" SELECT ENC.*
             FROM `{config.project_id}.{config.dataset_id}.FACT_ENCOUNTERS` ENC
             """
    if patients.empty:
        return pd.DataFrame()
    else:
        result=query.run_parallel_query(patients, "PATIENT_DK","PATIENT_DK", split, sql,filter_string)                         
        return result