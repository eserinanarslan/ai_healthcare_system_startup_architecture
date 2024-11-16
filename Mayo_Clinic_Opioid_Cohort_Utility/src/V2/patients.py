# © Copyright 2020-21 Mayo Foundation For Medical Education and Research (MFMER). All rights reserved. V 1.0.0 
# This software is provided as-is, without warranty or representation for any use or purpose.
# Your use of it is subject to your agreement with Mayo.

import pandas as pd
import random
import filter, query, config


def get_patients_with_diagnosis(diagnosis_codes: pd.DataFrame)->pd.DataFrame:
    """
        This function returns the dataframe of patients that match the DIAGNOSIS_CODE_DK
        Args: 
            diagnosis_codes: dataframe that contains DIAGNOSIS_CODE_DK
                
    """
    sql=f""" SELECT DISTINCT PAT.*
             FROM `{config.project_id}.{config.dataset_id}.DIM_PATIENT` PAT
             INNER JOIN `{config.project_id}.{config.dataset_id}.FACT_DIAGNOSIS` DIA ON DIA.PATIENT_DK=PAT.PATIENT_DK
             """
    if diagnosis_codes.empty:
        return pd.DataFrame()
    else:
        result=query.run_loop_query(diagnosis_codes, "DIAGNOSIS_CODE_DK","DIA.DIAGNOSIS_CODE_DK", 10000, sql)                         
        return result
    
def get_patients_with_diagnosis_age_filter(diagnosis_codes: pd.DataFrame, min_age:int=0, max_age:int=150)->pd.DataFrame:
    """
        This function returns the dataframe of patients that match the DIAGNOSIS_CODE_DK
        Args: 
            diagnosis_codes: dataframe that contains DIAGNOSIS_CODE_DK
            min_age: minimum age
            max_age: maximum age
                
    """
    sql=f""" SELECT DISTINCT PAT.*
             FROM `{config.project_id}.{config.dataset_id}.DIM_PATIENT` PAT
             INNER JOIN `{config.project_id}.{config.dataset_id}.FACT_DIAGNOSIS` DIA ON DIA.PATIENT_DK=PAT.PATIENT_DK
             """
    filter_string = f""" CAST(REPLACE(DIA.PATIENT_AGE_AT_EVENT, '+', '') AS INT64) > {min_age}
                      AND CAST(REPLACE(DIA.PATIENT_AGE_AT_EVENT, '+', '') AS INT64) < {max_age}      
                    """
    
    if diagnosis_codes.empty:
        return pd.DataFrame()
    else:
        result=query.run_loop_query(diagnosis_codes, "DIAGNOSIS_CODE_DK","DIA.DIAGNOSIS_CODE_DK", 10000, sql, filter_string)                         
        return result

def get_patients_with_comorbidity(diagnosis_codes: pd.DataFrame, patients: pd.DataFrame, comorbidity:str=None)->pd.DataFrame:
    """
        This function returns the dataframe of patients that match the DIAGNOSIS_CODE_DK
        Args: 
            diagnosis_codes: dataframe that contains DIAGNOSIS_CODE_DK
            patients: dataframe that contains PATIENT_DK
            comorbidity: name of the comorbidity e.g. has_diabetes
                
    """
    sql=f""" SELECT DISTINCT PAT.*
             FROM `{config.project_id}.{config.dataset_id}.DIM_PATIENT` PAT
             INNER JOIN `{config.project_id}.{config.dataset_id}.FACT_DIAGNOSIS` DIA ON DIA.PATIENT_DK=PAT.PATIENT_DK
             """
    string="abcdefghijklmnopqrstuvwxyz"
    if not comorbidity:
        comorbidity="comorbidity_"+str(random.choice(string))
        
    if diagnosis_codes.empty:
        patients[comorbidity]='NULL'
        return patients
    else:
        result=query.run_loop_query(diagnosis_codes, "DIAGNOSIS_CODE_DK","DIA.DIAGNOSIS_CODE_DK", 10000, sql)
        result[comorbidity]=1
        patients_with_comorbidity=pd.merge(patients,result, how='left')
        patients_with_comorbidity[comorbidity].fillna(0, inplace=True)
        return patients_with_comorbidity