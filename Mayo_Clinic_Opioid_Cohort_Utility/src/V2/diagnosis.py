# © Copyright 2020-21 Mayo Foundation For Medical Education and Research (MFMER). All rights reserved. V 1.0.0 
# This software is provided as-is, without warranty or representation for any use or purpose.
# Your use of it is subject to your agreement with Mayo.

import pandas as pd
import filter, query, config

def get_diagnosis_code(inclusion: list=None, exclusion: list=None)->pd.DataFrame:
    """
        This function returns the DIAGNOSIS_CODE_DK  that match the inclusion and exclusion criteria
        Args: 
            inclusion: list of disease that needs to be included  e.g. ["heart failure", "cardiac filter"]
            exclusion: list of disease that needs to be excluded  e.g. ["without mention of heart failure"]
                
    """
    sql=f""" SELECT DISTINCT DIAGNOSIS_CODE_DK, DIAGNOSIS_CODE, DIAGNOSIS_DESCRIPTION FROM `{config.project_id}.{config.dataset_id}.DIM_DIAGNOSIS_CODE`"""

    result=pd.DataFrame()
    if inclusion:
        if exclusion: 
            filter_str_1=filter.get_filter_string(inclusion, "LOWER(DIAGNOSIS_DESCRIPTION) LIKE", "OR")
            filter_str_2=filter.get_filter_string(exclusion,  "LOWER(DIAGNOSIS_DESCRIPTION) LIKE", "OR")            
            result_1=query.run_query(sql, filter_str_1)
            result_2=query.run_query(sql, filter_str_2)
            temp_result=pd.merge(result_1,result_2, how="outer", indicator='ind')
            result=temp_result[(temp_result.ind=='left_only')] 
            result=result.drop(columns=['ind'], axis=1)
        else:
            filter_str_1=filter.get_filter_string(inclusion, "LOWER(DIAGNOSIS_DESCRIPTION) LIKE", "OR")
            result=query.run_query(sql, filter_str_1)            
    else:
        test=1
        if exclusion: 
            filter_str_2=filter.get_filter_string(exclusion,  "LOWER(DIAGNOSIS_DESCRIPTION) LIKE", "OR")            
            result_1=query.run_query(sql)
            result_2=query.run_query(sql, filter_str_2)
            temp_result=pd.merge(result_1,result_2, how="outer", indicator='ind')
            result=temp_result[(temp_result.ind=='left_only')] 
            result=result.drop(columns=['ind'], axis=1)
        else:
            result=query.run_query(sql)
                          
    return result


def get_diagnosis_code_with_patient_dk(inclusion: list = None, exclusion: list = None) -> pd.DataFrame:
    """
        This function returns the DIAGNOSIS_CODE_DK  that match the inclusion and exclusion criteria
        Args:
            inclusion: list of disease that needs to be included  e.g. ["heart failure", "cardiac filter"]
            exclusion: list of disease that needs to be excluded  e.g. ["without mention of heart failure"]

    """
    sql = f""" SELECT DISTINCT PATIENT_DK, DIAGNOSIS_CODE_DK FROM `{config.project_id}.{config.dataset_id}.FACT_DIAGNOSIS`"""

    result = pd.DataFrame()
    result1 = query.run_query_without_filer(sql)
    result = result1.drop(columns=['ind'], axis=1)
    return result

def get_diagnosis_code_by_icd(inclusion: list=None, exclusion: list=None)->pd.DataFrame:
    """
        This function returns the DIAGNOSIS_CODE_DK  that match the inclusion and exclusion criteria
        Args: 
            inclusion: list of icd that needs to be included  e.g. ["F11.20", "F11.23"]
            exclusion: list of icd that needs to be excluded  e.g. ["O24"]
                
    """
    sql=f""" SELECT DISTINCT DIAGNOSIS_CODE_DK, DIAGNOSIS_CODE, DIAGNOSIS_DESCRIPTION FROM `{config.project_id}.{config.dataset_id}.DIM_DIAGNOSIS_CODE`"""
    result=pd.DataFrame()
    if inclusion:
        if exclusion: 
            filter_str_1=filter.get_filter_string(inclusion, "LOWER(DIAGNOSIS_CODE) LIKE", "OR")
            filter_str_2=filter.get_filter_string(exclusion,  "LOWER(DIAGNOSIS_CODE) LIKE", "OR")            
            result_1=query.run_query(sql, filter_str_1)
            result_2=query.run_query(sql, filter_str_2)
            temp_result=pd.merge(result_1,result_2, how="outer", indicator='ind')
            result=temp_result[(temp_result.ind=='left_only')] 
            result=result.drop(columns=['ind'], axis=1)           
        else:
            filter_str_1=filter.get_filter_string(inclusion, "LOWER(DIAGNOSIS_CODE) LIKE", "OR")
            result=query.run_query(sql, filter_str_1)            
    else:
        test=1
        if exclusion: 
            filter_str_2=filter.get_filter_string(exclusion,  "LOWER(DIAGNOSIS_CODE) LIKE", "OR")            
            result_1=query.run_query(sql)
            result_2=query.run_query(sql, filter_str_2)
            temp_result=pd.merge(result_1,result_2, how="outer", indicator='ind')
            result=temp_result[(temp_result.ind=='left_only')] 
            result=result.drop(columns=['ind'], axis=1)
        else:
            result=query.run_query(sql)
                          
    return result