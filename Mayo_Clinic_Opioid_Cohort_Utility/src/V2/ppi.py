# © Copyright 2020-21 Mayo Foundation For Medical Education and Research (MFMER). All rights reserved. V 1.0.0 
# This software is provided as-is, without warranty or representation for any use or purpose.
# Your use of it is subject to your agreement with Mayo.

import pandas as pd
import filter, query, config

def get_ppi_for_cohort(patients: pd.DataFrame, split: int=5000)->pd.DataFrame:
    """
        This function returns the dataframe of med order for matching generic med name description
        Args: 
            patients: dataframe that contains PATIENT_DK
                
    """
        
    sql=f""" SELECT QUES.QUESTION_TEXT, ANS.*
            FROM `{config.project_id}.{config.dataset_id}.FACT_PPI_ANSWERS` ANS
            RIGHT JOIN `{config.project_id}.{config.dataset_id}.DIM_PPI_QUESTIONS` QUES on QUES.QUESTION_DK=ANS.QUESTION_DK  AND QUES.ROW_SOURCE_ID=ANS.ROW_SOURCE_ID
             """
    if patients.empty:
        return pd.DataFrame()
    else:
        result=query.run_loop_query(patients, "PATIENT_DK","PATIENT_DK", split, sql)                         
        return result
    
def get_ppi_for_cohort_by_form(patients: pd.DataFrame, form: list=None,  split: int=5000)->pd.DataFrame:
    """
        This function returns the dataframe of med order for matching generic med name description
        Args: 
            patients: dataframe that contains PATIENT_DK
            forms: list of form_dk
                
    """
    if form:
        filter_string=filter.get_filter_string(form, "LOWER(FORM.FORM_DK) LIKE", "OR")
    else:
        filter_string=None
        
    sql=f""" SELECT FORM.FORM_NAME, QUES.QUESTION_TEXT, ANS.*
            FROM `{config.project_id}.{config.dataset_id}.FACT_PPI_ANSWERS` ANS
            RIGHT JOIN `{config.project_id}.{config.dataset_id}.DIM_PPI_QUESTIONS` QUES on QUES.QUESTION_DK=ANS.QUESTION_DK AND QUES.ROW_SOURCE_ID=ANS.ROW_SOURCE_ID
            RIGHT JOIN `{config.project_id}.{config.dataset_id}.DIM_PPI_FORMS` FORM ON FORM.FORM_DK=ANS.FORM_DK AND FORM.ROW_SOURCE_ID=ANS.ROW_SOURCE_ID
             """
    if patients.empty:
        return pd.DataFrame()
    else:
        result=query.run_loop_query(patients, "PATIENT_DK","PATIENT_DK", split, sql, filter_string)                         
        return result

def get_ppi_form(inclusion: list=None)->pd.DataFrame:
    """
        This function returns the form_DK  that match the inclusion and exclusion criteria
        Args: 
            forms: list of form_dk
                
    """
    if form:
        filter_string=filter.get_filter_string(inclusion, "LOWER(FORM.FORM_NAME) LIKE", "OR")
    else:
        filter_string=None
        
    sql=f""" SELECT FORM.*
            FROM `{config.project_id}.{config.dataset_id}.DIM_PPI_FORMS` FORM
             """
    if patients.empty:
        return pd.DataFrame()
    else:
        result=query.run_query(sql, filter_string)                         
        return result

