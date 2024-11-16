# © Copyright 2020-21 Mayo Foundation For Medical Education and Research (MFMER). All rights reserved. V 1.0.0 
# This software is provided as-is, without warranty or representation for any use or purpose.
# Your use of it is subject to your agreement with Mayo.

import pandas as pd
import filter, query, config

def get_med_order_for_cohort(patients: pd.DataFrame, inclusion: list=None, split: int=5000)->pd.DataFrame:
    """
        This function returns the dataframe of med order for matching generic med name description
        Args: 
            patients: dataframe that contains PATIENT_DK
            inclusion: list of MEDICATION_GENERIC_NAME
                
    """
    if inclusion:
        filter_string=filter.get_filter_string(inclusion, "LOWER(ITEM.MEDICATION_GENERIC_NAME) LIKE", "OR")
    else:
        filter_string=None
        
    sql=f""" SELECT ORD.*
             FROM `{config.project_id}.{config.dataset_id}.FACT_ORDERS` ORD
             INNER JOIN `{config.project_id}.{config.dataset_id}.DIM_ORDER_ITEM` ITEM ON ITEM.ORDER_ITEM_DK=MED.ORDER_ITEM_DK AND ITEM.ROW_SOURCE_ID=MED.ROW_SOURCE_ID
             """
    if patients.empty:
        return pd.DataFrame()
    else:
        result=query.run_loop_query(patients, "PATIENT_DK","PATIENT_DK", split, sql,filter_string)                         
        return result
    
def get_controlled_med_order_for_cohort(patients: pd.DataFrame, controlled: list=None, controlled_class: list=None, split: int=5000)->pd.DataFrame:
    """
        This function returns the dataframe of med order for matching generic med name description
        Args: 
            patients: dataframe that contains PATIENT_DK
            controlled: Whether this is controlled substance
            controlled_class: list of controlled_class
                
    """
    filter_string=filter.get_filter_inclusion_exclusion(controlled, "LOWER(ITEM.CONTROLLED_MEDICATION_INDICATOR) LIKE", "OR", controlled_class, "ITEM.CONTROLLED_MEDICATION_CLASS_CODE LIKE", "OR")

        
    sql=f""" SELECT MED.*
             FROM `{config.project_id}.{config.dataset_id}.FACT_ORDERS` MED
             INNER JOIN `{config.project_id}.{config.dataset_id}.DIM_ORDER_ITEM` ITEM ON ITEM.ORDER_ITEM_DK=MED.ORDER_ITEM_DK AND ITEM.ROW_SOURCE_ID=MED.ROW_SOURCE_ID
             """
    if patients.empty:
        return pd.DataFrame()
    else:
        result=query.run_loop_query(patients, "PATIENT_DK","PATIENT_DK", split, sql, filter_string)                         
        return result

def get_med_administered_for_cohort(patients: pd.DataFrame, inclusion: list=None, split: int=5000)->pd.DataFrame:
    """
        This function returns the dataframe of med order for matching generic med name description
        Args: 
            patients: dataframe that contains PATIENT_DK
            inclusion: list of MED_GENERIC_NAME_DESCRIPTION
                
    """
    if inclusion:
        filter_string=filter.get_filter_string(inclusion, "LOWER(TEST.MED_GENERIC_NAME_DESCRIPTION) LIKE", "OR")
    else:
        filter_string=None
        
    sql=f""" SELECT MED.*
             FROM `{config.project_id}.{config.dataset_id}.FACT_MEDS_ADMINISTERED` MED
             INNER JOIN `{config.project_id}.{config.dataset_id}.DIM_MED_NAME` TEST ON TEST.MED_NAME_DK=MED.MED_NAME_DK AND TEST.ROW_SOURCE_ID=MED.ROW_SOURCE_ID
             """
    if patients.empty:
        return pd.DataFrame()
    else:
        result=query.run_loop_query(patients, "PATIENT_DK","PATIENT_DK", split, sql,filter_string)                         
        return result
    
def get_med_administered_by_encounters(encounters: pd.DataFrame, key:str, inclusion: list=None, split: int=5000)->pd.DataFrame:
    """
        This function returns the dataframe of lab result from epic EHR for matching lab test description
        Args: 
            encounters: dataframe that contains encounters
            key: the column name of the df that will be used to filter the chunks e.g. APPOINTMENT_ID, EHR_ENCOUNTER_NUMBER
            search_key: column name of the queried table E.G. ENCOUNTER_NUMBER
            inclusion: list of lab test description
                
    """
    if inclusion:
        filter_string=filter.get_filter_string(inclusion, "LOWER(LAB_TEST_DESCRIPTION) LIKE", "OR")
    else:
        filter_string=None
        
    sql=f""" SELECT LAB.*
             FROM `{config.project_id}.{config.dataset_id}.FACT_LAB_TEST` LAB
             INNER JOIN `{config.project_id}.{config.dataset_id}.DIM_LAB_TEST` TEST ON TEST.LAB_TEST_DK=LAB.LAB_TEST_DK AND TEST.ROW_SOURCE_ID=LAB.ROW_SOURCE_ID
             """
    if encounters.empty:
        return pd.DataFrame()
    else:
        result=query.run_loop_query(encounters, key,"ENCOUNTER_NUMBER", split, sql,filter_string)                         
        return result