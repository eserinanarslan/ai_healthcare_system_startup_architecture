# © Copyright 2020-21 Mayo Foundation For Medical Education and Research (MFMER). All rights reserved. V 1.0.0 
# This software is provided as-is, without warranty or representation for any use or purpose.
# Your use of it is subject to your agreement with Mayo.

import pandas as pd
import filter, query, config


    
def get_orders_by_encounters(encounters: pd.DataFrame, key:str, inclusion: list=None, exclusion: list=None, split: int=5000)->pd.DataFrame:
    """
        This function returns the dataframe of lab result from epic EHR for matching lab test description
        Args: 
            encounters: dataframe that contains encounters
            key: the column name of the df that will be used to filter the chunks e.g. APPOINTMENT_ID, EHR_ENCOUNTER_NUMBER
            inclusion: list of orders by type
            exclusion: list of orders by type that needs to be excluded
                
    """
    filter_string=filter.get_filter_inclusion_exclusion(inclusion, "ITEM.ORDER_TYPE LIKE", "OR", exclusion, "ITEM.ORDER_TYPE NOT LIKE", "OR")
        
    sql=f""" SELECT ITEM.ORDER_DESCRIPTION, ITEM.ORDER_TYPE, ORD.*
             FROM `{config.project_id}.{config.dataset_id}.FACT_ORDERS` ORD
             INNER JOIN `{config.project_id}.{config.dataset_id}.DIM_ORDER_ITEM` ITEM ON ITEM.ORDER_ITEM_DK=ORD.ORDER_ITEM_DK AND ITEM.ROW_SOURCE_ID=ORD.ROW_SOURCE_ID
             """
    if encounters.empty:
        return pd.DataFrame()
    else:
        result=query.run_loop_query(encounters, key,"ENCOUNTER_NUMBER", split, sql,filter_string)                         
        return result