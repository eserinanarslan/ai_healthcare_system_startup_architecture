# © Copyright 2020-21 Mayo Foundation For Medical Education and Research (MFMER). All rights reserved. V 1.0.0 
# This software is provided as-is, without warranty or representation for any use or purpose.
# Your use of it is subject to your agreement with Mayo.


def get_filter_string(filter_list:list, filter_term:str, condition:str)->str:
    """
        This function builds filter string for SQL statement
        Args: 
            filter_list: list of items that needs to be applied to SQL filter. e.g. ["heart failure", "cardiac filter"] are list of diagnosis
            filter_term: the column name or filter term. e.g. DIAGNOSIS_DESCRIPTION LIKE 
            condition:   OR, AND
        Output: DIAGNOSIS_DESCRIPTION LIKE '%heart failure%' OR DIAGNOSIS_DESCRIPTION LIKE '%cardiac failure%'
    
    """
    if filter_list:
        filter_string=""
        for index,item in enumerate(filter_list, start=0):
            if index==0:
                filter_string=f"{filter_term} '%{item.lower()}%'"
            else:
                filter_string=f"{filter_string} {condition} {filter_term} '%{item.lower()}%'"
        return filter_string
    else:
        filter_string=f"{filter_term} '%'"
        return filter_string

def get_filter_inclusion_exclusion(filter_list_1:list, filter_term_1:str, condition_1:str, filter_list_2:list, filter_term_2:str, condition_2:str)->str:
    """
        This function builds filter string for SQL statement
        Args: 
            filter_list_1: list of items that needs to be applied to SQL filter as inclusion condition. e.g. ["heart failure", "cardiac filter"] are list of diagnosis
            filter_term_1: the column name or filter term. e.g. DIAGNOSIS_DESCRIPTION LIKE 
            condition_1:   OR, AND - condition to combime inclusion criteria
            filter_list_2: list of items that needs to be applied to SQL filter as exclusion condition. e.g. ["cancer"] are list of diagnosis
            filter_term_2: the column name or filter term. e.g. DIAGNOSIS_DESCRIPTION NOT LIKE 
            condition_1:   OR, AND - condition to combime exclusion criteria
            
        Output: (DIAGNOSIS_DESCRIPTION LIKE '%heart failure%' OR DIAGNOSIS_DESCRIPTION LIKE '%cardiac failure%') AND (DIAGNOSIS_DESCRIPTION NOT LIKE '%cancer%')
    
    """
    if filter_list_1:
        if filter_list_2: 
            filter_str_1=get_filter_string(filter_list_1, filter_term_1, condition_1)
            filter_str_2=get_filter_string(filter_list_2, filter_term_2, condition_2)
            filter_string=f"({filter_str_1}) AND ({filter_str_2})"
            return filter_string
        else:
            filter_str_1=get_filter_string(filter_list_1, filter_term_1, condition_1)
            return filter_str_1
    else:
        if filter_list_2: 
            filter_str_2=get_filter_string(filter_list_2, filter_term_2, condition_2)
            return filter_str_2
        else:
            filter_string=f"{filter_term_1} '%'"
            return filter_string

