# © Copyright 2020-21 Mayo Foundation For Medical Education and Research (MFMER). All rights reserved. V 1.0.0 
# This software is provided as-is, without warranty or representation for any use or purpose.
# Your use of it is subject to your agreement with Mayo.


from google.cloud import bigquery
from google.cloud import storage
import pandas as pd
import numpy as np
from datetime import datetime
from datetime import date
import threading

client=bigquery.Client()

#variable to store results
responses={}
responses_lock=threading.Lock()
#variable to store thread pool
pool = []

def run_query(sql:str, filter_string:str=None)->pd.DataFrame:
    """
        This function builds runs the query
        Args: 
            sql: sql statement
            filter_string: filter that needs to be applied to sql e.g. DIAGNOSIS_DESCRIPTION LIKE '%heart failure%' OR DIAGNOSIS_DESCRIPTION LIKE '%cardiac failure%'
                
    """
    if filter_string:
        SQL =f"{sql} WHERE {filter_string}"
        result=client.query(SQL).result().to_dataframe()
        return result
    else:
        SQL =f"{sql}"
        result=client.query(SQL).result().to_dataframe()
        return result
   
    
def run_loop_query(df:pd.DataFrame, key:str,search_key:str, split:int, sql:str, filter_string:str=None)->pd.DataFrame:
    """
        This query splits large patient cohort into manageable chhunks and run sql command for the chunks. I combines the result of all sql command and returns concat dataframe
        Args: 
            df: a dataframe containing key e.g. a patient dataframe with PATIENT_DK columns
            key: the column name of the df that will be used to filter the chunks e.g. PATIENT_DK
            search_key: column name of the queried table
            split: (integer) the chunk size
            sql: sql statement
            filter_string: filter that needs to be applied to sql e.g. DIAGNOSIS_DESCRIPTION LIKE '%heart failure%' OR DIAGNOSIS_DESCRIPTION LIKE '%cardiac failure%'           
    
    """
    x=df.shape[0]
    n=x//split
    if n==0:
        n=1
    df_split=np.array_split(df, n)
    results=pd.DataFrame()
    for x in range(0,n):
        tdf=df_split[x]
        filter_list=tdf[key].values.tolist()
        filter_key=str(filter_list)[1:-1]
        
        if filter_string:
            SQL =f"{sql} WHERE {search_key} IN ({filter_key}) AND {filter_string}"            
        else:
            SQL =f"{sql} WHERE {search_key} IN ({filter_key}) "
            
        temp_result=client.query(SQL).result().to_dataframe()
        frames=[results,temp_result]
        results=pd.concat(frames)
    return results

def task(sql:str, x:str):

    df=client.query(sql).result().to_dataframe()

    # to ensure that only one thread can modify global variable
    responses_lock.acquire()
    responses[x] = df
    responses_lock.release()

def run_parallel_query(df:pd.DataFrame, key:str,search_key:str, split:int, sql:str, filter_string:str=None)->pd.DataFrame:
    """
        This query splits large patient cohort into manageable chhunks and run sql command for the chunks. It combines the result of all sql command and returns concat dataframe
        Args: 
            df: a dataframe containing key e.g. a patient dataframe with PATIENT_DK columns
            key: the column name of the df that will be used to filter the chunks e.g. PATIENT_DK
            search_key:
            split: (integer) the chunk size
            sql: sql statement
            filter_string: filter that needs to be applied to sql e.g. DIAGNOSIS_DESCRIPTION LIKE '%heart failure%' OR DIAGNOSIS_DESCRIPTION LIKE '%cardiac failure%'           
    
    """
    x=df.shape[0]
    n=x//split
    if n==0:
        n=1
    df_split=np.array_split(df, n)
    results=pd.DataFrame()
    responses.clear()
    pool.clear()
    
    for x in range(0,n):
        tdf=df_split[x]
        filter_list=tdf[key].values.tolist()
        filter_key=str(filter_list)[1:-1]
        
        if filter_string:
            SQL =f"{sql} WHERE {search_key} IN ({filter_key}) AND {filter_string}"            
        else:
            SQL =f"{sql} WHERE {search_key} IN ({filter_key}) "
            
        #create new thread with task
        thread = threading.Thread(target=task,args=(SQL,str(x),))
        thread.daemon = True
        # store thread in pool 
        pool.append(thread)
        #thread started
        thread.start()
        
    #wait for all threads tasks done
    for thread in pool:
        thread.join()
        
    frames=list(responses.values())   
    results=pd.concat(frames)
    return results