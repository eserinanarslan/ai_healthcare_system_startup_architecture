# © Copyright 2020-21 Mayo Foundation For Medical Education and Research (MFMER). All rights reserved. V 1.0.0 
# This software is provided as-is, without warranty or representation for any use or purpose.
# Your use of it is subject to your agreement with Mayo.

import os

def getenv_boolean(var_name, default_value=False):
    result = default_value
    env_value = os.getenv(var_name)
    if env_value is not None:
        result = env_value.upper() in ("TRUE", "1")
    return result

# set the project id
project_id = "ml-mps-app-adeid-bq-p-644e"
# set the dataset id
dataset_id = "mcp"
# set the path for utility folder
path_location="/apps/jupyter/utility/"