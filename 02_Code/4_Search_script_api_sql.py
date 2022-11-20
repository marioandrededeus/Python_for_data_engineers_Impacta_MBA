#import bibtexparser
import pandas as pd
import numpy as np
import os
import yaml
import json
import sqlite3

from functions import write_yaml
from functions import write_json
from functions import write_csv
from functions import read_yaml
from functions import api_ieee
import warnings
warnings.simplefilter(action = 'ignore', category = FutureWarning)

#Reading yaml config_query
config_query = read_yaml("../05_Config/config_query.yaml")

print('#'*50,'\n',' API IEEE\n','#'*50, '\n')

if config_query['call_api_ieee'] == True:
    api_ieee(   field_search = config_query['api_ieee_field_search'], 
                string_search = config_query['api_ieee_string_search'], 
                max_pagination = config_query['max_api_pagination'])

print('#'*50,'\n',' SQLITE QUERY\n','#'*50)

#Connecting to SQLite db
dbfile = '/Users/amigosdadancamooca/Documents/Impacta/Python_for_Data_Engineers/03_OutputFiles/doi.db'
db = sqlite3.connect(dbfile)

if config_query['use_query_sql_bib_csv'] == True:
    tabela ='bib_csv'
    df_query_bib_csv = pd.read_sql_query(config_query['query_bib_csv'], db)
    print('\nquery_bib_csv: ',config_query['query_bib_csv'])
    print(f'\nquery_bib_csv: {df_query_bib_csv.shape} results', db)
    

if config_query['use_query_sql_ieee'] == True:
    tabela ='api_ieee'
    df_query_ieee = pd.read_sql_query(config_query['query_ieee'], db)
    print('\nquery_ieee: ',config_query['query_ieee'])
    print(f'\nquery_ieee: {df_query_ieee.shape} results', db)

db.commit()
db.close()

#########################################################################

#Exporting based on the yaml config file
#print("Output extensions options: ", configuration_file['output_extensions'])

if 'csv' in config_query['output_extensions']:
    if config_query['use_query_sql_bib_csv'] == True:
        write_csv(df_query_bib_csv, f'df_query_bib')
    if config_query['use_query_sql_ieee'] == True:
        write_csv(df_query_ieee, f'df_query_ieee')

if 'json' in config_query['output_extensions']:
    if config_query['use_query_sql_bib_csv'] == True:
        write_json(df_query_bib_csv, f'df_query_bib')
    if config_query['use_query_sql_ieee'] == True:
        write_json(df_query_ieee, f'df_query_ieee')

if 'yaml' in config_query['output_extensions']:
    if config_query['use_query_sql_bib_csv'] == True:
        write_yaml(df_query_bib_csv, f'df_query_bib')
    if config_query['use_query_sql_ieee'] == True:
        write_yaml(df_query_ieee, f'df_query_ieee')

if not config_query['output_extensions']:
    raise Exception("Please, select a valid output file extension [json, csv, yaml] in the config_query.yaml")

print('\n','#' * 50,'\n Process completed successfully','\n','#' * 50)