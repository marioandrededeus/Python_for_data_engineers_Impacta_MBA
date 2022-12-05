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
from functions import api_elsevier

import warnings
warnings.simplefilter(action = 'ignore', category = FutureWarning)
from pandas.core.common import SettingWithCopyWarning
warnings.simplefilter(action="ignore", category=SettingWithCopyWarning)

#Reading yaml config_query
config_query = read_yaml("../05_Config/config_query.yaml")

#Reading yaml config_query
config_api = read_yaml("../05_Config/config_api.yaml")

#Connecting to SQLite db
dbfile = config_query['path_db']
db = sqlite3.connect(dbfile)

#Call API IEEE
if config_query['call_api_ieee'] == True:
    api_ieee() #module from functions.py

#Call API Elsevier
if config_query['call_api_elsevier'] == True:
    api_elsevier() #module from functions.py

if config_query['use_query_sql_bib_csv'] == True:
    print('\n','#'*50,'\n',' SQLITE QUERY - BIBTEX_CSV\n','#'*50)
    tabela ='bib_csv'
    df_query_bib_csv = pd.read_sql_query(config_query['query_bib_csv'], db)
    print('\nquery_bib_csv: ', config_query['query_bib_csv'])
    print(f'query_bib_csv: {df_query_bib_csv.shape} results', db)
    

if config_query['use_query_sql_ieee'] == True:
    print('\n','#'*50,'\n',' SQLITE QUERY - API IEEE\n','#'*50)
    tabela ='api_ieee'
    df_query_ieee = pd.read_sql_query(config_query['query_ieee'], db)
    print('\nquery_ieee: ',config_query['query_ieee'])
    print(f'query_ieee: {df_query_ieee.shape} results', db)

if config_query['use_query_sql_elsevier'] == True:
    print('\n','#'*50,'\n',' SQLITE QUERY - API ELSEVIER\n','#'*50)
    tabela ='api_elsevier'
    df_query_elsevier = pd.read_sql_query(config_query['query_elsevier'], db)
    print('\nquery_elsevier: ',config_query['query_elsevier'])
    print(f'query_elsevier: {df_query_elsevier.shape} results', db)

db.commit()
db.close()

#########################################################################

#Exporting based on the yaml config file
#print("Output extensions options: ", configuration_file['output_extensions'])

print('\n','#'*50,'\n',' EXPORT FILES\n','#'*50)

if 'csv' in config_query['output_extensions']:
    if config_query['use_query_sql_bib_csv'] == True:
        write_csv(df_query_bib_csv, f'df_query_bib')
    if config_query['use_query_sql_ieee'] == True:
        write_csv(df_query_ieee, f'df_query_ieee')
    if config_query['use_query_sql_elsevier'] == True:
        write_csv(df_query_elsevier, f'df_query_elsevier')

if 'json' in config_query['output_extensions']:
    if config_query['use_query_sql_bib_csv'] == True:
        write_json(df_query_bib_csv, f'df_query_bib')
    if config_query['use_query_sql_ieee'] == True:
        write_json(df_query_ieee, f'df_query_ieee')
    if config_query['use_query_sql_elsevier'] == True:
        write_json(df_query_elsevier, f'df_query_elsevier')

if 'yaml' in config_query['output_extensions']:
    if config_query['use_query_sql_bib_csv'] == True:
        write_yaml(df_query_bib_csv, f'df_query_bib')
    if config_query['use_query_sql_ieee'] == True:
        write_yaml(df_query_ieee, f'df_query_ieee')
    if config_query['use_query_sql_elsevier'] == True:
        write_yaml(df_query_elsevier, f'df_query_elsevier')

if not config_query['output_extensions']:
    raise Exception("Please, select a valid output file extension [json, csv, yaml] in the config_query.yaml")

print('\n','#' * 50,'\n Process completed successfully','\n','#' * 50)