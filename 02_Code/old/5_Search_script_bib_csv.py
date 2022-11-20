#import bibtexparser
import pandas as pd
import numpy as np
import os
import glob
import yaml
import json

from functions import write_yaml
from functions import write_json
from functions import write_csv
from functions import read_yaml

import warnings
warnings.simplefilter(action = 'ignore', category = FutureWarning)

#Loading complete_data
df = pd.read_csv('01_Datasets/merged/df_merged_csv_bib.csv', sep=';', low_memory=False)

print(df[['title_csv', 'title_bib','keywords', 'abstract', 'year', 'type_publication', 'doi', 'jcr_value', 'scimago_value']].dtypes)
print(df.year.unique())

#########################################################################

# Creating yaml file if not exists
if not os.path.exists("../05_Config/configuration.yaml"):
  configuration_dict = {
        'output_extensions': ['csv', 'json', 'yaml'],
        'filter_atributes': ['title', 'keywords', 'abstract', 'year', 'type_publication', 'doi', 'jcr_value', 'scimago_value'],
        'filter_term':['big data']
    }
  with open('configuration.yaml', 'w') as yamlfile:
    data = yaml.dump(configuration_dict, yamlfile)

#########################################################################

#Reading yaml file
configuration_file = read_yaml("05_Config/configuration.yaml")


filter_columns = configuration_file['filter_atributes'][0]
filter_term = configuration_file['filter_term'][0]

print("\nfilter_atributes: ", filter_columns, '\n')
print("filter_term: ", filter_term, '\n')

#########################################################################

#Applying filter conditions
if df[filter_columns].dtype != 'object':
    df_filter = df.loc[df[filter_columns] == filter_term]
else:
    df_filter = df.loc[df[filter_columns].str.contains(filter_term, case=False, na=False)]
    
print(f'Found {df_filter.shape[0]} registers')
print(df_filter.head())

#########################################################################

#Exporting based on the yaml config file
print("Output extensions options: ", configuration_file['output_extensions'])

if 'csv' in configuration_file['output_extensions']:
    write_csv(df_filter, f'df_filter_{filter_columns}_{filter_term}')
if 'json' in configuration_file['output_extensions']:
    write_json(df_filter, f'df_filter_{filter_columns}_{filter_term}')
if 'yaml' in configuration_file['output_extensions']:
    write_yaml(df_filter, f'df_filter_{filter_columns}_{filter_term}')
if not configuration_file['output_extensions']:
  raise Exception("Please, select an output file extension")