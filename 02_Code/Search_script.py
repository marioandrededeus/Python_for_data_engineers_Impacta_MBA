#import bibtexparser
import pandas as pd
import numpy as np
import os
import glob

import yaml

import warnings
warnings.simplefilter(action = 'ignore', category = FutureWarning)

#########################################################################

def write_yaml (data, file_name):
  with open(f'../03_OutputFiles/{file_name}.yaml', 'w') as output_file:
    yaml.dump_all(data.to_dict('records'), output_file, default_flow_style=False)
    print(f"{file_name}.yaml successfully written")
#########################################################################

def write_json (data, file_name):
  data.to_json(f'../03_OutputFiles/{file_name}.json', orient='records', indent=4)
  print(f"{file_name}.json successfully written")
#########################################################################

def write_csv (data, file_name):
  data.to_csv(f'../03_OutputFiles/{file_name}.csv',sep=';', index = False)
  print(f"{file_name}.csv successfully written")
#########################################################################

def read_yaml (file_name):
  with open(file_name, "r") as yamlfile:
      data = yaml.load(yamlfile, Loader=yaml.FullLoader)
      print(f"{file_name} read successfully")
  return data
#########################################################################

#Loading complete_data
df = pd.read_csv('../01_Datasets/merged/df_merged_csv_bib.csv', sep=';', low_memory=False)
print(df[['title_csv', 'title_bib','keywords', 'abstract', 'year', 'type_publication', 'doi', 'jcr_value', 'scimago_value']].dtypes)
print(df.year.unique())

#########################################################################

# Creating yaml file if not exists
if not os.path.exists("configuration.yaml"):
  configuration_dict = {
        'output_extensions': ['csv', 'json', 'yaml'],
        'filter_atributes': ['title', 'keywords', 'abstract', 'year', 'type_publication', 'doi', 'jcr_value', 'scimago_value'],
        'filter_term':['big data']
    }
  with open('configuration.yaml', 'w') as yamlfile:
    data = yaml.dump(configuration_dict, yamlfile)

#########################################################################

#Reading yaml file
configuration_file = read_yaml("configuration.yaml")

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