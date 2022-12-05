import bibtexparser
import pandas as pd
import numpy as np
import os
import glob

import yaml

import warnings
warnings.simplefilter(action = 'ignore', category = FutureWarning)

#Def functions

#########################################################################

#def1
def read_bib(file_path: str):
    '''
    Function to read and parse bib files to dataframe object.
    path: bib file path
    '''
    with open(file_path) as bibtex_file:
        bib_file = bibtexparser.load(bibtex_file)
    df = pd.DataFrame(bib_file.entries)
    
    return df
#########################################################################

#def2
def load_bib(folder_path: str):
    
    '''
    Function to: 
    1) read and parse bib files from a list of directories (folders); 
    2) concatenate multiple dataframes in an only one
    
    folder_path: directories path where are located the bib files
    
    '''
    
    #listing bib files path from acm directory
    list_files = []
    for file in glob.glob(f'{folder_path}/*.bib'):
        list_files.append(file)

    #loading each bib file listed
    list_df = []
    c = 1
    for file in list_files:
        df_temp = read_bib(file) #def
        list_df.append(df_temp)
        print(f'{c} de {len(list_files)}: {file}')
        c += 1

    #concatenating all files in a unique dataframe object
    df = pd.concat(list_df)
    print(f'Shape df_{folder}: ', df.shape)
    
    return df
#########################################################################

#def3
def write_yaml (data, file_name):
  with open(f'../03_OutputFiles/{file_name}.yaml', 'w') as output_file:
    yaml.dump_all(data.to_dict('records'), output_file, default_flow_style=False)
    print(f"{file_name}.yaml successfully written")
#########################################################################

#def4
def write_json (data, file_name):
  data.to_json(f'../03_OutputFiles/{file_name}.json', orient='records', indent=4)
  print(f"{file_name}.json successfully written")
#########################################################################

#def5
def write_csv (data, file_name):
  data.to_csv(f'../03_OutputFiles/{file_name}.csv',sep=';', index = False)
  print(f"{file_name}.csv successfully written")
#########################################################################

#def6
def read_yaml (file_name):
  with open(file_name, "r") as yamlfile:
      data = yaml.load(yamlfile, Loader=yaml.FullLoader)
      print(f"{file_name} read successfully")
  return data

#########################################################################
#########################################################################

#read, load and concatenate bibtex files
list_folders = []
for folder in glob.glob(f'../01_Datasets/*'):
    list_folders.append(folder)
list_folders

list_df = []
for f in list_folders:
    print('\n',f)
    df_temp = load_bib(f) #def load_bib
    list_df.append(df_temp)
df_all_raw = pd.concat(list_df)
print('\nShape df_all_raw: ',df_all_raw.shape)

#Features renaming / selection
df_all = df_all_raw.copy()
df_all.rename(columns={'ENTRYTYPE': 'type_publication'}, inplace = True)
cols_to_keep = ['author', 'title', 'keywords', 'abstract', 'year', 'type_publication', 'doi']
df_all = df_all[cols_to_keep]

#Adjusting "year" feature dtype
df_all['year'] = df_all.year.astype('int64')

#Sorting values by "year"
df_all = df_all.sort_values('year')

#Dropna
print('Shape before dropna: ', df_all.shape)
df_all.dropna(axis = 0, inplace = True)
print('Shape after dropna: ', df_all.shape)

#Drop_duplicates
print('Shape before drop_duplicates: ',df_all.shape)
df_all.drop_duplicates(inplace = True)
print('Shape after drop_duplicates: ',df_all.shape)

#Creating 03_OutputFiles if not exists
if not os.path.exists('../03_OutputFiles/'):
  os.makedirs('../03_OutputFiles/')

#Creating yaml file if not exists
if not os.path.exists("configuration.yaml"):
  configuration_dict = {
        'output_extensions': ['csv', 'json', 'yaml']
    }
  with open('configuration.yaml', 'w') as yamlfile:
    data = yaml.dump(configuration_dict, yamlfile)

configuration_file = read_yaml("configuration.yaml")


print("Output extensions options: ", configuration_file['output_extensions'])

#Export
for extension_name in configuration_file['output_extensions']:
  if extension_name == 'csv':
    write_csv(df_all, 'df_all')
  elif extension_name == 'json':
    write_json(df_all, 'df_all')
  elif extension_name == 'yaml':
    write_yaml(df_all, 'df_all')