

import bibtexparser
import json
import yaml
import pandas as pd
import numpy as np
import os
import glob


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
def load_bib(folder_path: str) -> pd.DataFrame:
    
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
    print(f'Shape df_{folder_path.split("/")[-1]}: ', df.shape)
    
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

