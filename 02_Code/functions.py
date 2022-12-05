#Imports
import pandas as pd
import numpy as np
import bibtexparser
import json
import yaml
import os
import glob
import sqlite3
import requests
from datetime import datetime
import warnings
warnings.simplefilter(action = 'ignore', category = FutureWarning)
from pandas.core.common import SettingWithCopyWarning
warnings.simplefilter(action="ignore", category=SettingWithCopyWarning)

#def1
def read_bib(file_path: str):
    '''
    Function to read and parse bib files to dataframe object.
    path: bib file path
    '''

    with open(file_path, encoding = 'utf-8') as bibtex_file:
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
        print(f"\n{file_name}.yaml successfully written")
    return
#########################################################################

#def4
def write_json (data, file_name):
    data.to_json(f'../03_OutputFiles/{file_name}.json', orient='records', indent=4)
    print(f"\n{file_name}.json successfully written")
    return 
#########################################################################

#def5
def write_csv (data, file_name):
    data.to_csv(f'../03_OutputFiles/{file_name}.csv',sep=';', index = False)
    print(f"\n{file_name}.csv successfully written")
    return
#########################################################################

#def6
def read_yaml (file_name):
    with open(file_name, "r") as yamlfile:
        data = yaml.load(yamlfile, Loader=yaml.FullLoader)
        #print(f"{file_name} read successfully\n")
    return data

#########################################################################

#def7
def api_ieee():
    '''
    def functions to call/consume the API from IEEE portal

    Args:
    All args should be specified in the config_query.yaml file
    '''
    
    print('\n','#'*50,'\n',' API_IEEE\n','#'*50, '\n')

    #API_Params
    config_api = read_yaml("../05_Config/config_api.yaml")
    config_query = read_yaml("../05_Config/config_query.yaml")
    url = f"http://ieeexploreapi.ieee.org/api/v1/search/articles?apikey={config_api['apikey_ieee']}&format={config_api['format']}&max_records={config_api['max_records']}&sort_order={config_api['sort_order']}&sort_field=article_number&{config_query['api_ieee_field_search']}={config_query['api_ieee_string_search']}"
    #print("URL API_IEEE: ", url)

    #Request
    requestsIEEE = requests.get(url)

    if requestsIEEE.status_code < 300:
        dict_api_ieee = requestsIEEE.json()
        print('\ntotal_records: ' + str(dict_api_ieee['total_records']) +\
            '\ntotal_searched: ' + str(dict_api_ieee['total_searched']))

        if int(dict_api_ieee['total_records']) // 200 > config_query['max_api_ieee_pagination']:
            max_pages = config_query['max_api_ieee_pagination']
        else:
            max_pages = (int(dict_api_ieee['total_records']) // 200)
            print(f"Pagination available ({max_pages}) is bigger than max_api_pagination defined ({config_query['max_api_ieee_pagination']}) in the config_query.yaml")
        print('max_pagination: ', max_pages)

        list_output = []
        for start_record in range(1, max_pages * 200, 200):
            print(f'Requesting: {start_record} - {start_record+199}')
            url_pagination = f"{url}&start_record={start_record}"

            requestsIEEE = requests.get(url_pagination)
            dict_api_ieee = requestsIEEE.json()
            df_temp = pd.DataFrame.from_dict(dict_api_ieee['articles'])
            list_output.append(df_temp)

        df_api_raw = pd.concat(list_output, ignore_index = True)
        #print(df_api_raw.shape)

        #Extract only name of author when possible
        df_api_raw.authors = df_api_raw.authors.astype('O')
        for i, x in enumerate(df_api_raw.authors):

            if len(str(df_api_raw.authors[i]).split("'full_name': '", maxsplit = 1)) > 1:
                df_api_raw.authors[i] = str(df_api_raw.authors[i]).split("'full_name': '", maxsplit = 1)[-1].split("'", maxsplit = 1)[0]
            else:
                df_api_raw.authors[i] = np.nan

        df_api_ieee = df_api_raw[['title', 'abstract', 'publication_year', 'authors', 'doi', 'content_type', 'issn', 'isbn']]
        
        #Insert id_search [ieee_YYYYMMDD_HH_MM_SS]
        search_id = datetime.now().strftime("ieee_%Y%m%d_%H_%M_%S")
        df_api_ieee.insert(0, 'search_id', search_id)
        print('df_api_ieee: ', df_api_ieee.shape)

        #Export to SQLite Database
        dbfile = config_query['path_db']
        tabela ='api_ieee'
        db = sqlite3.connect(dbfile)
        # sqlDataTypes={}
        # for c in df_api_ieee.columns:
        #     if df_api_ieee[c].dtype.kind == 'i':  
        #         sqlDataTypes[c]='INTEGER'
        #     elif df_api_ieee[c].dtype.kind == 'f':
        #         sqlDataTypes[c]='REAL'
        #     else:
        #         sqlDataTypes[c]='TEXT'
        # df_api_ieee.to_sql(tabela, index=False, if_exists = 'append', dtype = sqlDataTypes, con = db)
        df_api_ieee.to_sql(tabela, index=False, if_exists = 'append', con = db)
        db.commit()
        db.close()
        print(f'{tabela} persisted in the sqlite db successfully')

    else:
        print('Status code: ',requestsIEEE.status_code, '\nProbably already reached the daily limit of consultations')

    return 

#########################################################################

#def8
def api_elsevier():
    '''
    def functions to call/consume the API from Elsevier portal

    Args:
    All args should be specified in the config_query.yaml file
    '''
    print('\n','#'*50,'\n',' API_ELSEVIER\n','#'*50, '\n')

    #API_Params
    config_api = read_yaml("../05_Config/config_api.yaml")
    config_query = read_yaml("../05_Config/config_query.yaml")
    url = f"https://api.elsevier.com/content/search/sciencedirect?query={config_query['api_elsevier_string_search']}&apiKey={config_api['apikey_elsevier']}"
    #print("URL API_ELSEVIER: ", url)

    #Request
    req_elsevier = requests.get(url)
    if req_elsevier.status_code < 300:
        dict_api_elsevier = req_elsevier.json()

        #Parsing json to df
        doi = [i['prism:doi'] for i in dict_api_elsevier['search-results']['entry']]
        title = [i['dc:title'] for i in dict_api_elsevier['search-results']['entry']]
        publisher = [i['prism:publicationName'] for i in dict_api_elsevier['search-results']['entry']]
        publication_year = [i['prism:coverDate'][:4] for i in dict_api_elsevier['search-results']['entry']]

        #Adjusting Authors feature
        authors = [i.get('authors') for i in dict_api_elsevier['search-results']['entry']]
        list_index = []
        list_authors = []
        for i, x in enumerate(authors):
            try:
                if type(x.get('author')) == str:
                    list_index.append(i)
                    list_authors.append(x.get('author'))
                
                else:
                    for y in x.get('author'):
                        list_index.append(i)
                        list_authors.append(y.get('$'))
            except:
                continue

        df_author = pd.DataFrame({'index':list_index, 'authors': list_authors})
        df_author = df_author.groupby('index')['authors'].apply(list)
        authors = df_author.tolist()

        #Creating DataFrame
        df_api_elsevier = pd.DataFrame({'doi': doi,
                                        'title': title,
                                        'publisher': publisher,
                                        'publication_year': publication_year,
                                        'authors': authors})

        #Convert list to string
        df_api_elsevier['authors'] = df_api_elsevier['authors'].map(lambda x: ', '.join(x))

        #Insert id_search [elsevier_YYYYMMDD_HH_MM_SS]
        search_id = datetime.now().strftime("elsevier_%Y%m%d_%H_%M_%S")
        df_api_elsevier.insert(0, 'search_id', search_id)
        print('df_api_elsevier: ', df_api_elsevier.shape)
        
        #Exporting to SQlite database
        dbfile = config_query['path_db']
        tabela ='api_elsevier'
        db = sqlite3.connect(dbfile)
        #sqlDataTypes={}
        # for c in df_api_elsevier.columns:
        #     if df_api_elsevier[c].dtype.kind == 'i':  
        #         sqlDataTypes[c]='INTEGER'
        #     elif df_api_elsevier[c].dtype.kind == 'f':
        #         sqlDataTypes[c]='REAL'
        #     else:
        #         sqlDataTypes[c]='TEXT'
        # df_api_elsevier.to_sql(tabela, index=False, if_exists='append', dtype = sqlDataTypes, con = db)
        df_api_elsevier.to_sql(tabela, index=False, if_exists='append', con = db)
        db.commit()
        db.close() 
        print(f'{tabela} persisted in the sqlite db successfully')

    else:
        print('Status code: ',req_elsevier.status_code, '\nProvavelmente foi atingido o limite de consultas diario!')

    return