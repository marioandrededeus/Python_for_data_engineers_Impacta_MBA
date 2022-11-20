#API - IEEE

def api_ieee():
    import pandas as pd
    import numpy as np
    import sqlite3
    import requests

    import warnings
    warnings.simplefilter(action = 'ignore', category = FutureWarning)

    from functions import read_yaml

    #################################################################################
    #API_Params
    query_params = read_yaml("../05_Config/config_api.yaml")
    print("query_params: ", query_params)

    url = f"http://ieeexploreapi.ieee.org/api/v1/search/articles?apikey={query_params['apikey']}&format={query_params['format']}&max_records={query_params['max_records']}&start_record={query_params['start_record']}&sort_order={query_params['sort_order']}&sort_field=article_number&{query_params['field_to_search1']}={query_params['search_for1']}&{query_params['field_to_search2']}={query_params['search_for2']}"
    print("URL: ",url)

    #######################################

    requestsIEEE = requests.get(url)

    if requestsIEEE.status_code >= 400:
        print('Status code: ',requestsIEEE.status_code, '\nProvavelmente já foi atingido o limite de consultas diario!')

    else:
        dict_api_ieee = requestsIEEE.json()
        print('\ntotal_records: ' + str(dict_api_ieee['total_records']) +\
            '\ntotal_searched: ' + str(dict_api_ieee['total_searched']))

        if (int(dict_api_ieee['total_records']) // 200) > 9:
            max_pages = (9*200)+2
        else:
            max_pages = ((int(dict_api_ieee['total_records']) // 200) *200) + 2
        list_output = []
        for start_record in range(1,max_pages,200):
            print('start_record: ', start_record)
            url = f"http://ieeexploreapi.ieee.org/api/v1/search/articles?apikey={query_params['apikey']}&format={query_params['format']}&max_records={query_params['max_records']}&start_record={start_record}&sort_order={query_params['sort_order']}&sort_field=article_number&{query_params['field_to_search1']}={query_params['search_for1']}&{query_params['field_to_search2']}={query_params['search_for2']}"
            requestsIEEE = requests.get(url)
            dict_api_ieee = requestsIEEE.json()
            df_temp = pd.DataFrame.from_dict(dict_api_ieee['articles'])
            list_output.append(df_temp)

        df_api_original_articles = pd.concat(list_output, ignore_index = True)
        print(df_api_original_articles.shape)

    #######################################

        #Extract only name of author when possible
        df_api_original_articles.authors = df_api_original_articles.authors.astype('O')
        for i, x in enumerate(df_api_original_articles.authors):

            if len(str(df_api_original_articles.authors[i]).split("'full_name': '", maxsplit = 1)) > 1:
                df_api_original_articles.authors[i] = str(df_api_original_articles.authors[i]).split("'full_name': '", maxsplit = 1)[-1].split("'", maxsplit = 1)[0]
            else:
                df_api_original_articles.authors[i] = np.nan

        df_api_articles = df_api_original_articles[['title', 'abstract', 'publication_year', 'authors', 'doi', 'content_type', 'issn', 'isbn']]

        print('\ndf_api_original_articles: ', df_api_original_articles.shape)
        print('df_api_articles: ', df_api_original_articles.shape)

    #######################################

    #Export to SQLite Database
    dbfile = '/Users/amigosdadancamooca/Documents/Impacta/Python_for_Data_Engineers/03_OutputFiles/doi.db'
    tabela ='api_ieee'
    db = sqlite3.connect(dbfile)
    sqlDataTypes={}
    for c in df_api_articles.columns:
        if df_api_articles[c].dtype.kind == 'i':  
            sqlDataTypes[c]='INTEGER'
        elif df_api_articles[c].dtype.kind == 'f':
            sqlDataTypes[c]='REAL'
        else:
            sqlDataTypes[c]='TEXT'
    df_api_articles.to_sql(tabela, index=False, if_exists='replace', dtype=sqlDataTypes, con=db)   
    db.commit()
    db.close() 
    print(f'{tabela} gravada com sucesso no banco SQLite')

    #######################################
    #Check db content

    db = sqlite3.connect(dbfile)
    tabela = 'api_ieee'
    df_query_api = pd.read_sql_query(f'select * from {tabela} LIMIT 5', db)
    print('Count(*): ', pd.read_sql_query(f'select count(*) from {tabela}', db))
    print(df_query_api)