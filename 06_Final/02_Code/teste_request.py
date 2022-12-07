import requests
from pprint import pprint
#bib_csv_fields ['abstract', 'author', 'country', 'doi', 'isbn', 'jcr_value', 'keywords', 'scimago_value', 'title_csv', 'title_bib', 'year']

# # IEEE Search id Route
# params_ieee_search_id = {
#         'query_ieee' : "SELECT * \
#                 FROM api_ieee \
#                 WHERE search_id LIKE '%20221207%' \
#                 LIMIT 1"}

# response = requests.get('http://127.0.0.1:5000/ieee_search_id/', json = params_ieee_search_id)

# print(response.status_code)

# if response.status_code < 300:
#     dict_search_id = response.json()
#     pprint(dict_search_id)

# Elsevier Search id Route
params_elsevier_search_id = {
        'query_elsevier' : "SELECT * \
                FROM api_elsevier \
                WHERE search_id LIKE '%20221207%' \
                LIMIT 1"}

response = requests.get('http://127.0.0.1:5000/elsevier_search_id/', json = params_elsevier_search_id)

print(response.status_code)

if response.status_code < 300:
    dict_search_id = response.json()
    pprint(dict_search_id)
    
# # IEEE api Route
                                         
# params_ieee_api = {'api_ieee_field_search': 'article_title', #[article_title, meta_data, abstract, publication_year, content_type, doi]
#                    'max_api_ieee_pagination': 1,
#                    'api_ieee_string_search': 'big+data',
#                     'query_ieee' : "SELECT * \
#                     FROM api_ieee \
#                     WHERE title LIKE '%big data%' \
#                     OR abstract LIKE '%data quality%' \
#                     LIMIT 1"}

# response = requests.get('http://127.0.0.1:5000/ieee_api/', json = params_ieee_api)

# print(response.status_code)

# if response.status_code < 300:
#     dict_search_id = response.json()
#     pprint(dict_search_id)


# # Elsevier api Route
# params_elsevier_api = {
#                         'api_elsevier_string_search': 'big+data',
#                         'query_elsevier' : "SELECT * \
#                         FROM api_elsevier \
#                         WHERE title LIKE '%big data%' \
#                         OR title LIKE '%data quality%'"}

# response = requests.get('http://127.0.0.1:5000/elsevier_api/', json = params_elsevier_api)

# print(response.status_code)

# if response.status_code < 300:
#     dict_search_id = response.json()
#     pprint(dict_search_id)