from functions_api import get_api
import pandas as pd
from functions import read_yaml
from pprint import pprint

config_query = read_yaml("../05_Config/config_query.yaml")

json_search_id, json_ieee, json_elsevier = get_api(config_query)

pprint("json_search_id:")
pprint(json_search_id)