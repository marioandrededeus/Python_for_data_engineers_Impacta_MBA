from flask import Flask, request
from flask import jsonify
import pandas as pd
import sqlite3
from functions import read_yaml
from functions import api_ieee, api_elsevier

app = Flask(__name__)
app.run(debug=True)

config_api = read_yaml("../05_Config/config_api.yaml")

@app.route('/ieee_search_id/', methods=['GET'])
def ieee_search_id():
    #Connecting to SQLite db
    dbfile = config_api['path_db']
    db = sqlite3.connect(dbfile, check_same_thread=False)
    
    request_data_json = request.get_json()
    config_query = request_data_json
    
    df_query_ieee = pd.read_sql_query(config_query['query_ieee'], db)
    json_ieee = df_query_ieee.to_json(orient='index', indent=4)
    db.commit()
    db.close()
    
    return json_ieee

@app.route('/elsevier_search_id/', methods=['GET'])
def elsevier_search_id():
    #Connecting to SQLite db
    dbfile = config_api['path_db']
    db = sqlite3.connect(dbfile, check_same_thread=False)
    
    request_data_json = request.get_json()
    config_query = request_data_json
    
    df_query_elsevier = pd.read_sql_query(config_query['query_elsevier'], db)
    json_elsevier = df_query_elsevier.to_json(orient='index', indent=4)
    db.commit()
    db.close()
    
    return json_elsevier

@app.route('/ieee_api/', methods=['GET'])
def ieee_api():
    global config_api
    
    request_data_json = request.get_json()
    config_query = request_data_json
    
    json_ieee_api = api_ieee(config_api, config_query)
    
    return json_ieee_api

@app.route('/elsevier_api/', methods=['GET'])
def elsevier_api():
    global config_api
    
    request_data_json = request.get_json()
    config_query = request_data_json
    
    json_elsevier_api = api_elsevier(config_api, config_query)
    
    return json_elsevier_api