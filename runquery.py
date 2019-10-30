from pymongo import MongoClient
import json
import time
import requests
from timeloop import Timeloop
from datetime import timedelta
from datetime import datetime


def connect_mongo(ip, port:int):
    try:
        client = MongoClient(ip, port)
    except Exception as e:
        print(e)
    return client

def connect_db(client, db, collection):
    try:
        dbname = client[db]
        coll = dbname[collection]
        return coll
    except Exception as e:
        print(e)

def job_info():
    with open('configuration_source.json') as json_file:
        data = json.load(json_file)
        for source in data['DataSources']['Source']:
            post = connect_db(connect_mongo('10.8.8.1', 27017), source['database'], source['collection'])
            result = post.find({'tag': source['tag']}).sort([('Y',-1),('M',-1),('D',-1),('H',-1),('MN',-1),('S',-1)]).limit(1)
            for i in result:
                print(i)


def run_query(database,collection):
    post = connect_db(connect_mongo('10.8.8.1', 27017), database, collection)
    query = {'tag': 'pari', 'schedule': "hourly", 'pair': "BTC_TL"}
    project = {"pair": 1, "data.volume":1, "D": 1, "H": 1}
    sortq = [("D", -1),("H", -1)]
    result = post.find(query, project).sort(sortq)
    for i in result:
        print(i)


run_query("currency","crypto")