from pymongo import MongoClient
import json
import time
import requests
from datetime import date
from datetime import datetime
from timeloop import Timeloop
from datetime import timedelta


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

def make_request(address):
    try:
        r = requests.request(method='GET', url=address)
        array = r.json()
        return array
    except Exception as e:
        print(e)
        time.sleep(5)
        make_request(address)

def job_info():
    with open('configuration_source.json') as json_file:
        data = json.load(json_file)
        for source in data['DataSources']['Source']:
            post = connect_db(connect_mongo('10.8.8.1', 27017), source['database'], source['collection'])
            result = post.find({'tag': source['tag']}).sort([('Y',-1),('M',-1),('D',-1),('H',-1),('MN',-1),('S',-1)]).limit(1)
            for i in result:
                print(i)


def timeinfo():
    print(time.ctime())
    print(time.time())
    print(datetime.now().month)
    print(datetime.now().hour)
    print(datetime.now().minute)
    print(datetime.now().second)

job_info()