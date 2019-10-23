from pymongo import MongoClient
import pymongo
import json
import time
import requests
import timeloop


def connect_mongo(ip,port:int):
    try:
        client = MongoClient(ip,port)
    except Exception as e:
        print(e)
    return client

def connect_db(client,db,collection):
    try:
        dbclient = client[db]
        coll = dbclient[collection]
        print(coll)
        return coll
    except Exception as e:
        print(e)

def make_request(address):
    try:
        r = requests.request(method='GET', url=address)
        array = r.json()
        print(array)
    except Exception as e:
        print(e)
        time.sleep(5)
        make_request(address)

if __name__ == '__main__':
    print("Test")

    with open('configuration_source.json') as json_file:
        data = json.load(json_file)
        for source in data['DataSources']['Source']:
            make_request(source['ticker_url'])
            post = connect_db(connect_mongo('127.0.0.1',27017), source['database'], source['collection'] )
    print(post.find_one())
