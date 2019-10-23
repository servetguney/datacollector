from pymongo import MongoClient
import pymongo
import json
import timeloop


def connect_mongo(ip,port:int):
    try:
        client = MongoClient(ip,port)
    except Exception as e:
        print(e)
    return client

def connect_db(client,db):
    try:
        post = client.db
        print(post)

    except Exception as e:
        print(e)

if __name__ == '__main__':
    print("Test")

    with open('configuration.json') as json_file:
        data = json.load(json_file)
        for source in data['DataSources']['Source']:
            print(source['subentity'])
    connect_db(connect_mongo('127.0.0.6',27000),"posts")