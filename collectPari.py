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

def connect_db(client,db,collection):
    try:
        post = client.db.collection
        return post
    except Exception as e:
        print(e)

if __name__ == '__main__':
    print("Test")

    with open('configuration_source.json') as json_file:
        data = json.load(json_file)
        for source in data['DataSources']['Source']:
            print(source['subentity'])
    post=connect_db(connect_mongo('127.0.0.1',27017),"pymongo_test","posts")
    mydict = {"name": "Peter", "address": "Lowstreet 27"}
    post.insert_one(mydict)
    print(post.find_one())
