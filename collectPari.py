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

def make_request(address):
    try:
        r = requests.request(method='GET', url=address)
        array = r.json()
        return array
    except Exception as e:
        print(e)
        time.sleep(5)
        make_request(address)


def add_log(source, collection ,log):
    try:
        post = connect_db(connect_mongo('10.8.8.1', 27017), source['database'], collection)
        post.insert_one(log)
    except Exception as e:
        print(e)


def get_ticker_data(array, source: dict):
    mydata = {}
    if (source['dtype'] == "dict") and (source["dformat"]== "Multiple"):
        for i in array.keys():
            mydata['tag'] = source['tag']
            mydata['type'] = source['type']
            mydata['schedule'] = source['schedule']
            mydata["pair"] = i
            mydata["data"] = array[i]
            mydata['Y'] = datetime.now().year
            mydata['M'] = datetime.now().month
            mydata['D'] = datetime.now().day
            mydata['H'] = datetime.now().hour
            mydata['MN'] = datetime.now().minute
            mydata['S'] = datetime.now().second
        return mydata
    elif source['dtype'] == "list" and (source["dformat"] == "Multiple"):
        for i in array:
            mydata['tag'] = source['tag']
            mydata['type'] = source['type']
            mydata['schedule'] = source['schedule']
            mydata["pair"] = i["pair"]
            mydata["data"] = i
            mydata['Y'] = datetime.now().year
            mydata['M'] = datetime.now().month
            mydata['D'] = datetime.now().day
            mydata['H'] = datetime.now().hour
            mydata['MN'] = datetime.now().minute
            mydata['S'] = datetime.now().second
        return mydata
    elif source['dtype'] == "dict" and (source["dformat"] == "Single"):
        for i in array:
            subarray = make_request(source["market_url"]+i+"/")
            mydata['tag'] = source['tag']
            mydata['type'] = source['type']
            mydata['schedule'] = source['schedule']
            mydata["pair"] = i
            mydata["data"] = subarray
            mydata['Y'] = datetime.now().year
            mydata['M'] = datetime.now().month
            mydata['D'] = datetime.now().day
            mydata['H'] = datetime.now().hour
            mydata['MN'] = datetime.now().minute
            mydata['S'] = datetime.now().second
        return mydata
    else:
        mydata['tag'] = source['tag']
        mydata['type'] = source['type']
        mydata['schedule'] = source['schedule']
        mydata["data"] = array
        mydata['Y'] = datetime.now().year
        mydata['M'] = datetime.now().month
        mydata['D'] = datetime.now().day
        mydata['H'] = datetime.now().hour
        mydata['MN'] = datetime.now().minute
        mydata['S'] = datetime.now().second
        return mydata


tl = Timeloop()


if __name__ == '__main__':
    tl.start(block=True)


@tl.job(interval=timedelta(seconds=30))
def job_info():
    with open('configuration_source.json') as json_file:
        data = json.load(json_file)
        for source in data['DataSources']['Source']:
            post = connect_db(connect_mongo('10.8.8.1', 27017), source['database'], 'log')
            post.find({'tag': source['tag']}).sort({'$natural': 1}).limit(5)


@tl.job(interval=timedelta(seconds=86400))
def job_daily():
    try:
        with open('configuration_source.json') as json_file:
            data = json.load(json_file)
        for source in data['DataSources']['Source']:
            if source['type'] == "daily":
                array = make_request(source['ticker_url'])
                post = connect_db(connect_mongo('10.8.8.1', 27017), source['database'], source['collection'])
                mydata = get_ticker_data(array, source)
                post.insert_one(mydata)
    except Exception as e:
        print(e)




