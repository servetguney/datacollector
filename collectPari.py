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
    mylist = []
    if (source['dtype'] == "dict") and (source["dformat"]== "Multiple"):
        for i in array.keys():
            mylist.append({
                'tag': source['tag'],
                'type': source['type'],
                'schedule': source['schedule'],
                'pair': i,
                'data': array[i],
                'Y': datetime.now().year,
                'M': datetime.now().month,
                'D': datetime.now().day,
                'H': datetime.now().hour,
                'MN': datetime.now().minute,
                'S': datetime.now().second
            })
        return mylist
    elif source['dtype'] == "list" and (source["dformat"] == "Multiple"):
        for i in array:
            mylist.append({
                'tag': source['tag'],
                'type': source['type'],
                'schedule': source['schedule'],
                'pair': i["pair"],
                'data': i,
                'Y': datetime.now().year,
                'M': datetime.now().month,
                'D': datetime.now().day,
                'H': datetime.now().hour,
                'MN': datetime.now().minute,
                'S': datetime.now().second
            })
        return mylist
    elif source['dtype'] == "dict" and (source["dformat"] == "Single"):
        for i in array:
            subarray = make_request(source["market_url"]+i+"/")
            mylist.append({
                'tag': source['tag'],
                'type': source['type'],
                'schedule': source['schedule'],
                'pair': i,
                'data': subarray,
                'Y': datetime.now().year,
                'M': datetime.now().month,
                'D': datetime.now().day,
                'H': datetime.now().hour,
                'MN': datetime.now().minute,
                'S': datetime.now().second
            })
        return mylist
    else:
        mylist.append({
            'tag': source['tag'],
            'type': source['type'],
            'schedule': source['schedule'],
            'data': array,
            'Y': datetime.now().year,
            'M': datetime.now().month,
            'D': datetime.now().day,
            'H': datetime.now().hour,
            'MN': datetime.now().minute,
            'S': datetime.now().second
        })
        return mylist


tl = Timeloop()


@tl.job(interval=timedelta(seconds=30))
def job_info():
    with open('configuration_source.json') as json_file:
        data = json.load(json_file)
        for source in data['DataSources']['Source']:
            post = connect_db(connect_mongo('10.8.8.1', 27017), source['database'], source['collection'])
            result = post.find({'tag': source['tag']}).limit(2)
            print(result)


@tl.job(interval=timedelta(seconds=10))
def job_daily():
    try:
        with open('configuration_source.json') as json_file:
            data = json.load(json_file)
        for source in data['DataSources']['Source']:
            if source['schedule'] == "daily":
                array = make_request(source['ticker_url'])
                post = connect_db(connect_mongo('10.8.8.1', 27017), source['database'], source['collection'])
                mylist = get_ticker_data(array, source)
                post.insert_many(mylist)
    except Exception as e:
        print(e)

if __name__ == '__main__':
    tl.start(block=True)


