from pymongo import MongoClient
import json
import time
import requests
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


def add_log(source, log):
    post = connect_db(connect_mongo('10.8.8.1', 27017), source['database'], 'log')
    post.insert(log)


tl = Timeloop()


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
                mydata = {}
                mydata['source'] = source['tag']
                mydata['time'] = time.ctime()
                mydata['type'] = source['type']
                if type(array) == list:
                    array = array[0]
                mydata['data'] = array
                post.insert_one(mydata)
                add_log(source, [mydata['source'], mydata['time'], mydata['type']])
    except Exception as e:
        print(e)


@tl.job(interval=timedelta(seconds=3600))
def job_hourly():
    try:
        with open('configuration_source.json') as json_file:
            data = json.load(json_file)
        for source in data['DataSources']['Source']:
            if source['type'] == "hourly":
                array = make_request(source['ticker_url'])
                post = connect_db(connect_mongo('10.8.8.1', 27017), source['database'], source['collection'])
                mydata = {}
                mydata['source'] = source['tag']
                mydata['time'] = time.ctime()
                mydata['type'] = source['type']
                if type(array) == list:
                    array = array[0]
                mydata['data'] = array
                post.insert_one(mydata)
                add_log(source, [mydata['source'], mydata['time'], mydata['type']])
    except Exception as e:
        print(e)


if __name__ == '__main__':
    tl.start(block=True)
