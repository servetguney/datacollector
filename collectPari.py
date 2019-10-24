from pymongo import MongoClient
import json
import time
import requests
from timeloop import Timeloop
from datetime import timedelta



def connect_mongo(ip,port:int):
    try:
        client = MongoClient(ip,port)
    except Exception as e:
        print(e)
    return client

def connect_db(client,db,collection):
    try:
        dbname = client[db]
        coll = dbname[collection]
        print(coll)
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

tl = Timeloop()


@tl.job(interval=timedelta(seconds=10))
def job_daily():
    mydata = {}
    try:
        with open('configuration_source.json') as json_file:
            data = json.load(json_file)
        for source in data['DataSources']['Source']:
            if data['DataSources']['Source']['type'] == "daily":
                array = make_request(source['ticker_url'])
                mydata['contentTitle']= source['tag']
                mydata['timestamp']= time.ctime()
                mydata['content']  = array
                post = connect_db(connect_mongo('10.8.8.1',27017), source['database'], source['collection'] )
                post.insert_one(mydata)
                for sample in post.find():
                    print(sample)
                post.insert_one(mydata)
    except Exception as e:
        print("Can not insert %s".format(e))



if __name__ == '__main__':
    tl.start(block=True)
