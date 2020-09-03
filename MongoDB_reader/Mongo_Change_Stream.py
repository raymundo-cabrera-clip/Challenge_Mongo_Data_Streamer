'''
requieres
pymongo
Kibana
elasticsearch python
'''

DATABASE = 'challengeDB' #This will be the index you need to select in Kibana, it is also the database to use in mongo to pull data from


from pymongo import MongoClient
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk, parallel_bulk
from collections import deque
from tqdm import tqdm
import time

client = Elasticsearch()
mgclient = MongoClient()
db = mgclient[DATABASE]
col = db['challengeCollection']

client = MongoClient()
#creates a change stream in mongo
with client.watch(
        #check for inserts
        [{'$match': {'operationType': 'insert'}}]) as stream:
    for insert_change in stream:
        # Pull from mongo and dump into ES
        actions = []
        for data in tqdm(col.find(), total=col.estimated_document_count()):
            data.pop('_id')
            action = {
                "_index": DATABASE,
                "_type": "myCollection",
                "_source": data
            }
            actions.append(action)

            # Dump x number of objects at a time into ES
            if len(actions) >= 100:
                deque(parallel_bulk(client, actions), maxlen=0)
                actions = []
            time.sleep(.01)
