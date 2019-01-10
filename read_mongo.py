import pymongo
from pymongo import MongoClient
import pprint

client = MongoClient('localhost:27017')
db = client.numtest
data_coll = db.numtest
print('Total Record for the collection: ' + str(data_coll.count()))
for record in data_coll.find().limit(999):
     pprint.pprint(record)
