from kafka import KafkaConsumer
from pymongo import MongoClient
from json import loads

consumer = KafkaConsumer(
    'trump',
     bootstrap_servers=['localhost:9092'],
     auto_offset_reset='earliest',
     enable_auto_commit=True,
     group_id='tweet11',
     value_deserializer=lambda x: loads(x.decode('utf-8')))


client = MongoClient('localhost:27017')
collection = client.tweet.tweet


for message in consumer:
    message = message.value
    collection.insert_one(message)
    print('{} added to {}'.format(message, collection))
    # print('Message is  {}'.format(message))
