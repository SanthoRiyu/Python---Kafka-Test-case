from time import sleep
from json import dumps
from kafka import KafkaProducer
print('Sending data......')
producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda x:
                         dumps(x).encode('utf-8'))

list1 = ['this is just a test', 'But this aint a message','This is not a dsd at all',
'This is not a dasdaaa at all','This is not a fhghg at all','This is not a mmhjmessage at all',
'This is not a dsada at all','This is not a xasadsa at all']
for e in list1:
    producer.send('numtest', value=e)
    sleep(1)
