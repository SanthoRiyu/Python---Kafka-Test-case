import sys
from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from uuid import uuid1

if __name__ == '__main__':
    sc = SparkContext(appName='PythonstreamingCount')
    ssc = StreamingContext(sc,2)

    broker, topic = 'localhost:9092', 'spark'
    kvs = KafkaUtils.createStream(ssc, broker,"raw-event-streaming-consumer",{topic:1})
    lines = kvs.map(lambda x: x[1])
    counts = lines.flatMap(lambda line: line.split(" ")).map(lambda word: (word, 1)).reduceByKey(lambda a, b: a+b)
    counts.pprint()
    ssc.start()
    ssc.awaitTermination()
