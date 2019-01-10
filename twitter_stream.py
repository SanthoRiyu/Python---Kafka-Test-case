from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
from kafka import SimpleProducer, KafkaClient

access_token = "937687556958949376-txuN4wFoIjmAar7Gc6mdr3p8tf69Qcl"
access_token_secret =  "0LOBdKj8UcZCRXLK707s3sSjgwoqaetzsbDsSASCjph5s"
consumer_key =  "tu7Hw5EQfMv3aT39g5CTE4Kz8"
consumer_secret =  "HNSfxB7ws5UFenIITBASfJXzRhHlxWIzvy4sN72OVRnya0C1le"

class StdOutListener(StreamListener):
    def on_data(self, data):
        producer.send_messages("trump", data.encode('utf-8'))
        print (data)
        return True
    def on_error(self, status):
        print (status)

kafka = KafkaClient("localhost:9092")
producer = SimpleProducer(kafka)
l = StdOutListener()
auth = OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
stream = Stream(auth, l)
stream.filter(track="trump")
