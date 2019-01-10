import time
import cv2
from kafka import SimpleProducer, KafkaClient

#connect to Kafka
kafka = KafkaClient('localhost:9092')
producer = SimpleProducer(kafka)

#Asign a topic

topic = 'my-topic'


def video_emitter(video):
    #open the video
    video = cv2.VideoCapture(video)
    print('emitting.......')

    #read the file
    while (video.isOpened):
        #read the image in each frame
        success, image = video.read()

        #check if the file has read to  the end
        if not success:
            break
        #convert th eimage png
        ret, jpeg = cv2.imencode('.png', image)
        #convert he image to bytes and send to Kafka
        producer.send_messages(topic, jpeg.tobytes())
        #to reduce the CPU usage create sleep time of 0.2search
        time.sleep(0.2)
    #clear the capture
    video.release()
    print('Done emitting')

if __name__ == '__main__':
    video_emitter('video.mp4')
