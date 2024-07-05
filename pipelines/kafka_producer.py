from kafka import KafkaProducer
import yaml
import cv2
import os
import time

def load_kafka_config(config_path):
    with open(config_path, 'r') as file:
        kafka_config = yaml.safe_load(file)
    return kafka_config

class TrafficViolationProducer:
    def __init__(self, config_path):
        self.config = load_kafka_config(config_path)
        self.topic = self.config['producer']['topic']
        self.producer = KafkaProducer(
            bootstrap_servers=self.config['bootstrap_server'],
            key_serializer=lambda key: key.encode('utf-8'),
            value_serializer=lambda value: value
        )
    
    def send_frame(self, frame, key):
        encode_param = [int(cv2.IMWRITE_JPEG_QUALITY), 90]
        _, buffer = cv2.imencode('.jpg', frame, encode_param)
        self.producer.send(self.topic, key=key, value=buffer.tobytes())
        self.producer.flush()

    def close(self):
        self.producer.close()

if __name__ == '__main__':
    config_file_path = "F:\\BigData\\Traffic_Violation_Detection\\config\\kafka_config.yml"
    producer = TrafficViolationProducer(config_file_path)

    video = cv2.VideoCapture("F:\\BigData\\videos_input\\IMG_1320.MOV")
    print("Publishing video...")
    # while True:
    #     success, frame = video.read()
    #     if not success:
    #         break

    #     frame = cv2.resize(frame, (960, 540))
    #     producer.send_frame(frame, "frame", 0.5)

    frame_skip_interval = 1  
    sleep_interval = 0  

    while True:
        success, frame = video.read()
        if not success:
            break

        
        current_time = time.time()
        frame = cv2.resize(frame, (640, 640))
        producer.send_frame(frame, "frame")
        time.sleep(0.01)

    producer.close()
    video.release()
    print("Publish complete")
