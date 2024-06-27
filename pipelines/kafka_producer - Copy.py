from kafka import KafkaProducer
import yaml
import cv2
import os
import time
import threading
from queue import Queue
import queue
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
        self.frame_queue = Queue()
        self.stop_event = threading.Event()

    def send_frame(self, frame, key):
        encode_param = [int(cv2.IMWRITE_JPEG_QUALITY), 60]  # Adjust quality here
        success, buffer = cv2.imencode('.jpg', frame, encode_param)
        
        if success:
            self.producer.send(self.topic, key=key, value=buffer.tobytes())

    def process_frames(self):
        while not self.stop_event.is_set() or not self.frame_queue.empty():
            try:
                frame = self.frame_queue.get(timeout=0.01)
                self.send_frame(frame, "frame")
                self.frame_queue.task_done()
            except queue.Empty:
                continue

    def start_processing(self):
        self.stop_event.clear()
        self.processing_threads = []
        for _ in range(4):  # Number of threads to use
            thread = threading.Thread(target=self.process_frames)
            thread.start()
            self.processing_threads.append(thread)

    def stop_processing(self):
        self.stop_event.set()
        for thread in self.processing_threads:
            thread.join()

    def send_video(self, video_path):
        self.start_processing()

        video = cv2.VideoCapture(video_path)
        frame_skip_interval = 0.001  # Capture one frame every 33ms (~30 FPS)

        while True:
            try:
                success, frame = video.read()
                if not success:
                    break

                frame = cv2.resize(frame, (960, 540))  # Adjust resolution here

                self.frame_queue.put(frame)
                time.sleep(frame_skip_interval)
            except KeyboardInterrupt:
                break

        video.release()
        self.stop_processing()

    def close(self):
        self.producer.close()

if __name__ == '__main__':
    config_file_path = "F:\\BigData\\Traffic_Violation_Detection\\config\\kafka_config.yml"
    producer = TrafficViolationProducer(config_file_path)

    video_path = "F:\\BigData\\videos_input\\IMG_1322.MOV"
    print("Publishing video...")
    producer.send_video(video_path)
    producer.close()
    print("Publish complete")
