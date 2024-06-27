from kafka_consumer import TrafficViolationConsumer
from kafka_producer import TrafficViolationProducer
import cv2
import yolov10_model
import numpy as np
#model = ''
#config_path = "F:\\BigData\\Traffic_Violation_Detection\\config\\kafka_config.yml"

class DetectionPipeline:
    def __init__(self, config_path, model_path):
        self.producer = TrafficViolationProducer(config_path)
        self.consumer = TrafficViolationConsumer(config_path)
        self.model = yolov10_model.load_model(model_path)

    def process_frames(self):
        for msg in self.consumer.consumer:
            frame = np.frombuffer(msg.value, dtype=np.uint8)
            if frame:
                frame = cv2.imdecode(frame, cv2.IMREAD_COLOR)
                frame_id = msg.key.decode('utf-8')
                detections = self.model.detect(frame)
                violations = self.check_violation(detections)
                self.producer.send_frame(violations, frame_id)
            else:
                print("Error decoding frame")


    def check_violation(self, detections):
        violations = []
        for detection in detections:
            if self.is_violations(detection):
                violations.append(detection)
        return violations
    
    def is_violations(self, detection):
        return detection['class'] in ['no_helmet', 'red_light']

if __name__ == "__main__":
    pipeline = DetectionPipeline('config/kafka_config.yml', 'data/models/yolov10/yolov10_weights.pth')
    pipeline.process_frames()