from kafka import KafkaConsumer
import yaml
import os
import numpy as np
import cv2
import time

def load_kafka_config(config_path):
    with open(config_path, 'r') as file:
        kafka_config = yaml.safe_load(file)
    return kafka_config

class TrafficViolationConsumer:
    def __init__(self, config_path):
        self.config = load_kafka_config(config_path)
        self.topic = self.config['consumer']['topic']
        self.consumer = KafkaConsumer(
            self.config['consumer']['topic'],
            bootstrap_servers=self.config['bootstrap_server'],
            group_id=self.config['consumer']['group_id'],
            value_deserializer=lambda value: value,
            auto_offset_reset=self.config['consumer']['auto_offset_reset']
        )
    
    def consume_frame(self):
        for msg in self.consumer:
            frame = np.frombuffer(msg.value, dtype=np.uint8)
            frame = cv2.imdecode(frame, cv2.IMREAD_COLOR)
            if frame is not None:
                cv2.imshow("frame", frame)
                if cv2.waitKey(1) & 0xFF == ord('q'):
                    break
            else:
                print("Error decoding frame")
    
    def close(self):
        self.consumer.close()

if __name__ == "__main__":
    config_folder_path = "F:\\BigData\\Traffic_Violation_Detection\\config"
    consumer = TrafficViolationConsumer(os.path.join(config_folder_path, 'kafka_config.yml'))
    consumer.consume_frame()
    consumer.close()
    cv2.destroyAllWindows()
