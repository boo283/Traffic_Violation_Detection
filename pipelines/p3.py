from kafka import KafkaConsumer
import cv2
import numpy as np
import time
import base64
from pymongo import MongoClient
client = MongoClient('localhost', 27017)
db = client['traffic_violation']
collection = db['violations']

def save_to_mongodb(frame, frame_id):
        _, buffer = cv2.imencode('.jpg', frame)
        frame_base64 = base64.b64encode(buffer).decode('utf-8')
        collection.insert_one({
            'frame_id': frame_id,
            'frame': frame_base64
        })
        print('Save frame to MongoDB')
# Initialize Kafka consumer which just reads newest frame
consumer = KafkaConsumer(
    'result',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    enable_auto_commit=False,
    value_deserializer=lambda x: x,
    key_deserializer=lambda x: x
)

# Display the frames
for message in consumer:
    frame_bytes = message.value
    frame = np.frombuffer(frame_bytes, dtype=np.uint8)
    frame = cv2.imdecode(frame, cv2.IMREAD_COLOR)
    
    if frame is not None:
        cv2.imshow('Processed Video', frame)
        time.sleep(0.05)
        
        frame_id = message.key.decode('utf-8')
        save_to_mongodb(frame, frame_id)
        if cv2.waitKey(1) & 0xFF == ord('q'):
            break
    else:
        print("No frame")

cv2.destroyAllWindows()
