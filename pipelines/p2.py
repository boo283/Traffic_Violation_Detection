import findspark
findspark.init()

import yaml
import numpy as np
from ultralytics import YOLOv10
import cv2
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.types import BinaryType
import supervision as sv


def load_kafka_config(config_path):
    with open(config_path, 'r') as file:
        kafka_config = yaml.safe_load(file)
    return kafka_config

#Create spark session
spark = SparkSession.builder \
    .appName("TrafficViolationDetection") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1")\
    .getOrCreate()

#model = torch.hub.load('F:\\BigData\\Traffic_Violation_Detection\\data\\models\\best.pt', model='yolov10')
model = YOLOv10('F:\\BigData\\Traffic_Violation_Detection\\data\\models\\best.pt')

# Define annotators
bounding_box_annotator = sv.BoundingBoxAnnotator()
label_annotator = sv.LabelAnnotator()


def process_frame_udf(frame_bytes):
    frame = np.frombuffer(frame_bytes, dtype=np.uint8)
    frame = cv2.imdecode(frame, cv2.IMREAD_COLOR)
    if frame is None:
        return []
    
    results = model(frame)[0]
    detections = sv.Detections.from_ultralytics(results)
    
# There couldn't be any BLOW THE RED LIGHT class if there is a GREEN class
    #Check if green is in class_id
    green_detections = [
                            {"class_id": class_id} 
                            for class_id in zip(detections.class_id) 
                            if class_id == 3 
                        ]
    if green_detections:
        #exlude all btrl class and box
        detections = sv.Detections(
            xyxy = [box for box, class_id in zip(detections.xyxy, detections.class_id) if class_id != 0],
            confidence = [score for score, class_id in zip(detections.confidence, detections.class_id) if class_id != 0],
            class_id = [class_id for class_id in detections.class_id if class_id != 0]
        )

    annotated_frame = bounding_box_annotator.annotate(scene=frame, detections=detections)
    annotated_frame = label_annotator.annotate(annotated_frame, detections)

    # Encode annotated frame as JPEG
    _, buffer = cv2.imencode('.jpg', annotated_frame)
    frame_bytes = buffer.tobytes()

    
    return frame_bytes

process_udf = udf(process_frame_udf, BinaryType())

spark_df = spark.readStream.format("kafka")\
    .option("kafka.bootstrap.servers", "localhost:9092")\
    .option("subscribe", "traffic_violation_video_stream")\
    .option("startingOffsets", "earliest")\
    .load()

#apply udf on spark_df
spark_df = spark_df.withColumn("value", process_udf("value"))

# Viết stream ra Kafka
query = spark_df.writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", "result") \
    .option("checkpointLocation", "F:\BigData\Traffic_Violation_Detection\logs\checkpoint") \
    .start()

query.awaitTermination()

# Stop when KeyboardInterrupt

query.stop()
spark.stop()