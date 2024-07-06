import findspark
findspark.init()

import yaml
import numpy as np
from ultralytics import YOLOv10
import cv2
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
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
model_path = 'F:\\BigData\\Traffic_Violation_Detection\\data\\models\\best.pt'
model = YOLOv10(model_path)

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
    is_green = False
    #Check if green is in class_id
    if (3 in detections.class_id) or (7 not in detections.class_id) or (8 not in detections.class_id):
        is_green = True

    if is_green:
        violation_detections = [class_id for class_id in detections.class_id if class_id == 0 or class_id == 6]
    else:
        violation_detections = [class_id for class_id in detections.class_id if class_id == 6]

    annotated_frame = bounding_box_annotator.annotate(scene=frame, detections=detections)
    annotated_frame = label_annotator.annotate(annotated_frame, detections)

    # Encode annotated frame as JPEG
    _, buffer = cv2.imencode('.jpg', annotated_frame)
    frame_bytes = buffer.tobytes()

    # Add a flag to indicate if violation is detected
    if violation_detections:
        frame_bytes += b'|violation'
    
    return frame_bytes

process_udf = udf(process_frame_udf, BinaryType())

spark_df = spark.readStream.format("kafka")\
    .option("kafka.bootstrap.servers", "localhost:9092")\
    .option("subscribe", "traffic_violation_video_stream")\
    .option("startingOffsets", "earliest")\
    .load()

#apply udf on spark_df
spark_df = spark_df.withColumn("value", process_udf("value"))

# stream ra Kafka
query = spark_df.writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", "rs") \
    .option("checkpointLocation", "F:\BigData\Traffic_Violation_Detection\logs\checkpoint1") \
    .start()

query.awaitTermination()

# Stop when KeyboardInterrupt

query.stop()            
spark.stop()