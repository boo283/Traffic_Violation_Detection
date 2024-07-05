from kafka.admin import KafkaAdminClient, NewTopic

admin_client = KafkaAdminClient(
    bootstrap_servers='localhost:9092',
    client_id='test'
)

# Delete topic
admin_client.delete_topics(['result'])
admin_client.delete_topics(['traffic_violation_video_stream'])

# Create topic
topic_list = [NewTopic(name='result', num_partitions=1, replication_factor=1)]
admin_client.create_topics(new_topics=topic_list, validate_only=False)

topic_list = [NewTopic(name='traffic_violation_video_stream', num_partitions=1, replication_factor=1)]
admin_client.create_topics(new_topics=topic_list, validate_only=False)
