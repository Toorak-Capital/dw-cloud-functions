import functions_framework
from google.cloud import bigquery, pubsub_v1
import json

def notify_pipeline_status(response):
    client = bigquery.Client()
    # Initialize Pub/Sub client
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(project_id, topic_path)

    query = """
            SELECT run_finished_time FROM reporting.dw_pipeline_log WHERE run_finished_time BETWEEN CAST(TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 2 HOUR) AS DATETIME) AND CAST(CURRENT_TIMESTAMP() AS DATETIME)
        """

    query_job = client.query(query)

    results = query_job.result()

    if len(results)>0:
        pipeline_run_finished_time = results[0]['run_finished_time'].date()
        # Create a message payload
        message = {
            'pipeline': 'dw_pipeline',
            'status': 'Completed',
            'run_finished_time': str(pipeline_run_finished_time),
            'message': 'Pipeline completed on time'
        }
        # Convert message to bytes and publish to the Pub/Sub topic
        message_bytes = json.dumps(message).encode('utf-8')
        future = publisher.publish(topic_path, data=message_bytes)

    return {
        'statusCode': 200,
        'body': json.dumps(f"Message published: {future.result()}")
    }