import logging
import os

import json
import boto3

from kafka import KafkaConsumer, KafkaProducer

from process_ingest import ingest


def main():
    logging.basicConfig(level=logging.INFO)

    read_topic = os.getenv('KAFKA_READ_TOPIC')
    bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS')

    endpoint_url = os.getenv("MINIO_ENDPOINT")  # например http://minio:9000
    access_key = os.getenv("MINIO_ACCESS_KEY")
    secret_key = os.getenv("MINIO_SECRET_KEY")
    bucket = os.getenv("MINIO_BUCKET")
    process_data_tasks_topic = os.getenv("KAFKA_PROCESS_DATA_TASKS_TOPIC")

    consumer = KafkaConsumer(
        read_topic,
        bootstrap_servers=bootstrap_servers,
        enable_auto_commit=True,
        auto_offset_reset='earliest',
        group_id='ingestors',
        max_poll_interval_ms=24 * 60 * 60 * 1000  # 24 часа
    )

    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda v: v.encode("utf-8"),
    )

    for message in consumer:
        message = json.loads(message.value)
        task_id = message['task_id']
        load_date = message['init_date']
        api_token = message['wb_token']
        try:
            ingest(
                api_token,
                endpoint_url,
                access_key,
                secret_key,
                bucket,
                task_id,
                load_date,
            )
            body = {
                "load_id": task_id,
                "created_at": load_date,
            }
            producer.send(topic=process_data_tasks_topic, value=body, key=task_id)
            logging.info(f"Process task to processing topic, load_id: {task_id}")
        except Exception as e:
            logging.error(e)
    consumer.close()


if __name__ == "__main__":
    main()
