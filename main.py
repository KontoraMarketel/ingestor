import logging
import os

import json
import boto3

from kafka import KafkaConsumer, KafkaProducer

from comissions import fetch_commissions


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
        group_id='ingestors'
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
                bootstrap_servers,
                process_data_tasks_topic,
            )
        except Exception as e:
            logging.error(e)
    consumer.close()


def ingest(api_token, endpoint_url, access_key, secret_key, bucket, task_id, load_date, bootstrap_servers,
           process_data_tasks_topic):
    if not api_token:
        raise ValueError("API_TOKEN environment variable is not set")

    # S3 / MinIO параметры
    logging.info(bucket)

    s3_client = boto3.client(
        's3',
        endpoint_url=endpoint_url,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key
    )

    # Префикс куда сохранять инжесты
    prefix = f"{load_date}/{task_id}/"

    fetch_commissions(api_token, s3_client, prefix)

    logging.info("Data successfully uploaded to minio")

    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        enable_auto_commit=True,
        auto_offset_reset='earliest',
    )

    body = {
        "load_id": task_id,
        "created_at": load_date,
    }

    producer.send(topic=process_data_tasks_topic, value=json.dumps(body).encode("utf-8"), key=task_id.encode("utf-8"))

    logging.info(f"Process task to processing topic, load_id: {task_id}")


if __name__ == "__main__":
    main()
