#Import requirements
import os

import json
import boto3
import time
from kafka import KafkaConsumer
from dotenv import load_dotenv

#Load environment variables
load_dotenv()


#MINIO Configuration variables
MINIO_ENDPOINT=os.getenv("MINIO_ENDPOINT", "http://localhost:9002")
MINIO_ACCESS_KEY=os.getenv("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET_KEY=os.getenv("MINIO_SECRET_KEY", "password123")
MINIO_BUCKET=os.getenv("MINIO_BUCKET", "bronze-transactions")


# Kafka configuration variables
KAFKA_TOPIC=os.getenv("KAFKA_TOPIC", "stock-quotes")
KAFKA_BOOTSTRAP_SERVERS=os.getenv("KAFKA_BOOTSTRAP_SERVERS")
KAFKA_GROUP_ID=os.getenv("KAFKA_GROUP_ID", "transactions-consumer-group")

#Minio Connection
s3 = boto3.client(
    "s3",
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=MINIO_ACCESS_KEY,
    aws_secret_access_key=MINIO_SECRET_KEY
)

# Ensure Bucket Exists
bucket_name = MINIO_BUCKET

# Ensure bucket exists (idempotent)
try:
    s3.head_bucket(Bucket=bucket_name)
    print(f"Bucket {bucket_name} already exists.")
except Exception:
    s3.create_bucket(Bucket=bucket_name)
    print(f"Created bucket {bucket_name}.")

#Define Consumer
consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id=KAFKA_GROUP_ID,
    value_deserializer=lambda v: json.loads(v.decode("utf-8"))
)

print("Consumerstreaming and saving to MinIO...")

#Main Function
for message in consumer:
    record = message.value
    symbol = record.get("symbol", "unknown")
    ts = record.get("fetched_at",int(time.time()))
    key = f"{symbol}/{ts}.json"

    s3.put_object(
        Bucket=bucket_name,
        Key=key,
        Body=json.dumps(record),
        ContentType="application/json"
    )
    print(f"Saved record for {symbol} = s3://{bucket_name}/{key}")
                    