"""
Shared sensor-processing logic for the ColdChain Monitoring Pipeline.

Contains reusable business rules for sensor validation,
anomaly detection, S3 storage, and SNS notifications.

Used by:
    - SQS-triggered consumer Lambda
    - SQS long-polling consumer Lambda
"""

import json
import logging
import os
from datetime import UTC, datetime

import boto3

# Configuration
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")
SNS_TOPIC_ARN = os.getenv("SNS_TOPIC_ARN")
SNS_SUBJECT = "ColdChain Sensor Anomaly Alert"

# Logging
logger = logging.getLogger(__name__)
logger.setLevel(os.getenv("LOG_LEVEL", "INFO").upper())

# AWS client
s3 = boto3.client("s3")
sns = boto3.client("sns")


def detect_anomalies(sensor):
    """Return alert messages for abnormal sensor readings."""
    alerts = []

    sensor_id = sensor["sensor_id"]
    warehouse_id = sensor["warehouse_id"]
    zone = sensor["zone"]
    reading_type = sensor["reading_type"]
    value = sensor["value"]
    unit = sensor["unit"]
    timestamp = sensor["timestamp"]
    battery_pct = sensor["battery_pct"]

    expected_range = None
    is_anomaly = False

    if reading_type == "temperature":
        if zone in ("cold-storage-A", "cold-storage-B"):
            minimum, maximum = -25, -15
        elif zone in ("dry-storage", "loading-dock"):
            minimum, maximum = 15, 25
        else:
            raise ValueError(f"Unknown temperature zone: {zone}")

        expected_range = f"{minimum} to {maximum}"
        is_anomaly = value < minimum or value > maximum

    elif reading_type == "humidity":
        minimum, maximum = 30, 60
        expected_range = f"{minimum} to {maximum}"
        is_anomaly = value < minimum or value > maximum

    else:
        raise ValueError(f"Unknown reading type: {reading_type}")

    if is_anomaly:
        alerts.append(
            f"Anomaly Detected: {sensor_id} in {warehouse_id}, "
            f"Zone: {zone}, Reading Type: {reading_type}, "
            f"Value: {value} {unit}, "
            f"Expected Range: {expected_range} {unit}, "
            f"Timestamp: {timestamp}"
        )

    if battery_pct < 15:
        alerts.append(
            f"Low Battery: {sensor_id} in {warehouse_id}, "
            f"Zone: {zone}, Battery: {battery_pct}%, "
            f"Threshold: Below 15%, Timestamp: {timestamp}"
        )

    return alerts

def save_to_s3(sensor, message_id):
    """Save one sensor reading as a date-partitioned JSONL object."""
    timestamp = sensor["timestamp"]

    # Parse the timestamp rather than relying on fixed string positions.
    reading_time = datetime.fromisoformat(timestamp)

    folder_path = reading_time.strftime("%Y/%m/%d/")
    cleaned_timestamp = reading_time.astimezone(
        UTC
    ).strftime("%Y%m%dT%H%M%SZ")

    filename = f"readings_{cleaned_timestamp}_{message_id}.jsonl"
    s3_key = f"{folder_path}{filename}"

    jsonl_data = json.dumps(sensor) + "\n"

    s3.put_object(
        Bucket=S3_BUCKET_NAME,
        Key=s3_key,
        Body=jsonl_data.encode("utf-8"),
        ContentType="application/x-ndjson"
    )

    logger.info(
        "S3 upload successful: bucket=%s key=%s",
        S3_BUCKET_NAME,
        s3_key
    )

    return s3_key

def send_alert(alert_message):
    """Publish an anomaly notification to Amazon SNS."""

    response = sns.publish(
        TopicArn=SNS_TOPIC_ARN,
        Subject=SNS_SUBJECT,
        Message=alert_message
    )

    logger.info(
        "SNS alert published: message_id=%s",
        response["MessageId"]
    )

def process_sensor(sensor, message_id):
    """
    Validate, archive, and evaluate one sensor reading.

    Args:
        sensor (dict): Deserialized sensor reading.
        message_id (str): SQS message identifier.

    Raises:
        ValueError: If required sensor fields are missing.
        Exception: If S3 storage or SNS publishing fails.
    """

    required_fields = (
        "sensor_id",
        "warehouse_id",
        "zone",
        "reading_type",
        "value",
        "unit",
        "timestamp",
        "battery_pct"
    )

    missing_fields = [
        field for field in required_fields if field not in sensor
    ]

    if missing_fields:
        raise ValueError(
            f"Missing required sensor fields: {missing_fields}"
        )

    sensor_id = sensor["sensor_id"]

    logger.info("Processing sensor_id=%s", sensor_id)

    alerts = detect_anomalies(sensor)

    # Save to S3 before publishing notifications.
    s3_key = save_to_s3(sensor, message_id)

    for alert_message in alerts:
        logger.warning("%s", alert_message)
        send_alert(alert_message)

    logger.info(
        "Sensor processed: sensor_id=%s alerts=%d s3_key=%s",
        sensor_id,
        len(alerts),
        s3_key
    )