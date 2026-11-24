
"""
ColdChain Monitoring Pipeline - SQS Long-Polling Consumer.

Purpose:
    Retrieve sensor messages from Amazon SQS using long polling.

Responsibilities:
    1. Poll up to five messages from the configured SQS queue.
    2. Deserialize each message body from JSON.
    3. Process each sensor reading.
    4. Delete messages only after successful processing.
    5. Log processing results and errors.

Note:
    The actual sensor-processing function will be implemented
    in the next step.
"""

import json
import logging
import os

from lambdas.common.sensor_processor import process_sensor

import boto3


# --------------------------------------------------
# Configuration
# --------------------------------------------------

AWS_REGION = os.getenv("AWS_REGION")
SQS_QUEUE_URL = os.getenv("SQS_QUEUE_URL")
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")


# --------------------------------------------------
# Logging
# --------------------------------------------------

logger = logging.getLogger(__name__)
logger.setLevel(LOG_LEVEL.upper())


# --------------------------------------------------
# AWS Clients
# --------------------------------------------------

sqs = boto3.client("sqs", region_name=AWS_REGION)


# --------------------------------------------------
# Configuration Validation
# --------------------------------------------------

def validate_configuration():
    """Validate required environment variables."""

    if not SQS_QUEUE_URL:
        raise ValueError(
            "Missing required environment variable: SQS_QUEUE_URL"
        )


# --------------------------------------------------
# Receive Messages
# --------------------------------------------------

def receive_messages():
    """
    Retrieve up to five messages from Amazon SQS.

    Long polling waits up to 10 seconds for messages
    when the queue is empty.

    Returns:
        list: SQS message dictionaries.
    """

    response = sqs.receive_message(
        QueueUrl=SQS_QUEUE_URL,
        MaxNumberOfMessages=5,
        WaitTimeSeconds=10,
        MessageSystemAttributeNames=["ApproximateReceiveCount"]
    )

    messages = response.get("Messages", [])

    logger.info(
        "Received %d messages from SQS",
        len(messages)
    )

    return messages

# --------------------------------------------------
# Process SQS Messages
# --------------------------------------------------

def process_messages(messages):
    """
    Process received SQS messages individually.

    Delete a message only after successful processing.
    Failed messages remain in SQS for retry.
    """

    for message in messages:

        message_id = message.get("MessageId")
        receipt_handle = message.get("ReceiptHandle")

        try:
             # Validate SQS message identifiers
            if not message_id or not receipt_handle:
                raise ValueError(
                "SQS message is missing MessageId or ReceiptHandle"
                )

            # Deserialize JSON message body
            message_body = message["Body"]
            sensor = json.loads(message_body)

            logger.info(
                "Processing SQS message: message_id=%s",
                message_id
            )
            
            # Process the sensor reading
            process_sensor(sensor, message_id)

            # Delete only after successful processing
            sqs.delete_message(
                QueueUrl=SQS_QUEUE_URL,
                ReceiptHandle=receipt_handle
            )

            logger.info(
                "Successfully processed and deleted message: %s",
                message_id
            )

        except Exception:
            logger.exception(
                "Failed to process SQS message: message_id=%s",
                message_id
            )


# --------------------------------------------------
# Lambda Entry Point
# --------------------------------------------------

def lambda_handler(event, context):
    """
    AWS Lambda entry point.

    Validates configuration, polls SQS, and processes
    the received messages.
    """

    validate_configuration()

    logger.info("Starting ColdChain long-polling consumer")

    messages = receive_messages()

    if not messages:
        logger.info("No messages available in SQS")

        return {
            "statusCode": 200,
            "message": "No messages received",
            "received_count": 0
        }

    process_messages(messages)

    logger.info("Long-polling consumer invocation completed")

    return {
        "statusCode": 200,
        "message": "Message processing attempted",
        "received_count": len(messages)
    }
