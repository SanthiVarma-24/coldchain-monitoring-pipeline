
"""
Process ColdChain sensor readings received through Amazon SQS.

Delegates sensor validation, S3 storage, and SNS alerting
to the shared sensor-processing module.

Reports failed SQS message IDs for selective retry.
"""

import json
import logging
import os

from lambdas.common.sensor_processor import process_sensor


# --------------------------------------------------
# Configuration
# --------------------------------------------------

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")


# --------------------------------------------------
# Logging
# --------------------------------------------------

logger = logging.getLogger(__name__)
logger.setLevel(LOG_LEVEL.upper())


# --------------------------------------------------
# Configuration Validation
# --------------------------------------------------

def validate_configuration():
    """Ensure required AWS resource configuration is available."""

    if not os.getenv("SNS_TOPIC_ARN"):
        raise ValueError(
            "SNS_TOPIC_ARN environment variable is missing"
        )

    if not os.getenv("S3_BUCKET_NAME"):
        raise ValueError(
            "S3_BUCKET_NAME environment variable is missing"
        )


def process_record(record):
    """
    Deserialize one SQS record and delegate sensor processing
    to the shared module.

    Args:
        record (dict): SQS event record delivered by AWS Lambda.

    Raises:
        json.JSONDecodeError: If the message body is invalid JSON.
        Exception: If shared sensor processing fails.
    """
    sensor = json.loads(record["body"])

    process_sensor(sensor, record["messageId"])


def lambda_handler(event, context):
    """Process SQS records and report individual message failures."""
    validate_configuration()

    batch_failures = []

    records = event.get("Records", [])

    logger.info("Received SQS batch: count=%d", len(records))

    for record in records:
        message_id = record.get("messageId")

        try:
            process_record(record)

        except Exception:
            logger.exception(
                "SQS record processing failed: message_id=%s",
                message_id
            )

            if message_id:
                batch_failures.append({
                    "itemIdentifier": message_id
                })
            else:
                # Local tests must also provide realistic SQS message IDs.
                raise

    logger.info(
        "Batch completed: received=%d failed=%d",
        len(records),
        len(batch_failures)
    )

    return {
        "batchItemFailures": batch_failures
    }
