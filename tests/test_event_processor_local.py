
"""Local test for the ColdChain SQS event processor."""

import json
from unittest.mock import patch

from lambdas.event_processor.lambda_function import lambda_handler

test_sensor = {
    "sensor_id": "SENS-WH01-H001",
    "warehouse_id": "WH-01",
    "zone": "loading-dock",
    "reading_type": "humidity",
    "value": 75.0,
    "unit": "percent",
    "timestamp": "2026-11-20T10:00:00Z",
    "battery_pct": 90
}

test_event = {
    "Records": [
        {
            "messageId": "local-test-001",
            "body": json.dumps(test_sensor)
        }
    ]
}


def test_event_processor():
    """Verify processing without making real AWS API calls."""

    with (
        patch(
            "lambdas.common.sensor_processor.s3.put_object"
        ) as mock_s3,
        patch(
            "lambdas.common.sensor_processor.sns.publish"
        ) as mock_sns
    ):
        mock_sns.return_value = {
            "MessageId": "mock-sns-message-001"
        }

        result = lambda_handler(test_event, None)

        assert result == {"batchItemFailures": []}
        assert mock_s3.call_count == 1
        assert mock_sns.call_count == 1

        print("Local consumer test passed")

def test_s3_upload_failure():
    """Verify that an S3 failure marks the SQS message for retry."""

    with patch(
        "lambdas.common.sensor_processor.s3.put_object"
    ) as mock_s3:

        mock_s3.side_effect = Exception("Simulated S3 upload failure")

        result = lambda_handler(test_event, None)

        assert result == {
            "batchItemFailures": [
                {"itemIdentifier": "local-test-001"}
            ]
        }

def test_sns_publish_failure():
    """Verify SNS failure marks the SQS message for retry."""

    with (
        patch(
            "lambdas.common.sensor_processor.s3.put_object"
        ) as mock_s3,
        patch(
            "lambdas.common.sensor_processor.sns.publish"
        ) as mock_sns
    ):
        mock_sns.side_effect = Exception(
            "Simulated SNS publishing failure"
        )

        result = lambda_handler(test_event, None)

        assert result == {
            "batchItemFailures": [
                {"itemIdentifier": "local-test-001"}
            ]
        }

        mock_s3.assert_called_once()
        mock_sns.assert_called_once()

def test_partial_batch_failure():
    """Verify that only failed SQS messages are reported for retry."""

    records = []

    for i in range(1, 4):
        sensor = test_sensor.copy()
        sensor["sensor_id"] = f"SENS-TEST-{i:03d}"
        sensor["value"] = 45.0  # Normal humidity, no SNS alerts

        records.append({
            "messageId": f"message-{i}",
            "body": json.dumps(sensor)
        })

    batch_event = {"Records": records}

    def mock_s3_upload(**kwargs):
        if "message-2" in kwargs["Key"]:
            raise RuntimeError("Simulated S3 failure for message 2")

        return {"ETag": "mock-etag"}

    with patch(
        "lambdas.common.sensor_processor.s3.put_object"
    ) as mock_s3:

        mock_s3.side_effect = mock_s3_upload

        result = lambda_handler(batch_event, None)

        assert result == {
            "batchItemFailures": [
                {"itemIdentifier": "message-2"}
            ]
        }

        assert mock_s3.call_count == 3

def test_invalid_json():
    """Verify malformed JSON is reported as a failed SQS message."""

    invalid_event = {
        "Records": [
            {
                "messageId": "invalid-json-001",
                "body": '{"sensor_id": "SENS-001", "value": 75.0'
            }
        ]
    }

    with patch(
        "lambdas.common.sensor_processor.s3.put_object"
    ) as mock_s3:

        result = lambda_handler(invalid_event, None)

        assert result == {
            "batchItemFailures": [
                {"itemIdentifier": "invalid-json-001"}
            ]
        }

        mock_s3.assert_not_called()

def test_missing_required_fields():
    """Verify incomplete sensor data is reported as failed."""

    incomplete_sensor = {
        "sensor_id": "SENS-001",
        "warehouse_id": "WH-01",
        "value": 75.0
    }

    event = {
        "Records": [
            {
                "messageId": "missing-fields-001",
                "body": json.dumps(incomplete_sensor)
            }
        ]
    }

    with patch(
        "lambdas.common.sensor_processor.s3.put_object"
    ) as mock_s3:

        result = lambda_handler(event, None)

        assert result == {
            "batchItemFailures": [
                {"itemIdentifier": "missing-fields-001"}
            ]
        }

        mock_s3.assert_not_called()

if __name__ == "__main__":
    test_event_processor()