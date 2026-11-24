import json
from unittest.mock import call, patch

from lambdas.long_poll_consumer.lambda_function import (
    SQS_QUEUE_URL,
    lambda_handler,
    process_messages,
    receive_messages,
)


def test_successful_message_processing():
    sensor = {
        "sensor_id": "SENSOR-001",
        "warehouse_id": "WH-01",
        "zone": "cold-storage-A",
        "reading_type": "temperature",
        "value": -20,
        "unit": "C",
        "timestamp": "2026-11-23T10:00:00Z",
        "battery_pct": 85
    }

    message = {
        "MessageId": "test-message-001",
        "ReceiptHandle": "test-receipt-handle-001",
        "Body": json.dumps(sensor)
    }

    with patch(
        "lambdas.long_poll_consumer.lambda_function.process_sensor"
    ) as mock_process, patch(
        "lambdas.long_poll_consumer.lambda_function.sqs.delete_message"
    ) as mock_delete:

        process_messages([message])

        mock_process.assert_called_once_with(
            sensor, "test-message-001"
        )

        mock_delete.assert_called_once_with(
            QueueUrl=SQS_QUEUE_URL,
            ReceiptHandle="test-receipt-handle-001"
        )

def test_failed_message_not_deleted():
    sensor = {
        "sensor_id": "SENSOR-001",
        "warehouse_id": "WH-01",
        "zone": "cold-storage-A",
        "reading_type": "temperature",
        "value": -20,
        "unit": "C",
        "timestamp": "2026-11-23T10:00:00Z",
        "battery_pct": 85
    }
    
    message = {
        "MessageId": "test-message-001",
        "ReceiptHandle": "test-receipt-handle-001",
        "Body": json.dumps(sensor)
    }

    with patch(
        "lambdas.long_poll_consumer.lambda_function.process_sensor"
    ) as mock_process, patch(
        "lambdas.long_poll_consumer.lambda_function.sqs.delete_message"
    ) as mock_delete:       
         
        mock_process.side_effect = ValueError("Simulated processing failure")

        process_messages([message])

        mock_delete.assert_not_called()

def test_mixed_message_processing():
    sensor = {
        "sensor_id": "SENSOR-001",
        "warehouse_id": "WH-01",
        "zone": "cold-storage-A",
        "reading_type": "temperature",
        "value": -20,
        "unit": "C",
        "timestamp": "2026-11-23T10:00:00Z",
        "battery_pct": 85
    }
      
    messages = [
    {
        "MessageId": "test-message-001",
        "ReceiptHandle": "test-receipt-handle-001",
        "Body": json.dumps(sensor)
    },

    {
        "MessageId": "test-message-002",
        "ReceiptHandle": "test-receipt-handle-002",
        "Body": json.dumps(sensor)
    },

    {
        "MessageId": "test-message-003",
        "ReceiptHandle": "test-receipt-handle-003",
        "Body": json.dumps(sensor)
    }]

    with patch(
        "lambdas.long_poll_consumer.lambda_function.process_sensor"
    ) as mock_process, patch(
        "lambdas.long_poll_consumer.lambda_function.sqs.delete_message"
    ) as mock_delete:
    
        def simulate_processing(sensor, MessageId):
            if MessageId == "test-message-002":
                raise ValueError ("Simulated processing failure")

        mock_process.side_effect = simulate_processing

        process_messages(messages)

        assert mock_process.call_count == 3
        assert mock_delete.call_count == 2

        mock_delete.assert_has_calls([
            call(
            QueueUrl=SQS_QUEUE_URL,
            ReceiptHandle="test-receipt-handle-001"
        ),
            call(
                QueueUrl=SQS_QUEUE_URL,
                ReceiptHandle="test-receipt-handle-003"
        )
    ])

def test_receive_messages():

    with patch(
        "lambdas.long_poll_consumer.lambda_function.sqs.receive_message"
    ) as mock_receive:

        mock_receive.return_value = {
            "Messages": [
            {
                "MessageId": "test-message-001",
                "ReceiptHandle": "test-receipt-001",
                "Body": "{}"
            }
            ]
        }

        messages = receive_messages()

        assert len(messages) == 1
        mock_receive.assert_called_once_with(
            QueueUrl=SQS_QUEUE_URL,
            MaxNumberOfMessages=5,
            WaitTimeSeconds=10,
            MessageSystemAttributeNames=["ApproximateReceiveCount"]
            )

def test_receive_messages_empty_queue():
    with patch(
        "lambdas.long_poll_consumer.lambda_function.sqs.receive_message"
    ) as mock_receive:

        mock_receive.return_value = {}

        messages = receive_messages()

        assert messages == []
        mock_receive.assert_called_once()

def test_lambda_handler_empty_queue():

    with patch(
        "lambdas.long_poll_consumer.lambda_function.validate_configuration"
    ) as mock_validate, patch(
        "lambdas.long_poll_consumer.lambda_function.receive_messages"
    ) as mock_receive, patch(
        "lambdas.long_poll_consumer.lambda_function.process_messages"
    ) as mock_process:

        mock_receive.return_value = []

        response = lambda_handler({}, None)

        assert response["statusCode"] == 200
        assert response["received_count"] == 0

        mock_validate.assert_called_once()
        mock_receive.assert_called_once()
        mock_process.assert_not_called()

def test_lambda_handler_with_messages():

    message = {
        "MessageId": "test-message-001",
        "ReceiptHandle": "test-receipt-001",
        "Body": "{}"
    }

    with patch(
        "lambdas.long_poll_consumer.lambda_function.validate_configuration"
    ) as mock_validate, patch(
        "lambdas.long_poll_consumer.lambda_function.receive_messages"
    ) as mock_receive, patch(
        "lambdas.long_poll_consumer.lambda_function.process_messages"
    ) as mock_process:

        mock_receive.return_value = [message]

        response = lambda_handler({}, None)

        assert response["statusCode"] == 200
        assert response["received_count"] == 1

        mock_validate.assert_called_once()
        mock_receive.assert_called_once()
        mock_process.assert_called_once_with([message])
                

                    