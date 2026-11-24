import pytest
from unittest.mock import patch

from lambdas.common.sensor_processor import process_sensor

def test_process_sensor_success():
    sensor = {
        "sensor_id": "SENSOR-001",
        "warehouse_id": "WH-01",
        "zone": "cold-storage-A",
        "reading_type": "temperature",
        "value": -20,
        "unit": "C",
        "timestamp": "2026-11-21T10:00:00Z",
        "battery_pct": 85
    }

    message_id = "test-message-001"

    with (
        patch("lambdas.common.sensor_processor.detect_anomalies") as mock_detect,
        patch("lambdas.common.sensor_processor.save_to_s3") as mock_save,
        patch("lambdas.common.sensor_processor.send_alert") as mock_alert
    ):
        mock_detect.return_value = []
        mock_save.return_value = "2026/11/21/readings_test.jsonl"

        process_sensor(sensor, message_id)
    
        mock_detect.assert_called_once_with(sensor)
        mock_save.assert_called_once_with(sensor, message_id)
        mock_alert.assert_not_called()

def test_process_sensor_anomaly():
    sensor = {
        "sensor_id": "SENSOR-002",
        "warehouse_id": "WH-01",
        "zone": "cold-storage-A",
        "reading_type": "temperature",
        "value": -10,
        "unit": "C",
        "timestamp": "2026-11-21T10:00:00Z",
        "battery_pct": 85
    }

    message_id = "test-message-002"
    alert_message = "Temperature anomaly detected"

    with (
        patch("lambdas.common.sensor_processor.detect_anomalies") as mock_detect,
        patch("lambdas.common.sensor_processor.save_to_s3") as mock_save,
        patch("lambdas.common.sensor_processor.send_alert") as mock_alert
    ):
        mock_detect.return_value = [alert_message]
        mock_save.return_value = "2026/11/21/readings_test.jsonl"

        process_sensor(sensor, message_id)

        mock_detect.assert_called_once_with(sensor)
        mock_save.assert_called_once_with(sensor, message_id)
        mock_alert.assert_called_once_with(alert_message)

def test_process_sensor_missing_field():
    sensor = {
        "sensor_id": "SENSOR-003",
        "warehouse_id": "WH-03",
        "zone": "cold-storage-A",
        "reading_type": "temperature",
        "value": -10,
        "unit": "C",
        "timestamp": "2026-11-21T10:00:00Z"
        # battery_pct intentionally missing
    }

    message_id = "test-message-003"

    with (
        patch("lambdas.common.sensor_processor.save_to_s3") as mock_save,
        patch("lambdas.common.sensor_processor.send_alert") as mock_alert
    ):
        with pytest.raises(ValueError, match="Missing required sensor fields"):
            process_sensor(sensor, message_id)

        mock_save.assert_not_called()
        mock_alert.assert_not_called()

def test_process_sensor_s3_failure():
    sensor = {
        "sensor_id": "SENSOR-004",
        "warehouse_id": "WH-01",
        "zone": "cold-storage-A",
        "reading_type": "temperature",
        "value": -20,
        "unit": "C",
        "timestamp": "2026-11-21T10:00:00Z",
        "battery_pct": 85
    }

    message_id = "test-message-004"

    with (
        patch("lambdas.common.sensor_processor.detect_anomalies") as mock_detect,
        patch("lambdas.common.sensor_processor.save_to_s3") as mock_save,
        patch("lambdas.common.sensor_processor.send_alert") as mock_alert
    ):
        mock_detect.return_value = []
        mock_save.side_effect = Exception("Simulated S3 upload failure")

        with pytest.raises(Exception, match="Simulated S3 upload failure"):
            process_sensor(sensor, message_id)

        mock_alert.assert_not_called()
        mock_save.side_effect = Exception("Simulated S3 upload failure")

def test_process_sensor_sns_failure():
    sensor = {
        "sensor_id": "SENSOR-005",
        "warehouse_id": "WH-01",
        "zone": "cold-storage-A",
        "reading_type": "temperature",
        "value": -10,
        "unit": "C",
        "timestamp": "2026-11-21T10:00:00Z",
        "battery_pct": 85
    }

    message_id = "test-message-005"
    alert_message = "Temperature anomaly detected"

    with (
        patch("lambdas.common.sensor_processor.detect_anomalies") as mock_detect,
        patch("lambdas.common.sensor_processor.save_to_s3") as mock_save,
        patch("lambdas.common.sensor_processor.send_alert") as mock_alert
    ):
        mock_detect.return_value = [alert_message]
        mock_save.return_value = "2026/11/21/readings_test.jsonl"
        mock_alert.side_effect = Exception("Simulated SNS failure")

        with pytest.raises(Exception, match="Simulated SNS failure"):
            process_sensor(sensor, message_id)

        mock_save.assert_called_once_with(sensor, message_id)
        mock_alert.assert_called_once_with(alert_message)

def test_other_sensor_missing_battery_still_fails():
    sensor = {
        "sensor_id": "SENSOR-LP-FAIL-001",
        "warehouse_id": "WH-01",
        "zone": "cold-storage-A",
        "reading_type": "temperature",
        "value": -20,
        "unit": "C",
        "timestamp": "2026-11-23T19:45:00Z"
    }

    with pytest.raises(ValueError):
        process_sensor(sensor, "test-other-message-001")