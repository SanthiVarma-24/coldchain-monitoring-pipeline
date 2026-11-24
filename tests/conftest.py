import os

# Configure DUMMY AWS resources for unit testing
os.environ["AWS_DEFAULT_REGION"] = "ap-south-2"
os.environ["AWS_REGION"] = "ap-south-2"
os.environ["AWS_ACCESS_KEY_ID"] = "testing"
os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
os.environ["AWS_SESSION_TOKEN"] = "testing"
os.environ["AWS_EC2_METADATA_DISABLED"] = "true"

os.environ["SNS_TOPIC_ARN"] = (
    "arn:aws:sns:ap-south-2:123456789012:test-alerts"
)
os.environ["S3_BUCKET_NAME"] = "test-sensor-readings"
os.environ["SQS_QUEUE_URL"] = (
    "https://sqs.ap-south-2.amazonaws.com/123456789012/test-queue"
)
os.environ["LOG_LEVEL"] = "INFO"