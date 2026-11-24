#!/usr/bin/env bash
set -euo pipefail

echo "Deploying Lambda functions"

echo "Deploying data generator"
aws lambda update-function-code --function-name coldchain_data_generator --zip-file fileb://build/coldchain_data_generator.zip
aws lambda wait function-updated --function-name coldchain_data_generator

echo "Deploying coldchain_event_processor"
aws lambda update-function-code --function-name coldchain_event_processor --zip-file fileb://build/coldchain_event_processor.zip
aws lambda wait function-updated --function-name coldchain_event_processor

echo "Updating event processor handler"
aws lambda update-function-configuration --function-name coldchain_event_processor --handler lambdas.event_processor.lambda_function.lambda_handler
aws lambda wait function-updated --function-name coldchain_event_processor

echo "Deploying coldchain_long_poll_consumer"
aws lambda update-function-code --function-name coldchain_long_poll_consumer --zip-file fileb://build/coldchain_long_poll_consumer.zip
aws lambda wait function-updated --function-name coldchain_long_poll_consumer

echo "All Lambda functions deployed successfully"