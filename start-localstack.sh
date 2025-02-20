#!/bin/bash

export AWS_PAGER=""
# Start LocalStack in Docker
docker run -d -p 4566:4566 -e "SERVICES=kinesis" localstack/localstack:latest

# Wait for LocalStack to start
sleep 5

# Configure AWS CLI to use LocalStack endpoint
export AWS_ACCESS_KEY_ID=test
export AWS_SECRET_ACCESS_KEY=test
export AWS_DEFAULT_REGION=us-east-1

# Create a Kinesis stream
aws --endpoint-url=http://localhost:4566 kinesis create-stream --stream-name my-stream --shard-count 1

# Wait for the stream to become active
aws --endpoint-url=http://localhost:4566 kinesis wait stream-exists --stream-name my-stream

echo "LocalStack started and Kinesis stream 'my-stream' created."
