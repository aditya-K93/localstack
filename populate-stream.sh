#!/bin/bash
export AWS_PAGER=""

# Set AWS CLI to use LocalStack
export AWS_ACCESS_KEY_ID=test
export AWS_SECRET_ACCESS_KEY=test
export AWS_DEFAULT_REGION=us-east-1

# Populate the stream with sample JSON events
for i in {1..10}; do
  data=$(jq -n --arg v "$i" '{value: $v}' | jq -r @base64) # Encode JSON
  aws --endpoint-url=http://localhost:4566 kinesis put-record \
    --stream-name my-stream \
    --data "$data" \
    --partition-key "partition1"
done

echo "Populated 'my-stream' with 10 events."
