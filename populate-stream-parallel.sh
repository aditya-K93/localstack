#!/bin/bash
export AWS_PAGER=""
export AWS_ACCESS_KEY_ID=test
export AWS_SECRET_ACCESS_KEY=test
export AWS_DEFAULT_REGION=us-east-1

# Function to send a single record with a unique partition key
send_record() {
    local data="$1"
    local partition_key="$2"
    aws --endpoint-url=http://localhost:4566 kinesis put-record \
        --stream-name my-stream \
        --data "$data" \
        --partition-key "$partition_key" > /dev/null 2>&1 &
}

num_shards=5

for i in {1..100}; do
    partition_key=$((i % num_shards))
    data=$(jq -n --arg v "$i" '{value: $v}' | jq -r @base64)
    send_record "$data" "$partition_key"
    sleep 0.01  # Optional: Add a tiny sleep to prevent overwhelming
done

# Wait for all background processes to complete
wait

echo "Populated 'my-stream' with 1000 events across multiple shards."