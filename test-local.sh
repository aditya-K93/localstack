#!/bin/bash

export AWS_PAGER=""

# Start LocalStack and create the stream
./start-localstack.sh || exit 1

# Populate the stream
./populate-stream.sh || exit 1

# Run the app
./run-app.sh || exit 1 &

# Wait for some output
sleep 10

# Check the output file
cat output.txt

# Clean up: Stop LocalStack
docker stop $(docker ps -q --filter ancestor=localstack/localstack)

echo "Test completed."
