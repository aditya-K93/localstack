#!/bin/bash

export AWS_PAGER=""

# Stop any running LocalStack containers
echo "Stopping any existing LocalStack containers..."
docker stop $(docker ps -q --filter ancestor=localstack/localstack) 2>/dev/null || true

# Kill any running sbt or java processes related to the app
echo "Stopping any running sbt or java processes..."
pkill -f "sbt.*runMain" 2>/dev/null || true
pkill -f "java.*kinesis.Main" 2>/dev/null || true

# Clear output before starting test
rm -rf output.txt

# Start LocalStack and create the stream
echo "Starting LocalStack..."
./start-localstack.sh || exit 1

# Start the consumer in the background with LATEST
echo "Starting consumer in the background..."
./run-app.sh || exit 1 &
APP_PID=$!

# Wait for consumer to initialize (compile + start)
echo "Waiting 10 seconds for consumer to initialize..."
sleep 10

# Send 10 events to the stream
echo "Sending 10 events to the stream..."
./populate-stream.sh || exit 1

# Wait for the app to process all events
echo "Waiting 12 seconds for app to process all events..."
sleep 12

# Display aggregated result
echo "Aggregated result:"
cat output.txt

# Stop the app and its JVM
echo -e "\nStopping the app and its JVM (PID: $APP_PID)..."
kill -9 $APP_PID 2>/dev/null || true
# Kill all Java processes
ps -eo pid,comm | grep -v grep | grep java | awk '{print $1}' | xargs kill -9 2>/dev/null || true

# Wait to ensure all processes terminate
sleep 2

# Clean up: Stop LocalStack
docker stop $(docker ps -q --filter ancestor=localstack/localstack) 2>/dev/null || true

echo "Test completed."
