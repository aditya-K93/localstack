#!/bin/bash

# Configure AWS SDK to use LocalStack
export AWS_PAGER=""
export AWS_ACCESS_KEY_ID=test
export AWS_SECRET_ACCESS_KEY=test
export AWS_REGION=us-east-1

# Build and run the app with SBT, pointing to LocalStack

sbt "runMain com.github.adityak93.kinesis.Main"
echo "Application started. Check 'output.txt' for results."
