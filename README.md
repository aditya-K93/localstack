
---

# Kinesis Aggregator LocalStack Test

LocalStack docker setup with Scala 3 and Cats Effect to test full-fledged kinesis stream aggregation

## Features

- **Local Testing**: Scripts for running with LocalStack.
- **Error Handling**: Graceful error logging, retries on disconnects
- **Concurrency**: Asynchronous processing 

## Prerequisites

- **Scala**: 3.x
- **SBT**: 1.9.x
- **Docker**: For LocalStack
- **AWS CLI**: For LocalStack interaction
- **Java**: 21 (Adoptium recommended)

## Setup

1. **Clone the Repository**:
   ```bash
   git clone https://github.com/aditya-K93/localstack.git
   cd localstack
   ```

2. **Install Dependencies**:
   - Ensure `sbt` is installed.
   - Run `sbt compile` to fetch dependencies (listed in `build.sbt`).

3. **Set Up LocalStack**:
   - Ensure Docker is running.
   - Use the provided scripts to start LocalStack and create a Kinesis stream:
     ```bash
     chmod +x *.sh
     ./start-localstack.sh
     ```

## Usage

### Running Locally with LocalStack


1. **Run the Application**:
   - Execute the app:
     ```bash
     sbt "runMain com.github.adityak93.kinesis.Main"
     ```

2. **Populate the Stream**:
   - Add test data to the Kinesis stream:
     ```bash
     ./populate-stream.sh
     ```
3. **Verify logs**:
    - Check `output.txt` for aggregated results (e.g., running sums: `0, 1, 3, 6, ...`).

3. **Full Test**:

   - Run the end-to-end test script:
     ```bash
     ./test-local.sh
     ```
   - Stops LocalStack after execution.

### Configuration
- **Stream Details**: Edit `Main.scala` to change `streamName`, `shardId`, or `outputFile` in `KinesisApp.Config`.
- **Logging**: Customize `src/main/resources/logback.xml` for log levels or output patterns.

## Project Structure

```
localstack/
├── src/
│   ├── main/
│   │   ├── scala/
│   │   │   └── com/github/adityak93/kinesis/
│   │   │       ├── app/
│   │   │       │   ├── AppConfig.scala
│   │   │       │   └── KinesisApp.scala
│   │   │       ├── domain/
│   │   │       │   └── Domain.scala
│   │   │       ├── kinesis/
│   │   │       │   └── KinesisClient.scala
│   │   │       ├── Main.scala
│   │   │       ├── sink/
│   │   │       │   └── StatsWriter.scala
│   │   │       └── stream/
│   │   │           └── KinesisConsumer.scala
│   │   └── resources/
│   │       └── logback.xml
│   └── test/
├── build.sbt
├── start-localstack.sh
├── populate-stream.sh
├── run-app.sh
├── test-local.sh
└── README.md
```

## Logging

```
2025-02-20 05:10:12.345 ERROR [main] com.github.adityak93.kinesis.Main - Unable to load region...
```
- Configured in `logback.xml` with timestamp, level, thread, logger name, and message.

## Error Handling

- Errors (e.g., missing region) are logged with no stack trace.
- Exits with code 1 on failure, configurable in `Main.run`.

## License

This project is licensed under the [MIT License](LICENSE) (see the standalone `LICENSE` file).

---
