# Cloud ETL & Streaming Data Pipeline

A portfolio implementation of a distributed data pipeline using Python, PySpark, Apache Spark, and AWS event-driven services.

## Architecture

```text
Raw data -> Amazon S3 -> Lambda event handler -> Amazon SQS
                    \
                     -> PySpark ETL -> validation -> deduplication -> Parquet -> curated S3
                                                                    \
                                                                     -> RDS / analytics
```

## Tech Stack

- Python
- Apache Spark / PySpark
- Amazon S3
- AWS Lambda
- Amazon SQS
- Amazon RDS-compatible downstream persistence

## Features

- Distributed CSV ingestion and transformation with PySpark.
- Required-field validation and malformed-record filtering.
- Duplicate removal using business identifiers.
- Timestamp normalization and ingestion metadata.
- Partitioned Parquet output for downstream analytics.
- S3-triggered Lambda handler that publishes processing work to SQS.
- Retry/DLQ-friendly event-driven design.

## Repository Structure

```text
.
├── README.md
├── etl_job.py
├── lambda_handler.py
├── requirements.txt
├── sample_data/
│   └── transactions.csv
└── tests/
    └── test_lambda_handler.py
```

## Run Locally

Install dependencies:

```bash
python -m pip install -r requirements.txt
```

Run the Spark ETL job:

```bash
spark-submit etl_job.py \
  --input sample_data/transactions.csv \
  --output output/curated
```

For AWS execution, use S3 URIs instead of local paths:

```bash
spark-submit etl_job.py \
  --input s3://your-bucket/raw/ \
  --output s3://your-bucket/curated/
```

## Lambda Configuration

Set:

```text
PROCESSING_QUEUE_URL=<your-sqs-queue-url>
```

The Lambda function accepts standard S3 event notifications and publishes one SQS message per uploaded object.

## Testing

```bash
python -m unittest discover -s tests
```

## Portfolio Scope

This implementation demonstrates the architecture and technologies described for the Cloud ETL & Streaming Data Pipeline project on my resume.