# SparkTrail [![CodeQL](https://github.com/notdodo/SparkTrail/actions/workflows/codeql.yml/badge.svg)](https://github.com/notdodo/SparkTrail/actions/workflows/codeql.yml)

Use this Python script to start a Spark standalone session to interact with the CloudTrail bucket.
Spark allow to query the logs using a SQL-like syntax.

The startup `main.py` script will automatically load SSO credentials and set AWS temporary credentials from the SSO to authenticated to the bucket.

## Usage

0. Spawn Poetry shell:

`poetry shell`

1. Start the cluster:

`PYSPARK_DRIVER_PYTHON=ipython PYTHONSTARTUP=main.py pyspark --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 --name SparkTrail`

2. Now the environment is configured and to start running queries link the S3 bucket. When the IPython shell is return link the bucket and start performing queries:

```python
spark = link_s3("s3a://audit-cloudtrail-logs/AWSLogs/")
spark.select("Records.eventName").distinct().show(10)
```
