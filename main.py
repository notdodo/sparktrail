import json
import os
from configparser import ConfigParser
from hashlib import sha1
from pathlib import Path

import boto3
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession


def get_sso_credentials():
    profile_name = os.environ["AWS_PROFILE"]
    boto3.setup_default_session(profile_name=profile_name)
    session = boto3.session.Session()
    creds = session.get_credentials().get_frozen_credentials()
    return {
        "accessKey": creds.access_key,
        "secretAccessKey": creds.secret_key,
        "sessionToken": creds.token,
    }


def configure_spark():
    credentials = get_sso_credentials()
    conf = (
        SparkConf()
        .set("spark.sql.debug.maxToStringFields", 10000)
        .set(
            "spark.hadoop.fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem",
        )
        .set("spark.hadoop.com.amazonaws.services.s3.enableV4", "true")
        .set(
            "spark.jars.packages",
            "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262",
        )
        .set(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider",
        )
        .set("spark.hadoop.fs.s3a.access.key", credentials["accessKey"])
        .set("spark.hadoop.fs.s3a.secret.key", credentials["secretAccessKey"])
        .set("spark.hadoop.fs.s3a.session.token", credentials["sessionToken"])
    )
    sc = SparkContext.getOrCreate()
    sc.stop()
    sc = SparkContext(conf=conf)
    return SparkSession(sc)


def link_s3(bucket):
    spark = configure_spark()
    collect = spark.read.format("json").load(f"s3a://{bucket}")
    return collect
