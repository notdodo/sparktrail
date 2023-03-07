import json
import os
from configparser import ConfigParser
from hashlib import sha1
from pathlib import Path

import boto3
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession


def read_aws_config():
    config = ConfigParser()
    config.read(Path("~/.aws/config").expanduser())
    return config["profile " + os.environ["AWS_PROFILE"]]


def is_sso():
    profile = read_aws_config()
    try:
        sso_url = [i for i in profile if "sso_start_url" == i][0]
        return profile[sso_url]
    except KeyError:
        return False


def get_sso_cached_login(sso_url):
    cache = sha1(sso_url.encode()).hexdigest()
    sso_cache_file = Path(f"~/.aws/sso/cache/{cache}.json").expanduser()
    return json.loads(sso_cache_file.read_text())


def get_sso_credentials():
    if sso_url := is_sso():
        sso = boto3.session.Session(
            profile_name=os.environ["AWS_PROFILE"],
        ).client("sso")
        profile = read_aws_config()
        return sso.get_role_credentials(
            roleName=profile["sso_role_name"],
            accountId=profile["sso_account_id"],
            accessToken=get_sso_cached_login(sso_url)["accessToken"],
        )["roleCredentials"]


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
            "org.apache.hadoop:hadoop-aws:3.2.2,com.amazonaws:aws-java-sdk-bundle:1.11.888",
        )
        .set(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider",
        )
        .set("spark.hadoop.fs.s3a.access.key", credentials["accessKeyId"])
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
