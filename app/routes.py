import json
from flask import request
import pandas as pd
import boto3
import botocore
import flatten_json
from app import app
from app.helpers import read_json, transform, dump_data, validate_data
s3_client = boto3.client("s3")
s3_resource = boto3.resource("s3")
s3_bucket = s3_resource.Bucket("refynedata")


@app.route("/etl", methods=["POST"])
def index():
    request_json = request.get_json()
    bucket = request_json.get("bucket")
    path = request_json.get("path")
    file = read_json(s3_client, bucket, path)
    df = transform(file)
    df = validate_data(df)
    print(df)
    df.to_csv("data.csv", index=False)
    status = dump_data("data.csv")
    return {"status": status}
