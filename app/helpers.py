import json
import botocore
import flatten_json
import pandas as pd
import psycopg2
import csv


def read_json(s3_client, bucket, path):
    file = None
    try:
        obj = s3_client.get_object(Bucket=bucket, Key=path)
        file = json.loads(obj["Body"].read())
    except botocore.exceptions.ClientError as ce:
        if ce.response["Error"]["Code"] in [
            "NoSuchKey",
            "NoSuchBucket",
            "AccessDenied",
            "InvalidAccessKeyId",
        ]:
            pass
    return file


def transform(file):
    mod_file = []
    for doc in file:
        userstatuslogs = doc["userstatuslogs"]
        df = pd.json_normalize(userstatuslogs)
        df["id"] = doc["id"]
        df["name"] = doc["name"]
        df["address"] = doc["address"]
        df["age"] = doc["age"]
        df["salary"] = doc["salary"]
        mod_file.append(df)
    output = pd.concat(mod_file, ignore_index=True)
    output = output[
        [
            "id",
            "name",
            "age",
            "address",
            "salary",
            "status",
            "in_ts",
            "out_ts"
        ]
    ]
    return output


def dump_data(path):
    conn = psycopg2.connect(
        host="refynedb.c46okv2nkjrd.ap-south-1.rds.amazonaws.com",
        dbname="Refyne",
        user="ajay",
        password="ajayunited9",
        port=5432
    )
    cur = conn.cursor()
    with open(path, "r") as f:
        reader = csv.reader(f, delimiter=',')
        next(reader)
        for row in reader:
            cur.execute("INSERT INTO refyne VALUES (%s, %s, %s, %s, %s, %s, %s, %s)", row)
            conn.commit()
    cur.close()
    conn.close()
    return 1


def validate_data(df):
    idxes = []
    for index, row in df.iterrows():
        if ((row["age"] < 0) or (row["age"] > 120)):
            idxes.append(index)
        if (row["out_ts"] < row["in_ts"]):
            idxes.append(index)
    df.drop(idxes, inplace=True)
    return df
