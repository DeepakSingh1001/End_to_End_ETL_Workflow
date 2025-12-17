from io import StringIO

import boto3
import pandas as pd

BUCKET_NAME = "deepaks-source-bucket"
RAW_PREFIX = "RawData/"
VALIDATED_PREFIX = "Validated/"

s3 = boto3.client("s3")

REQUIRED_COLUMNS = [
    "order_id",
    "product_id",
    "quantity",
    "price",
    "city",
    "order_timestamp",
]


def validate_file(key):
    obj = s3.get_object(Bucket=BUCKET_NAME, Key=key)
    df = pd.read_csv(obj["Body"])

    # Schema validation
    for col in REQUIRED_COLUMNS:
        if col not in df.columns:
            raise ValueError(f"Missing column {col} in {key}")

    # Drop nulls
    df = df.dropna()

    # Business rules
    df = df[df["quantity"] > 0]
    df = df[df["price"] > 0]

    # Timestamp validation
    df["order_timestamp"] = pd.to_datetime(df["order_timestamp"], errors="coerce")
    df = df.dropna(subset=["order_timestamp"])

    return df


def validate_all_files():
    response = s3.list_objects_v2(Bucket=BUCKET_NAME, Prefix=RAW_PREFIX)

    if "Contents" not in response:
        raise ValueError("No files found in RawData/")

    for obj in response["Contents"]:
        key = obj["Key"]

        if not key.endswith(".csv"):
            continue

        print(f"Validating {key}")

        df_clean = validate_file(key)

        # Write to Validated/
        output_key = key.replace(RAW_PREFIX, VALIDATED_PREFIX)

        buffer = StringIO()
        df_clean.to_csv(buffer, index=False)

        s3.put_object(Bucket=BUCKET_NAME, Key=output_key, Body=buffer.getvalue())

        print(f"✅ Saved validated file: {output_key}")


if __name__ == "__main__":
    validate_all_files()
