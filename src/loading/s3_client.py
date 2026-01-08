# import logging
# import os
# from io import BytesIO
# from typing import List, Optional
# import pandas as pd
# import boto3



# logger = logging.getLogger(__name__)
# logger.setLevel(os.getenv("LOG_LEVEL", "INFO"))

# class S3LoadingClient:
#     def __init__(self, bucket: str):
#         self.bucket_name = bucket
#         self.s3 = boto3.client("s3")
#         logger.info("Initialising S3LoadingClient. bucket=%s", self.bucket_name)

#     def list_parquet_keys(self, table_name: str) -> List[str]:


#         # Returns parquet keys under: <table_name>/
#         # Sorted by LastModified ascending (oldest -> newest).

#         prefix = f"{table_name}/"
#         paginator = self.s3.get_paginator("list_objects_v2")
#         objects = []
#         for page in paginator.paginate(Bucket=self.bucket_name, Prefix=prefix):
#             objects.extend(page.get("Contents", []))


#         parquet_objects = [
#             obj for obj in objects
#             if obj["Key"].endswith(".parquet")
#             and not obj["Key"].endswith("/")]

#         sorted_objects = sorted(parquet_objects, key=lambda x: x["LastModified"])
#         keys = [obj["Key"] for obj in sorted_objects]
#         logger.info("Found %s parquet files under prefix=%s in bucket=%s", len(keys), prefix, self.bucket_name)
        
#         return keys
    
#     def read_parquet_to_df(self, key: str) -> pd.DataFrame:
#         logger.info("Reading parquet from s3://%s/%s", self.bucket_name, key)
#         response = self.s3.get_object(Bucket=self.bucket_name, Key=key)
#         body_bytes = response["Body"].read()
#         buffer = BytesIO(body_bytes)
        
#         df = pd.read_parquet(buffer)
#         logger.info("Loaded parquet rows=%s cols=%s key=%s", len(df), len(df.columns), key)
#         return df
    
#     def read_latest_parquet(self, table_name: str) -> Optional[pd.DataFrame]:
#         #find latest parqet file for a table and read it to df

#         keys = self.list_parquet_keys(table_name)
#         if not keys:
#             logger.warning("No parquet files found for table '%s'", table_name)
#             return None

#         latest_key = keys[-1]
#         return self.read_parquet_to_df(latest_key)
import boto3
import pandas as pd
from io import BytesIO
import logging
from botocore.config import Config

logger = logging.getLogger()
logger.setLevel(logging.INFO)


class S3LoaderClient:
    def __init__(self, bucket: str):
        self.bucket = bucket
        self.s3 = boto3.client(
            "s3",
            config=Config(
                connect_timeout=5,
                read_timeout=10,
                retries={"max_attempts": 2}
            )
        )
        logger.info(f"S3LoaderClient initialized for bucket: {bucket}")

    def read_latest_parquet(self, table_prefix: str) -> pd.DataFrame:
        prefix = f"{table_prefix}/"
        logger.info(f"Looking for latest Parquet under prefix: {prefix}")

        response = self.s3.list_objects_v2(
            Bucket=self.bucket,
            Prefix=prefix,
            MaxKeys=10
        )

        contents = response.get("Contents", [])

        if not contents:
            raise FileNotFoundError(f"No objects found under {prefix}")

        parquet_files = [
            obj for obj in contents if obj["Key"].endswith(".parquet")
        ]

        if not parquet_files:
            raise FileNotFoundError(f"No parquet files under {prefix}")

        parquet_files.sort(key=lambda x: x["LastModified"], reverse=True)
        latest_key = parquet_files[0]["Key"]

        logger.info(f"Reading Parquet: {latest_key}")
        return self.read_parquet(latest_key)

    def read_parquet(self, key: str) -> pd.DataFrame:
        obj = self.s3.get_object(Bucket=self.bucket, Key=key)
        buffer = BytesIO(obj["Body"].read())
        return pd.read_parquet(buffer)
