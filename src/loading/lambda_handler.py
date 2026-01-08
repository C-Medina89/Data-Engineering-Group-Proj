# import json
# import logging
# import os

# from loading.db_client import WarehouseDBClient
# from loading.load_service import LoadService

# logger = logging.getLogger()
# logger.setLevel(os.getenv("LOG_LEVEL", "INFO"))


# def _get_env(name: str) -> str:
#     value = os.getenv(name)
#     if not value:
#         raise ValueError(f"Missing required env var: {name}")
#     return value


# def lambda_handler(event, context):
    
#     # Loading Lambda entry point.

#     # Expected env vars:
#     #   - PROCESSED_BUCKET_NAME: S3 bucket with processed parquet outputs

    
#     logger.info("Load Lambda triggered. event=%s", json.dumps(event))

#     processed_bucket = _get_env("PROCESSED_BUCKET_NAME")
#     checkpoints_prefix = os.getenv("LOAD_CHECKPOINTS_PREFIX", "_load_checkpoints")

#     try:
#         with WarehouseDBClient() as db:
#             service = LoadService(
#                 processed_bucket=processed_bucket,
#                 db=db,
#                 checkpoints_prefix=checkpoints_prefix,
#             )

#             target_table = None
            
#             target_table = event.get("table") if isinstance(event, dict) else None

#             if target_table:
#                 logger.info("Loading single table=%s", target_table)
#                 result = service.load_one_table(target_table)
#             else:
#                 logger.info("Loading all discovered tables from bucket=%s", processed_bucket)
#                 result = service.load_all_tables()

#         return {
#             "statusCode": 200,
#             "body": json.dumps({"message": "Loading complete", "result": result}, default=str),
#         }

#     except Exception as e:
#         logger.exception("Loading Lambda failed")
#         return {
#             "statusCode": 500,
#             "body": json.dumps({"message": "Loading failed", "error": str(e)}),
#         }
import json
import logging
import os
from loading.load_service import LoadService

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    logger.info(f"Load Lambda triggered with event: {json.dumps(event, default=str)}")
    
    try:
        processed_bucket = os.getenv("PROCESSED_BUCKET_NAME")
        if not processed_bucket:
            raise ValueError("PROCESSED_BUCKET_NAME environment variable is required")
        
        service = LoadService(processed_bucket)
        
        # Check if triggered by S3 event
        if "Records" in event and len(event["Records"]) > 0:
            record = event["Records"][0]
            if "s3" in record:
                s3_key = record["s3"]["object"]["key"]
                result = service.load_from_s3_event(s3_key)
            else:
                # If not S3 event, do full load
                result = service.load_all_tables()
        else:
            # Manual invocation or schedule - do full load
            result = service.load_all_tables()
        
        logger.info(f"Load completed: {result}")
        
        return {
            "statusCode": 200,
            "body": json.dumps({
                "message": "Warehouse load successful",
                "result": result
            })
        }
        
    except Exception as e:
        logger.exception("Load Lambda failed")
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(e)})
        }
    finally:
        if 'service' in locals():
            service.close()