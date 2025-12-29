import os
import json
import pytest
from transformation.lambda_handler import lambda_handler
from transformation.transform_service import TransformService


def test_lambda_handler_calls_run(monkeypatch):
    # Import your handler module
    import transformation.lambda_handler as lh

    # Set env vars
    monkeypatch.setenv("LANDING_BUCKET_NAME", "landing_bucket")
    monkeypatch.setenv("PROCESSED_BUCKET_NAME", "processed_bucket")

    # Fake TransformService that tracks calls
    calls = {"init": None, "run_single_table": 0}

    class FakeTransformService:
        def __init__(self, ingest_bucket: str, processed_bucket: str):
            calls["init"] = (ingest_bucket, processed_bucket)

        def run_single_table(self, table_name: str):
            calls["run_single_table"] = table_name
            return {"status": "Succes"}
        
    # Patch TransformService used in the handler module namespace
    monkeypatch.setattr(lh, "TransformService", FakeTransformService)

    # Minimal S3 event (can be any key if you don't guard)
    # event = {"Records": [{"s3": {"object": {"key": "ingest/_SUCCESS"}}}]}

    event = {"Records": [{"s3": {"object": {"key": "sales_order/raw_2025.json"}}}]}

    resp = lh.lambda_handler(event, None)

    assert calls["init"] == ('landing_bucket', 'processed_bucket')
    assert calls["run_single_table"] == "sales_order"
    assert resp["statusCode"] == 200
