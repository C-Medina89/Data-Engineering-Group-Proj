import json
from src.ingestion.lambda_handler import lambda_handler
from src.ingestion.ingest_service import IngestionService

def test_lambda_handler(mocker):

    mocker.patch("os.getenv", return_value="test_bucket")

    mock_service =  mocker.patch("src.ingestion.lambda_handler.IngestionService")

    fake_instance = mock_service.return_value
    fake_instance.ingest_table_preview.return_value = {

        "table": "staff",
        "rows": 2,
        "s3_key": "staff//raw.json"
    }

    event = {"table": "staff"}

    response = lambda_handler(event, None)
    body = json.loads(response["body"])

    assert response["statusCode"] == 200
    assert body["result"]["table"] == "staff"
    assert fake_instance.ingest_table_preview.called



