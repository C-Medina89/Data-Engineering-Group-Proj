import pandas as pd
import pytest
import numpy as np
from datetime import date, time
from pprint import pprint

class FakeS3TransformationClient:
    """
    Fake replacement for S3TransformationClient used by TransformService.
    Stores data per bucket; captures parquet writes.
    """
    data = {}     # {bucket: {table_name: DataFrame}}
    writes = {}   # {bucket: {output_table: DataFrame}}
    read_calls = []  # [(bucket, table_name)]

    def __init__(self, bucket: str):
        self.bucket = bucket
        FakeS3TransformationClient.data.setdefault(bucket, {})
        FakeS3TransformationClient.writes.setdefault(bucket, {})

    def read_table(self, table_name: str) -> pd.DataFrame:
        FakeS3TransformationClient.read_calls.append((self.bucket, table_name))
        return FakeS3TransformationClient.data[self.bucket][table_name].copy()

    def write_parquet(self, table_name: str, df: pd.DataFrame):
        FakeS3TransformationClient.writes[self.bucket][table_name] = df.copy()
        return f"{table_name}/processed_TEST.parquet"


@pytest.fixture
def seeded_service(monkeypatch):
    """
    Seeds landing bucket with minimal tables needed to build your dims + facts.
    Monkeypatches TransformService to use FakeS3TransformationClient.
    """
    import transformation.transform_service as ts_mod

    # Patch the client class used inside transform_service.py
    monkeypatch.setattr(ts_mod, "S3TransformationClient", FakeS3TransformationClient)

    landing = "landing-bucket"
    processed = "processed-bucket"

    # reset fake state
    FakeS3TransformationClient.data = {landing: {}, processed: {}}
    FakeS3TransformationClient.writes = {landing: {}, processed: {}}
    FakeS3TransformationClient.read_calls = []

    # ---- seed ingest tables ----
    FakeS3TransformationClient.data[landing]["currency"] = pd.DataFrame(
        [{"currency_id": 1, "currency_code": "GBP"}]
    )
    FakeS3TransformationClient.data[landing]["department"] = pd.DataFrame(
        [{"department_id": 10, "department_name": "Sales"}]
    )
    FakeS3TransformationClient.data[landing]["staff"] = pd.DataFrame(
        [{
            "staff_id": 100,
            "first_name": "Ada",
            "last_name": "Lovelace",
            "department_id": 10,
            "location": "London",
            "email_address": "ada@example.com",
        }]
    )
    FakeS3TransformationClient.data[landing]["address"] = pd.DataFrame(
        [{
            "address_id": 500,
            "address_line_1": "1 Main St",
            "address_line_2": "",
            "district": "Central",
            "city": "London",
            "postal_code": "SW1A 1AA",
            "country": "UK",
            "phone": "000",
        }]
    )
    FakeS3TransformationClient.data[landing]["counterparty"] = pd.DataFrame(
        [{"counterparty_id": 200, "counterparty_legal_name": "Acme", "legal_address_id": 500}]
    )
    FakeS3TransformationClient.data[landing]["design"] = pd.DataFrame(
        [{"design_id": 300, "design_name": "Poster", "file_location": "/x", "file_name": "p.pdf"}]
    )
    FakeS3TransformationClient.data[landing]["payment_type"] = pd.DataFrame(
        [{"payment_type_id": 400, "payment_type_name": "Card"}]
    )
    FakeS3TransformationClient.data[landing]["transaction"] = pd.DataFrame(
        [{"transaction_id": 900, "transaction_type": "sale", "sales_order_id": 1000, "purchase_order_id": None}]
    )
    FakeS3TransformationClient.data[landing]["sales_order"] = pd.DataFrame(
        [{
            "sales_order_id": 1000,
            "created_at": "2024-01-01T10:00:00Z",
            "last_updated": "2024-01-02T10:00:00Z",
            "staff_id": 100,
            "counterparty_id": 200,
            "units_sold": 2,
            "unit_price": 10.0,
            "currency_id": 1,
            "design_id": 300,
            "agreed_delivery_location_id": 500,
            "agreed_delivery_date": "2024-01-10",
            "agreed_payment_date": "2024-01-05",
        }]
    )
    FakeS3TransformationClient.data[landing]["purchase_order"] = pd.DataFrame(
        [{
            "purchase_order_id": 7000,
            "created_at": "2024-01-03T09:00:00Z",
            "last_updated": "2024-01-04T09:30:00Z",
            "staff_id": 100,
            "counterparty_id": 200,
            "item_code": "SKU1",
            "item_quantity": 5,
            "item_unit_price": 3.0,
            "currency_id": 1,
            "agreed_delivery_date": "2024-01-12",
            "agreed_payment_date": "2024-01-06",
            "agreed_delivery_location_id": 500,
        }]
    )
    FakeS3TransformationClient.data[landing]["payment"] = pd.DataFrame(
        [{
            "payment_id": 8000,
            "created_at": "2024-01-05T08:00:00Z",
            "last_updated": "2024-01-05T08:30:00Z",
            "transaction_id": 900,
            "counterparty_id": 200,
            "payment_amount": 20.0,
            "currency_id": 1,
            "payment_type_id": 400,
            "paid": True,
            "payment_date": "2024-01-05",
        }]
    )

    from transformation.transform_service import TransformService
    return TransformService(ingest_bucket=landing, processed_bucket=processed), landing, processed


def test_make_dim_transaction_columns(seeded_service):
    service, _, _ = seeded_service
    df = service.make_dim_transaction()
    assert list(df.columns) == ["transaction_id", "transaction_type", "sales_order_id", "purchase_order_id"]


def test_cache_reads_only_once(seeded_service):
    service, _, _ = seeded_service

    _ = service._get_ingest_table("currency")
    _ = service._get_ingest_table("currency")

    calls = [c for c in FakeS3TransformationClient.read_calls if c[1] == "currency"]
    assert len(calls) == 1


def test_run_writes_all_required_tables(seeded_service):
    service, _, processed = seeded_service

    service.run()

    written = set(FakeS3TransformationClient.writes[processed].keys())
    required = {
        "fact_sales_order",
        "fact_purchase_orders",
        "fact_payment",
        "dim_transaction",
        "dim_staff",
        "dim_payment_type",
        "dim_location",
        "dim_design",
        "dim_date",
        "dim_currency",
        "dim_counterparty",
    }
    assert required.issubset(written), f"Missing: {required - written}"



# TESTING FOR CORRECT TYPES IN EACH TABLE

def test_dim_location_value_types(seeded_service):
    service, _, _ = seeded_service
    df = service.make_dim_location()

    assert isinstance(df.loc[0, "location_id"], (int, np.integer))
    assert isinstance(df.loc[0, "address_line_1"], str)
    assert isinstance(df.loc[0, "city"], str)
    assert isinstance(df.loc[0, "postal_code"], str)

def test_dim_currency_value_types(seeded_service):
    service, _, _ = seeded_service
    df = service.make_dim_currency()

    assert isinstance(df.loc[0, "currency_id"], (int, np.integer))
    assert isinstance(df.loc[0, "currency_code"], str)


def test_dim_staff_value_types(seeded_service):
    service, _, _ = seeded_service
    df = service.make_dim_staff()

    assert isinstance(df.loc[0, "staff_id"], (int, np.integer))
    assert isinstance(df.loc[0, "first_name"], str)
    assert isinstance(df.loc[0, "last_name"], str)

    val = df.loc[0, "department_name"]
    assert isinstance(val, str) or pd.isna(val)

    assert isinstance(df.loc[0, "location"], str)
    assert isinstance(df.loc[0, "email_address"], str)


def test_dim_counterparty_value_types_all_present(seeded_service):
    service, landing, _ = seeded_service

    address_df = pd.DataFrame([{
        "address_id": 1,
        "address_line_1": "1 Main St",
        "address_line_2": "Suite 10",
        "district": "Central",
        "city": "London",
        "postal_code": "SW1A 1AA",
        "country": "UK",
        "phone": "0123456789",
    }]).set_index("address_id", drop= False)   # <-- IMPORTANT: join matches to right index

    FakeS3TransformationClient.data[landing]["address"] = address_df

    FakeS3TransformationClient.data[landing]["counterparty"] = pd.DataFrame([{
        "counterparty_id": 100,
        "counterparty_legal_name": "Acme Ltd",
        "legal_address_id": 1,     # <-- matches address_df index
    }])

    df = service.make_dim_counterparty()

    assert isinstance(df.loc[0, "counterparty_id"], (int, np.integer))
    assert isinstance(df.loc[0, "counterparty_legal_name"], str)
    assert isinstance(df.loc[0, "counterparty_legal_address_line_1"], str)
    assert isinstance(df.loc[0, "counterparty_legal_address_line_2"], str)
    assert isinstance(df.loc[0, "counterparty_legal_district"], str)
    assert isinstance(df.loc[0, "counterparty_legal_city"], str)
    assert isinstance(df.loc[0, "counterparty_legal_postal_code"], str)
    assert isinstance(df.loc[0, "counterparty_legal_country"], str)
    assert isinstance(df.loc[0, "counterparty_legal_phone_number"], str)


def test_dim_design_value_types(seeded_service):
    service, _, _ = seeded_service
    df = service.make_dim_design()

    assert isinstance(df.loc[0, "design_id"], (int, np.integer))
    assert isinstance(df.loc[0, "design_name"], str)
    assert isinstance(df.loc[0, "file_location"], str)
    assert isinstance(df.loc[0, "file_name"], str)


def test_dim_payment_type_value_types(seeded_service):
    service, _, _ = seeded_service
    df = service.make_dim_payment_type()

    assert isinstance(df.loc[0, "payment_type_id"], (int, np.integer))
    assert isinstance(df.loc[0, "payment_type_name"], str)


def test_dim_transaction_value_types(seeded_service):
    service, _, _ = seeded_service
    df = service.make_dim_transaction()

    assert isinstance(df.loc[0, "transaction_id"], (int, np.integer))
    assert isinstance(df.loc[0, "transaction_type"], str)
    assert isinstance(df.loc[0, "sales_order_id"], (int, np.integer))

    # purchase_order_id can be None in your seeded data, so allow None
    assert (df.loc[0, "purchase_order_id"] is None) or isinstance(df.loc[0, "purchase_order_id"], (int, np.integer))


def test_dim_location_value_types(seeded_service):
    service, _, _ = seeded_service
    df = service.make_dim_location()

    assert isinstance(df.loc[0, "location_id"], (int, np.integer))
    assert isinstance(df.loc[0, "address_line_1"], str)
    assert isinstance(df.loc[0, "address_line_2"], str)
    assert isinstance(df.loc[0, "district"], str)
    assert isinstance(df.loc[0, "city"], str)
    assert isinstance(df.loc[0, "postal_code"], str)
    assert isinstance(df.loc[0, "country"], str)
    assert isinstance(df.loc[0, "phone"], str)


def test_dim_date_value_types(seeded_service):
    service, _, _ = seeded_service
    df = service.make_dim_date()

    assert isinstance(df.loc[0, "date_id"], (int, np.integer))
    assert isinstance(df.loc[0, "date"], date)
    assert isinstance(df.loc[0, "year"], (int, np.integer))
    assert isinstance(df.loc[0, "month"], (int, np.integer))
    assert isinstance(df.loc[0, "day"], (int, np.integer))
    assert isinstance(df.loc[0, "day_of_week"], (int, np.integer))
    assert isinstance(df.loc[0, "day_name"], str)
    assert isinstance(df.loc[0, "month_name"], str)
    assert isinstance(df.loc[0, "quarter"], (int, np.integer))


def test_fact_sales_order_value_types(seeded_service):
    service, _, _ = seeded_service
    df = service.make_fact_sales_order()

    assert isinstance(df.loc[0, "sales_order_id"], (int, np.integer))
    assert isinstance(df.loc[0, "created_date"], date)
    assert isinstance(df.loc[0, "created_time"], time)
    assert isinstance(df.loc[0, "last_updated_date"], date)
    assert isinstance(df.loc[0, "last_updated_time"], time)
    assert isinstance(df.loc[0, "sales_staff_id"], (int, np.integer))
    assert isinstance(df.loc[0, "sales_counterparty_id"], (int, np.integer))
    assert isinstance(df.loc[0, "units_sold"], (int, np.integer))
    assert isinstance(df.loc[0, "unit_price"], (float, np.floating))
    assert isinstance(df.loc[0, "currency_id"], (int, np.integer))
    assert isinstance(df.loc[0, "design_id"], (int, np.integer))
    assert isinstance(df.loc[0, "agreed_payment_date"], date)
    assert isinstance(df.loc[0, "agreed_delivery_date"], date)
    assert isinstance(df.loc[0, "agreed_delivery_location_id"], (int, np.integer))


def test_fact_purchase_order_value_types(seeded_service):
    service, _, _ = seeded_service
    df = service.make_fact_purchase_order()

    assert isinstance(df.loc[0, "purchase_record_id"], (int, np.integer))
    assert isinstance(df.loc[0, "purchase_order_id"], (int, np.integer))
    assert isinstance(df.loc[0, "created_date"], date)
    assert isinstance(df.loc[0, "created_time"], time)
    assert isinstance(df.loc[0, "last_updated_date"], date)
    assert isinstance(df.loc[0, "last_updated_time"], time)
    assert isinstance(df.loc[0, "staff_id"], (int, np.integer))
    assert isinstance(df.loc[0, "counterparty_id"], (int, np.integer))
    assert isinstance(df.loc[0, "item_code"], str)
    assert isinstance(df.loc[0, "item_quantity"], (int, np.integer))
    assert isinstance(df.loc[0, "item_unit_price"], (float, np.floating))
    assert isinstance(df.loc[0, "currency_id"], (int, np.integer))
    assert isinstance(df.loc[0, "agreed_delivery_date"], date)
    assert isinstance(df.loc[0, "agreed_payment_date"], date)
    assert isinstance(df.loc[0, "agreed_delivery_location_id"], (int, np.integer))


def test_fact_payment_value_types(seeded_service):
    service, _, _ = seeded_service
    df = service.make_fact_payment()

    assert isinstance(df.loc[0, "payment_id"], (int, np.integer))
    assert isinstance(df.loc[0, "transaction_id"], (int, np.integer))
    assert isinstance(df.loc[0, "counterparty_id"], (int, np.integer))
    assert isinstance(df.loc[0, "payment_amount"], (float, np.floating))
    assert isinstance(df.loc[0, "currency_id"], (int, np.integer))
    assert isinstance(df.loc[0, "payment_type_id"], (int, np.integer))
    assert isinstance(df.loc[0, "payment_date"], date)
    assert isinstance(df.loc[0, "paid"], (bool, np.bool_))



def test_debug_dim_counterparty(seeded_service):
    service, _, _ = seeded_service
    df = service.make_dim_counterparty()
    pprint(df)
