import pytest
from unittest.mock import MagicMock, patch
from db_conductor.adapters.snowflake_adapter import SnowflakeAdapter


@pytest.fixture
def mock_snowpark_session():
    with patch('snowflake.snowpark.Session') as mock_session:
        mock_session.return_value = MagicMock()
        yield mock_session

@pytest.fixture
def snowflake_adapter(mock_snowpark_session):
    adapter = SnowflakeAdapter(
        user="test_user",
        password="test_password",
        account="test_account",
        role="test_role",
        warehouse="test_warehouse",
        database="test_database",
        schema="test_schema"
    )
    adapter.connect()  # Establish the mock connection
    return adapter

def test_connect(snowflake_adapter):
    """Test that connect establishes a connection to Snowflake."""
    assert snowflake_adapter._connected is True
    snowflake_adapter.session.create_dataframe.assert_not_called()

def test_save_single_record(snowflake_adapter):
    """Test saving a single record to Snowflake."""
    record = {"id": 1, "name": "test_name"}
    snowflake_adapter.save(record)

    snowflake_adapter.session.create_dataframe.assert_called_once()
    snowflake_adapter.session.create_dataframe.return_value.write.save_as_table.assert_called_once_with(
        "test_schema.table_name", mode="append"
    )

def test_save_batch_records(snowflake_adapter):
    """Test saving a batch of records to Snowflake."""
    records = [
        {"id": 1, "name": "name_1"},
        {"id": 2, "name": "name_2"}
    ]
    snowflake_adapter.save_batch(records)

    snowflake_adapter.session.create_dataframe.assert_called_once()
    snowflake_adapter.session.create_dataframe.return_value.write.save_as_table.assert_called_once_with(
        "test_schema.table_name", mode="append"
    )

def test_get_record(snowflake_adapter):
    """Test retrieving a record from Snowflake."""
    condition = {"id": 1}
    snowflake_adapter.get(condition)

    snowflake_adapter.session.table.assert_called_once_with("test_schema.table_name")
    snowflake_adapter.session.table.return_value.filter.assert_called_once()
    snowflake_adapter.session.table.return_value.filter.return_value.collect.assert_called_once()

def test_disconnect(snowflake_adapter):
    """Test disconnecting from Snowflake."""
    snowflake_adapter.disconnect()

    snowflake_adapter.session.close.assert_called_once()
