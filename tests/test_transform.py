import pytest
import os
from unittest.mock import patch, MagicMock
from scripts.transform import run_transformations

# --- TEST 1: Happy Path ---
@patch('scripts.transform.psycopg2.connect')
def test_run_transformations_success(mock_connect):
    """Test that the transformation script executes the SQL payload."""
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    
    mock_connect.return_value = mock_conn
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

    run_transformations()

    mock_connect.assert_called_once()
    mock_cursor.execute.assert_called_once()
    mock_conn.commit.assert_called_once()
    mock_conn.close.assert_called_once()

    executed_sql = mock_cursor.execute.call_args[0][0]
    assert "INSERT INTO warehouse.dim_country" in executed_sql
    assert "INSERT INTO warehouse.audit_missing_countries" in executed_sql
    assert "INSERT INTO warehouse.fact_economic_indicators" in executed_sql
    assert "CREATE OR REPLACE VIEW warehouse.rpt_economic_indicators" in executed_sql

# --- TEST 2: Environment Variable Configuration ---
@patch('scripts.transform.psycopg2.connect')
@patch.dict(os.environ, {
    "DB_HOST": "test_host", 
    "DB_USER": "test_user", 
    "DB_PASSWORD": "test_password", 
    "DB_NAME": "test_db"
})
def test_run_transformations_db_connection_params(mock_connect):
    """Test that the script uses the correct environment variables."""
    mock_conn = MagicMock()
    mock_connect.return_value = mock_conn
    
    run_transformations()
    
    mock_connect.assert_called_once_with(
        host="test_host",
        user="test_user",
        password="test_password",
        dbname="test_db"
    )

# --- TEST 3: Exception Handling (Database Closure) ---
@patch('scripts.transform.psycopg2.connect')
def test_run_transformations_handles_exception(mock_connect):
    """Test that DB errors are handled and the connection closes safely."""
    mock_conn = MagicMock()
    mock_connect.return_value = mock_conn
    
    mock_cursor = MagicMock()
    mock_cursor.execute.side_effect = Exception("Simulated DB failure")
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

    run_transformations()

    mock_conn.commit.assert_not_called() 
    mock_conn.close.assert_called_once()

# --- TEST 4: Error Logging ---
@patch('scripts.transform.psycopg2.connect')
@patch('scripts.transform.logging.error')
def test_run_transformations_logs_error(mock_logging_error, mock_connect):
    """Test that exceptions during SQL execution are cleanly logged."""
    mock_conn = MagicMock()
    mock_connect.return_value = mock_conn
    
    mock_cursor = MagicMock()
    mock_cursor.execute.side_effect = Exception("Syntax error in SQL")
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

    run_transformations()

    mock_logging_error.assert_called_once()
    args, _ = mock_logging_error.call_args
    assert "Transformation failed: Syntax error in SQL" in args[0]