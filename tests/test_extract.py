import pytest
from unittest.mock import patch, MagicMock
from scripts.extract import WorldBankAPIClient

@patch('scripts.extract.requests.get')
def test_fetch_paginated_data_success(mock_get):
    """Test that the client correctly parses a valid World Bank API response."""
    
    # Simulate the JSON response from the World Bank API
    mock_response = MagicMock()
    mock_response.json.return_value = [
        {"page": 1, "pages": 1, "per_page": 50, "total": 2}, # Metadata
        [{"id": "ZAF", "name": "South Africa"}, {"id": "IND", "name": "India"}] # Data
    ]
    mock_response.raise_for_status = MagicMock()
    mock_get.return_value = mock_response

    client = WorldBankAPIClient(per_page=50)
    
    # Consume the generator
    results = list(client.fetch_paginated_data("country"))
    
    assert len(results) == 1  # One page of results
    assert len(results[0]) == 2 # Two countries in that page
    assert results[0][0]["id"] == "ZAF"
    mock_get.assert_called_once()

@patch('scripts.extract.requests.get')
def test_fetch_paginated_data_empty(mock_get):
    """Test how the client handles an empty response."""
    
    mock_response = MagicMock()
    # The API returns a single dictionary or missing data array when empty
    mock_response.json.return_value = [{"page": 1, "pages": 0}] 
    mock_get.return_value = mock_response

    client = WorldBankAPIClient()
    results = list(client.fetch_paginated_data("country"))
    
    assert len(results) == 0 # Generator should yield nothing and break cleanly