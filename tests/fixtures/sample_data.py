"""Sample data fixtures for testing"""
import pytest
from pyspark.sql import Row


@pytest.fixture
def sample_api_rows():
    """Sample rows for API request testing"""
    return [
        Row(request_id="req_001", patient_id="P123", tracking_id="track_001"),
        Row(request_id="req_002", patient_id="P456", tracking_id="track_002"),
        Row(request_id="req_003", patient_id="P789", tracking_id="track_003"),
    ]


@pytest.fixture
def sample_api_response():
    """Sample API response for testing"""
    return {
        "status": "success",
        "data": {
            "patient_id": "P123",
            "name": "John Doe",
            "procedures": [
                {"id": "PROC-001", "type": "surgery"},
                {"id": "PROC-002", "type": "consultation"},
            ]
        }
    }


@pytest.fixture
def sample_config_dict():
    """Minimal valid config as dictionary"""
    return {
        "endpoint": {
            "name": "test_endpoint",
            "base_url": "https://api.example.com",
            "url_path": "/v1/patients",
            "method": "GET",
            "vendor": "test_vendor",
        },
        "transport": {
            "type": "aiohttp",
            "base_timeout": 30,
            "warmup_timeout": 10,
            "tcp_connection": {"limit": 50}
        },
        "auth": {"type": "none"},
        "middleware": [],
        "tables": {
            "sink": {
                "name": "bronze_api_data",
                "namespace": "catalog.schema",
            }
        }
    }
