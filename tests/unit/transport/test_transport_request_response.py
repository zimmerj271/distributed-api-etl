"""Unit tests for TransportRequest and TransportResponse data classes"""
import pytest
from request_execution.transport.base import TransportRequest, TransportResponse


@pytest.mark.unit
@pytest.mark.transport
class TestTransportRequest:
    """Tests for TransportRequest dataclass"""
    
    def test_creates_with_required_fields(self):
        """
        GIVEN required fields for TransportRequest
        WHEN creating instance
        THEN it should initialize successfully
        """
        req = TransportRequest(
            method="GET",
            url="/api/endpoint",
            headers={"Accept": "application/json"},
        )
        
        assert req.method == "GET"
        assert req.url == "/api/endpoint"
        assert req.headers == {"Accept": "application/json"}
    
    def test_optional_fields_default_to_none(self):
        """
        GIVEN only required fields
        WHEN creating TransportRequest
        THEN optional fields should be None
        """
        req = TransportRequest(
            method="GET",
            url="/test",
            headers={},
        )
        
        assert req.params is None
        assert req.json is None
        assert req.data is None
    
    def test_creates_with_all_fields(self):
        """
        GIVEN all possible fields
        WHEN creating TransportRequest
        THEN all should be set correctly
        """
        req = TransportRequest(
            method="POST",
            url="/api/resource",
            headers={"Content-Type": "application/json"},
            params={"filter": "active"},
            json={"name": "test"},
            data=b"binary data",
        )
        
        assert req.method == "POST"
        assert req.url == "/api/resource"
        assert req.params["filter"] == "active"
        assert req.json["name"] == "test"
        assert req.data == b"binary data"


@pytest.mark.unit
@pytest.mark.transport
class TestTransportResponse:
    """Tests for TransportResponse dataclass"""
    
    def test_creates_with_required_fields(self):
        """
        GIVEN required fields for TransportResponse
        WHEN creating instance
        THEN it should initialize successfully
        """
        resp = TransportResponse(
            status=200,
            headers={"Content-Type": "application/json"},
            body=b'{"ok": true}',
        )
        
        assert resp.status == 200
        assert resp.headers["Content-Type"] == "application/json"
        assert resp.body == b'{"ok": true}'
    
    def test_error_defaults_to_none(self):
        """
        GIVEN successful response
        WHEN creating TransportResponse
        THEN error should be None
        """
        resp = TransportResponse(
            status=200,
            headers={},
            body=None,
        )
        
        assert resp.error is None
    
    def test_creates_error_response(self):
        """
        GIVEN an error scenario
        WHEN creating TransportResponse with error
        THEN status and body can be None
        """
        resp = TransportResponse(
            status=None,
            headers={},
            body=None,
            error="Connection timeout",
        )
        
        assert resp.status is None
        assert resp.body is None
        assert resp.error == "Connection timeout"
    
    def test_headers_can_be_none(self):
        """
        GIVEN a response without headers
        WHEN creating TransportResponse
        THEN headers can be None
        """
        resp = TransportResponse(
            status=204,
            headers=None,
            body=None,
        )
        
        assert resp.headers is None
