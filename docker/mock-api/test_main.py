"""
Unit tests for Mock API Server
"""

import base64
import pytest
from fastapi.testclient import TestClient

from main import app, BASIC_AUTH_USERNAME, BASIC_AUTH_PASSWORD


@pytest.fixture
def client():
    return TestClient(app)


@pytest.fixture
def valid_basic_auth_header():
    credentials = f"{BASIC_AUTH_USERNAME}:{BASIC_AUTH_PASSWORD}"
    encoded = base64.b64encode(credentials.encode()).decode()
    return {"Authorization": f"Basic {encoded}"}


@pytest.fixture
def invalid_basic_auth_header():
    credentials = "wrong-user:wrong-password"
    encoded = base64.b64encode(credentials.encode()).decode()
    return {"Authorization": f"Basic {encoded}"}


@pytest.fixture
def valid_bearer_header():
    return {"Authorization": "Bearer eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.test-token"}


class TestHealthEndpoint:
    def test_health_returns_healthy(self, client):
        response = client.get("/health")

        assert response.status_code == 200
        assert response.json() == {"status": "healthy"}


class TestNoAuthEndpoint:
    def test_returns_data_without_auth(self, client):
        response = client.get("/api/noauth/data")

        assert response.status_code == 200
        data = response.json()
        assert data["auth_method"] == "none"
        assert "request_id" in data
        assert "timestamp" in data
        assert "data" in data

    def test_returns_random_data_fields(self, client):
        response = client.get("/api/noauth/data")

        data = response.json()["data"]
        assert "transaction_id" in data
        assert "amount" in data
        assert "currency" in data
        assert "status" in data
        assert "customer_id" in data
        assert "product_code" in data
        assert "quantity" in data
        assert "region" in data
        assert "priority" in data
        assert "metadata" in data

    def test_returns_different_data_on_each_request(self, client):
        response1 = client.get("/api/noauth/data")
        response2 = client.get("/api/noauth/data")

        data1 = response1.json()
        data2 = response2.json()

        assert data1["request_id"] != data2["request_id"]
        assert data1["data"]["transaction_id"] != data2["data"]["transaction_id"]


class TestBasicAuthEndpoint:
    def test_returns_data_with_valid_credentials(self, client, valid_basic_auth_header):
        response = client.get("/api/basic/data", headers=valid_basic_auth_header)

        assert response.status_code == 200
        data = response.json()
        assert data["auth_method"] == f"basic:{BASIC_AUTH_USERNAME}"
        assert "request_id" in data
        assert "data" in data

    def test_rejects_invalid_credentials(self, client, invalid_basic_auth_header):
        response = client.get("/api/basic/data", headers=invalid_basic_auth_header)

        assert response.status_code == 401
        assert response.json()["detail"] == "Invalid credentials"

    def test_rejects_missing_credentials(self, client):
        response = client.get("/api/basic/data")

        assert response.status_code == 401

    def test_rejects_wrong_username(self, client):
        credentials = f"wrong-user:{BASIC_AUTH_PASSWORD}"
        encoded = base64.b64encode(credentials.encode()).decode()
        headers = {"Authorization": f"Basic {encoded}"}

        response = client.get("/api/basic/data", headers=headers)

        assert response.status_code == 401

    def test_rejects_wrong_password(self, client):
        credentials = f"{BASIC_AUTH_USERNAME}:wrong-password"
        encoded = base64.b64encode(credentials.encode()).decode()
        headers = {"Authorization": f"Basic {encoded}"}

        response = client.get("/api/basic/data", headers=headers)

        assert response.status_code == 401


class TestOAuth2Endpoint:
    def test_returns_data_with_valid_bearer_token(self, client, valid_bearer_header):
        response = client.get("/api/oauth2/data", headers=valid_bearer_header)

        assert response.status_code == 200
        data = response.json()
        assert data["auth_method"] == "oauth2:bearer"
        assert "request_id" in data
        assert "data" in data

    def test_rejects_missing_token(self, client):
        response = client.get("/api/oauth2/data")

        assert response.status_code == 401

    def test_rejects_empty_token(self, client):
        headers = {"Authorization": "Bearer "}

        response = client.get("/api/oauth2/data", headers=headers)

        assert response.status_code == 401

    def test_accepts_any_non_empty_token(self, client):
        headers = {"Authorization": "Bearer any-token-value"}

        response = client.get("/api/oauth2/data", headers=headers)

        assert response.status_code == 200


class TestBearerEndpoint:
    def test_returns_data_with_valid_bearer_token(self, client, valid_bearer_header):
        response = client.get("/api/bearer/data", headers=valid_bearer_header)

        assert response.status_code == 200
        data = response.json()
        assert data["auth_method"] == "bearer:static"
        assert "request_id" in data
        assert "data" in data

    def test_rejects_missing_token(self, client):
        response = client.get("/api/bearer/data")

        assert response.status_code == 401

    def test_accepts_any_non_empty_token(self, client):
        headers = {"Authorization": "Bearer my-static-api-key"}

        response = client.get("/api/bearer/data", headers=headers)

        assert response.status_code == 200


class TestResponseDataStructure:
    def test_metadata_contains_expected_fields(self, client):
        response = client.get("/api/noauth/data")

        metadata = response.json()["data"]["metadata"]
        assert metadata["source"] == "mock-api"
        assert metadata["version"] == "1.0.0"
        assert "random_value" in metadata

    def test_currency_is_valid_value(self, client):
        valid_currencies = {"USD", "EUR", "GBP", "JPY"}

        for _ in range(10):
            response = client.get("/api/noauth/data")
            currency = response.json()["data"]["currency"]
            assert currency in valid_currencies

    def test_status_is_valid_value(self, client):
        valid_statuses = {"pending", "completed", "failed", "processing"}

        for _ in range(10):
            response = client.get("/api/noauth/data")
            status = response.json()["data"]["status"]
            assert status in valid_statuses

    def test_region_is_valid_value(self, client):
        valid_regions = {"NA", "EU", "APAC", "LATAM"}

        for _ in range(10):
            response = client.get("/api/noauth/data")
            region = response.json()["data"]["region"]
            assert region in valid_regions

    def test_priority_is_valid_value(self, client):
        valid_priorities = {"low", "medium", "high", "critical"}

        for _ in range(10):
            response = client.get("/api/noauth/data")
            priority = response.json()["data"]["priority"]
            assert priority in valid_priorities

    def test_quantity_is_positive_integer(self, client):
        for _ in range(10):
            response = client.get("/api/noauth/data")
            quantity = response.json()["data"]["quantity"]
            assert isinstance(quantity, int)
            assert quantity >= 1

    def test_amount_is_non_negative_float(self, client):
        for _ in range(10):
            response = client.get("/api/noauth/data")
            amount = response.json()["data"]["amount"]
            assert isinstance(amount, float)
            assert amount >= 0
