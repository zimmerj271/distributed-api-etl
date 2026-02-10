"""
Mock API Server for Authentication Testing

Provides endpoints that demonstrate different authentication patterns
and return random data for ETL pipeline testing.
"""

import base64
import secrets
import uuid
from datetime import datetime, timezone
from typing import Annotated

from fastapi import Depends, FastAPI, Header, HTTPException, status
from fastapi.security import HTTPBasic, HTTPBasicCredentials, HTTPBearer, HTTPAuthorizationCredentials
from pydantic import BaseModel

app = FastAPI(title="Mock API", description="Authentication testing endpoints")

# Security schemes
basic_security = HTTPBasic()
bearer_security = HTTPBearer()

# Configured credentials for basic auth
BASIC_AUTH_USERNAME = "test-user"
BASIC_AUTH_PASSWORD = "test-password"


class DataResponse(BaseModel):
    """Response model for data endpoints"""
    request_id: str
    timestamp: str
    auth_method: str
    data: dict


def generate_random_data() -> dict:
    """Generate random data for response payload"""
    return {
        "transaction_id": str(uuid.uuid4()),
        "amount": round(secrets.randbelow(100000) / 100, 2),
        "currency": secrets.choice(["USD", "EUR", "GBP", "JPY"]),
        "status": secrets.choice(["pending", "completed", "failed", "processing"]),
        "customer_id": f"CUST-{secrets.randbelow(99999):05d}",
        "product_code": f"PRD-{secrets.token_hex(4).upper()}",
        "quantity": secrets.randbelow(100) + 1,
        "region": secrets.choice(["NA", "EU", "APAC", "LATAM"]),
        "priority": secrets.choice(["low", "medium", "high", "critical"]),
        "metadata": {
            "source": "mock-api",
            "version": "1.0.0",
            "random_value": secrets.token_hex(8),
        }
    }


def create_response(auth_method: str) -> DataResponse:
    """Create a standardized response with random data"""
    return DataResponse(
        request_id=str(uuid.uuid4()),
        timestamp=datetime.now(timezone.utc).isoformat(),
        auth_method=auth_method,
        data=generate_random_data(),
    )


def verify_basic_auth(
    credentials: Annotated[HTTPBasicCredentials, Depends(basic_security)]
) -> str:
    """Verify basic auth credentials"""
    is_valid_username = secrets.compare_digest(
        credentials.username.encode("utf-8"),
        BASIC_AUTH_USERNAME.encode("utf-8")
    )
    is_valid_password = secrets.compare_digest(
        credentials.password.encode("utf-8"),
        BASIC_AUTH_PASSWORD.encode("utf-8")
    )

    if not (is_valid_username and is_valid_password):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid credentials",
            headers={"WWW-Authenticate": "Basic"},
        )

    return credentials.username


def verify_bearer_token(
    credentials: Annotated[HTTPAuthorizationCredentials, Depends(bearer_security)]
) -> str:
    """
    Verify bearer token is present.

    Note: For demo purposes, we only verify the token is present and non-empty.
    In production, you would validate the JWT signature against Keycloak's public key.
    """
    if not credentials.credentials:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid token",
            headers={"WWW-Authenticate": "Bearer"},
        )

    return credentials.credentials[:20] + "..."  # Return truncated token for logging


@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {"status": "healthy"}


@app.get("/api/noauth/data", response_model=DataResponse)
async def get_data_noauth():
    """
    Get random data without authentication.

    Use this endpoint to test NoAuthStrategy.
    """
    return create_response(auth_method="none")


@app.get("/api/basic/data", response_model=DataResponse)
async def get_data_basic(
    username: Annotated[str, Depends(verify_basic_auth)]
):
    """
    Get random data with HTTP Basic authentication.

    Use this endpoint to test BasicAuthStrategy.

    Credentials:
    - Username: test-user
    - Password: test-password
    """
    return create_response(auth_method=f"basic:{username}")


@app.get("/api/oauth2/data", response_model=DataResponse)
async def get_data_oauth2(
    token_info: Annotated[str, Depends(verify_bearer_token)]
):
    """
    Get random data with OAuth2 Bearer token authentication.

    Use this endpoint to test OAuth2 strategies (password grant or client credentials).

    Requires a valid Bearer token from Keycloak in the Authorization header.
    """
    return create_response(auth_method=f"oauth2:bearer")


@app.get("/api/bearer/data", response_model=DataResponse)
async def get_data_bearer(
    token_info: Annotated[str, Depends(verify_bearer_token)]
):
    """
    Get random data with static Bearer token authentication.

    Use this endpoint to test BearerTokenStrategy.

    Accepts any non-empty Bearer token.
    """
    return create_response(auth_method="bearer:static")
