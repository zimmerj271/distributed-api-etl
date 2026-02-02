from auth.token.token_provider import Token
from datetime import datetime, timedelta, timezone


def test_token_is_not_expired_without_expiry():
    token = Token("x", None)
    assert token.is_expired is False


def test_token_is_expired():
    token = Token(
        "x",
        expires_at=datetime.now(timezone.utc) - timedelta(seconds=1),
    )
    assert token.is_expired is True


def test_token_will_expire_within():
    token = Token(
        "x",
        expires_at=datetime.now(timezone.utc) + timedelta(seconds=30),
    )
    assert token.will_expire_within(60) is True


def test_token_serialization():
    token = Token(
        "abc",
        expires_at=datetime(2025, 1, 1, tzinfo=timezone.utc),
    )
    payload = token.serialize_token()
    assert payload["token_value"] == "abc"
    assert payload["expires_at"].startswith("2025")
