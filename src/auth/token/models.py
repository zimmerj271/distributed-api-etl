from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True)
class Token:
    token_value: str
    expires_at: datetime | None = None

    @property
    def is_expired(self) -> bool:
        """
        Returns boolean to flag if the token has expired, with a small buffer.
        """
        if self.expires_at is None:
            return False
        return datetime.now() >= self.expires_at

    def seconds_until_expiration(self) -> float:
        if self.expires_at is not None:
            return (self.expires_at - datetime.now()).total_seconds()
        return 0

    def will_expire_within(self, seconds: int) -> bool:
        if self.expires_at is None:
            return False
        return self.seconds_until_expiration() <= seconds

    def serialize_token(self) -> dict[str, str]:
        return {
            "token_value": self.token_value,
            "expires_at": self.expires_at.isoformat() if self.expires_at else "",
        } 

