class AuthTokenExpiredError(Exception):
    """Raised on workers when the remote API indicates the token is invalid or expired"""

    pass


class MultipleCleanupFailures(Exception):
    """Raised when multiple cleanup operations fail."""

    def __init__(self, exceptions) -> None:
        self.exceptions = exceptions
        messages = "\n".join(f" - {type(e).__name__}: {e}" for e in exceptions)
        super().__init__(f"Multiple cleanup failures occurred:\n{messages}")
