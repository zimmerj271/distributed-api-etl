class AuthTokenExpiredError(Exception):
    """Raised on workers when the remote API indicates the token is invalid or expired"""
    pass
