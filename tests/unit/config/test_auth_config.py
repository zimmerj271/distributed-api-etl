def test_basic_auth_runtime_args(basic_auth):
    args = basic_auth.to_runtime_args()
    assert args["username"] == "user"
    assert args["password"] == "pass"


def test_bearer_auth_runtime_args(bearer_auth):
    assert bearer_auth.to_runtime_args()["token"] == "abc123"


def test_oauth2_runtime_args(oauth2_password_auth):
    args = oauth2_password_auth.to_runtime_args()
    assert "token_url" in args
    assert args["refresh_margin"] == 120


def test_no_auth_runtime_args(no_auth):
    assert no_auth.to_runtime_args() == {}

