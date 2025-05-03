import contextlib
from unittest.mock import patch

import crate.client.http

original_request = crate.client.http.Client._request


class jwt_token_patch(contextlib.ContextDecorator):
    """
    Patch the `Client._request` method to add the Authorization header for JWT token-based authentication.
    """

    def __init__(self, jwt_token: str = None):
        self.jwt_token = jwt_token

    def __enter__(self):
        self.patcher = patch.object(crate.client.http.Client, "_request", _mk_crate_client_request(self.jwt_token))
        self.patcher.start()
        return self

    def __exit__(self, type, value, tb):  # noqa: A002
        self.patcher.stop()
        return self


def _mk_crate_client_request(jwt_token: str = None):
    """
    Create a monkey patched `Client._request` method to add the Authorization header for JWT token-based authentication.
    """

    def _crate_client_request(self, *args, **kwargs):
        """
        Amendment for the `Client._request` method to add the Authorization header for JWT token-based authentication.

        TODO: Submit to upstream libraries and programs `crate.client`, `crate.crash`, and `sqlalchemy-cratedb`.
        """
        if jwt_token:
            kwargs.setdefault("headers", {})
            kwargs["headers"].update({"Authorization": "Bearer " + jwt_token})
        return original_request(self, *args, **kwargs)

    return _crate_client_request
