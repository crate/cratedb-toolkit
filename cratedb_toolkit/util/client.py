import contextlib
from unittest.mock import patch

import crate.client.http

original_request = crate.client.http.Client._request


@contextlib.contextmanager
def jwt_token_patch(jwt_token: str = None):
    """
    Patch the `Client._request` method to add the Authorization header for JWT token-based authentication.
    """
    with patch.object(crate.client.http.Client, "_request", _mk_crate_client_request(jwt_token)):
        yield


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
