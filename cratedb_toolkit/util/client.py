import contextlib
from typing import Optional
from unittest.mock import patch

import crate.client.http

original_request = crate.client.http.Client._request


class jwt_token_patch(contextlib.ContextDecorator):
    """
    Patch the `Client._request` method to add the Authorization header for JWT token-based authentication.
    """

    def __init__(self, jwt_token: Optional[str] = None):
        self.jwt_token = jwt_token
        self._patch_stack = contextlib.ExitStack()

    def __enter__(self):
        if self.jwt_token:
            self._patch_stack.enter_context(
                patch.object(crate.client.http.Client, "_request", _mk_crate_client_request(self.jwt_token))
            )
        return self

    def __exit__(self, type, value, tb):  # noqa: A002
        self._patch_stack.close()
        return False


def _mk_crate_client_request(jwt_token: Optional[str] = None):
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
