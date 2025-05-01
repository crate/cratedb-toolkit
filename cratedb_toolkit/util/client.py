import contextlib
from unittest import mock

import crate.client.http


@contextlib.contextmanager
def jwt_token_patch(jwt_token: str = None):
    with mock.patch.object(crate.client.http.Client, "_request", _mk_crate_client_server_request(jwt_token)):
        yield


def _mk_crate_client_server_request(jwt_token: str = None):
    """
    Create a monkey patched Server.request method to add the Authorization header for JWT token-based authentication.
    """

    _crate_client_server_request_dist = crate.client.http.Client._request

    def _crate_client_server_request(self, *args, **kwargs):
        """
        Monkey patch the Server.request method to add the Authorization header for JWT token-based authentication.

        TODO: Submit to upstream libraries and programs `crate.client`, `crate.crash`, and `sqlalchemy-cratedb`.
        """
        if jwt_token:
            kwargs.setdefault("headers", {})
            kwargs["headers"].update({"Authorization": "Bearer " + jwt_token})
        return _crate_client_server_request_dist(self, *args, **kwargs)

    return _crate_client_server_request
