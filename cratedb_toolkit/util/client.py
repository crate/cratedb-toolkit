import contextlib

import crate.client.http


@contextlib.contextmanager
def jwt_token_patch(jwt_token: str = None):
    original_request = crate.client.http.Client._request
    crate.client.http.Client._request = _mk_crate_client_request(original_request, jwt_token)
    try:
        yield
    finally:
        crate.client.http.Client._request = original_request


def _mk_crate_client_request(orig, jwt_token: str = None):
    """
    Create a monkey patched Client._request method to add the Authorization header for JWT token-based authentication.
    """

    def _crate_client_request(self, *args, **kwargs):
        """
        Monkey patch the Server.request method to add the Authorization header for JWT token-based authentication.

        TODO: Submit to upstream libraries and programs `crate.client`, `crate.crash`, and `sqlalchemy-cratedb`.
        """
        if jwt_token:
            kwargs.setdefault("headers", {})
            kwargs["headers"].update({"Authorization": "Bearer " + jwt_token})
        return orig(self, *args, **kwargs)

    return _crate_client_request
