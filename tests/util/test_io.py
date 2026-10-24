from http.server import BaseHTTPRequestHandler, HTTPServer
from threading import Thread

import pytest

aiohttp = pytest.importorskip("aiohttp")
conf = pytest.importorskip("fsspec.config").conf
pytest.importorskip("orjsonl")
pytest.importorskip("upath")

from cratedb_toolkit.util.io import read_json  # noqa: E402


@pytest.fixture
def json_server():
    requests = []
    body = b'{"value": 42}\n'

    class Handler(BaseHTTPRequestHandler):
        def do_HEAD(self):
            self.respond()

        def do_GET(self):
            self.respond()

        def respond(self):
            requests.append((self.command, self.path, self.headers.get("X-Test")))
            redirect = self.path == "/redirect"
            self.send_response(302 if redirect else 200)
            if redirect:
                self.send_header("Location", "/data")
            else:
                self.send_header("Content-Length", str(len(body)))
            header_size = 0
            if (
                self.path == "/redirect"
                or (self.path == "/head" and self.command == "HEAD")
                or (self.path == "/get" and self.command == "GET")
            ):
                header_size = 10 * 1024
            elif self.path == "/over-info" or (self.path == "/over-get" and self.command == "GET"):
                header_size = 70 * 1024
            if header_size:
                self.send_header("Content-Security-Policy", "x" * header_size)
            self.end_headers()
            if self.command == "GET" and not redirect:
                self.wfile.write(body)

    server = HTTPServer(("127.0.0.1", 0), Handler)
    thread = Thread(target=server.serve_forever)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}", requests
    finally:
        server.shutdown()
        thread.join()
        server.server_close()


@pytest.mark.parametrize("path", ["head", "get", "redirect"])
def test_read_json_large_http_header(json_server, path):
    url, requests = json_server
    assert list(read_json(f"{url}/{path}")) == [{"value": 42}]
    expected = [("HEAD", f"/{path}"), ("GET", f"/{path}")]
    if path == "redirect":
        expected = [("HEAD", "/redirect"), ("HEAD", "/data"), ("GET", "/redirect"), ("GET", "/data")]
    assert [(method, target) for method, target, _ in requests] == expected


@pytest.mark.parametrize("path", ["over-info", "over-get"])
def test_read_json_rejects_oversized_http_header(json_server, path):
    url, _ = json_server
    with pytest.raises((FileNotFoundError, aiohttp.ClientResponseError)) as error:
        list(read_json(f"{url}/{path}"))
    cause = error.value.__cause__ if isinstance(error.value, FileNotFoundError) else error.value
    assert isinstance(cause, aiohttp.ClientResponseError)
    assert "65536" in cause.message


def test_read_json_preserves_http_session_config(json_server, monkeypatch):
    url, requests = json_server
    config = {"client_kwargs": {"headers": {"X-Test": "configured"}}}
    monkeypatch.setitem(conf, "http", config)
    assert list(read_json(f"{url}/redirect")) == [{"value": 42}]
    assert requests
    assert all(header == "configured" for _, _, header in requests)
    assert config == {"client_kwargs": {"headers": {"X-Test": "configured"}}}


@pytest.mark.parametrize("protocol", ["http", "https", "memory"])
def test_read_json_preserves_storage_options(mocker, protocol):
    options = {"client_kwargs": {"headers": {"X-Test": "explicit"}}}
    path = mocker.patch("cratedb_toolkit.util.io.UPath").return_value
    path.protocol = protocol
    path.storage_options = options
    fs = mocker.patch("cratedb_toolkit.util.io.filesystem")
    mocker.patch("cratedb_toolkit.util.io.orjsonl.stream")
    read_json("unused")
    expected: dict[str, object] = {"client_kwargs": {"headers": {"X-Test": "explicit"}}}
    if protocol in ("http", "https"):
        expected["max_field_size"] = 64 * 1024
    fs.assert_called_once_with(protocol, **expected)
    assert options == {"client_kwargs": {"headers": {"X-Test": "explicit"}}}


def test_read_json_preserves_explicit_http_limit(mocker):
    path = mocker.patch("cratedb_toolkit.util.io.UPath").return_value
    path.protocol = "https"
    path.storage_options = {"max_field_size": 128 * 1024}
    fs = mocker.patch("cratedb_toolkit.util.io.filesystem")
    mocker.patch("cratedb_toolkit.util.io.orjsonl.stream")
    read_json("unused")
    fs.assert_called_once_with("https", max_field_size=128 * 1024)
    assert path.storage_options == {"max_field_size": 128 * 1024}


@pytest.mark.parametrize("as_uri", [False, True])
def test_read_json_local_file(tmp_path, as_uri):
    path = tmp_path / "data.ndjson"
    path.write_text('{"value": 42}\n')
    assert list(read_json(path.as_uri() if as_uri else str(path))) == [{"value": 42}]
