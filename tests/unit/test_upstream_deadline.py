import threading
import time

import pytest

from maestro.execution.gateway import HTTPUpstream


def test_elapsed_deadline_cannot_dispatch_to_provider(monkeypatch):
    called = []
    def connection(*args, **kwargs):
        called.append(True)
        raise AssertionError("Must reject before any connection")
    monkeypatch.setattr("maestro.execution.gateway.http.client.HTTPSConnection", connection)
    upstream = HTTPUpstream("https://provider.invalid", "key", absolute_deadline=time.monotonic()-1)
    with pytest.raises(TimeoutError):
        upstream({"_maestro_http_path": "/v1/messages", "max_tokens": 1})
    assert not called


def test_absolute_deadline_interrupts_a_slow_response(monkeypatch):
    stopped = threading.Event()
    class Socket:
        def shutdown(self, flags):
            stopped.set()
    class Response:
        status = 200
        def read(self, maximum):
            stopped.wait(3)
            return b'{"usage":{"output_tokens":1}}'
        def getheader(self, *args):
            return "application/json"
    class Connection:
        def __init__(self, *args, **kwargs):
            self.watched_socket = Socket()
        def request(self, *args, **kwargs):
            pass
        def getresponse(self):
            return Response()
        def close(self):
            pass
    monkeypatch.setattr("maestro.execution.gateway.http.client.HTTPSConnection", Connection)
    upstream = HTTPUpstream("https://provider.invalid", "key", absolute_deadline=time.monotonic()+0.05)
    start = time.monotonic()
    with pytest.raises(TimeoutError):
        upstream({"_maestro_http_path": "/v1/messages", "max_tokens": 1})
    assert time.monotonic()-start < 1
