"""Minimal stdio MCP transport to a phase-bound controller tool service.

The subprocess has no workspace capability itself: every tools/call traverses
StageToolBroker. It cannot execute shell or read arbitrary filesystem paths.
"""
from __future__ import annotations
import json
import os
import sys
import urllib.request

MAX_MESSAGE = 1024 * 1024


def serve(source, target, endpoint):
    from maestro.backends.codex_session.tools import MAESTRO_TOOL_SPEC
    while True:
        line = source.readline(MAX_MESSAGE+1)
        if not line:
            return
        if len(line) > MAX_MESSAGE:
            raise ValueError('MCP input too large')
        request = json.loads(line)
        if 'id' not in request:
            continue
        method = request.get('method')
        try:
            if method == 'initialize':
                result = {'protocolVersion': '2024-11-05', 'capabilities': {'tools': {}},
                          'serverInfo': {'name': 'maestro', 'version': '1'}}
            elif method == 'tools/list':
                result = {'tools': [MAESTRO_TOOL_SPEC]}
            elif method == 'ping':
                result = {}
            elif method == 'tools/call':
                params = request['params']
                if params.get('name') != 'maestro_tool':
                    raise ValueError('Unknown tool')
                body = json.dumps(params.get('arguments', {})).encode()
                call = urllib.request.Request(endpoint, data=body,
                                              headers={'Content-Type': 'application/json'})
                # No inherited HTTP proxy, no redirects, and only the controller
                # loopback URL from trusted MCP configuration may be contacted.
                class NoRedirect(urllib.request.HTTPRedirectHandler):
                    def redirect_request(self, *args, **kwargs):
                        return None
                opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), NoRedirect())
                remaining = float(os.environ['MAESTRO_TOOL_DEADLINE']) - __import__('time').monotonic()
                if remaining <= 0:
                    raise TimeoutError('Tool deadline elapsed')
                with opener.open(call, timeout=remaining) as response:
                    value = response.read(MAX_MESSAGE+1)
                if len(value) > MAX_MESSAGE:
                    raise ValueError('MCP response too large')
                result = {'content': [{'type': 'text', 'text': value.decode()}]}
            else:
                raise ValueError('Unsupported MCP method')
            reply = {'jsonrpc': '2.0', 'id': request['id'], 'result': result}
        except Exception:
            reply = {'jsonrpc': '2.0', 'id': request['id'],
                     'error': {'code': -32602, 'message': 'Controller tool operation rejected'}}
        target.write(json.dumps(reply)+'\n'); target.flush()


if __name__ == '__main__':
    serve(sys.stdin, sys.stdout, os.environ['MAESTRO_TOOL_ENDPOINT'])
