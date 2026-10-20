"""Security contract regressions found during independent live-path review.

These tests inspect the actual generated Docker commands and exercise the
gateway reservation boundary. They do not assert that Docker ran on this host.
"""

from dataclasses import asdict
import json
from pathlib import Path
import subprocess
import time

import pytest

from maestro.execution import agent_container
from maestro.execution.budgets import BudgetExceeded, BudgetStore
from maestro.execution.gateway import BudgetGateway, HTTPUpstream
from maestro.execution.sandbox import DockerRunner, Mount, ResourceLimits, RunRequest


def test_formal_monitor_has_kill_capability_for_different_uid_child(tmp_path, monkeypatch):
    commands = []
    runner = DockerRunner("test@sha256:" + "1" * 64)
    monkeypatch.setattr(runner, "preflight", lambda image=None: {})
    def docker(args, **kwargs):
        commands.append(args)
        return subprocess.CompletedProcess(args, 0, "a" * 64 if args[0] == "create" else "", "")
    monkeypatch.setattr(runner, "_docker", docker)
    monkeypatch.setattr("maestro.execution.sandbox.subprocess.run", lambda args, **kwargs:
        subprocess.CompletedProcess(args, 0, json.dumps({"returncode": 0, "stdout": "", "stderr": "",
            "cpu_seconds": 0.1, "wall_seconds": 0.1, "timed_out": False, "diagnostics": {}}), ""))
    runner.run(RunRequest(("python3", "/candidate/solver.py"), tmp_path,
                         mounts=(Mount(tmp_path, "/candidate", True),)), ResourceLimits())
    create = commands[0]
    assert "--cap-drop=ALL" in create
    assert "--cap-add=KILL" in create
    assert "--user=0:0" in create
    assert "--network=none" in create
    assert "--read-only" in create


def test_agent_monitor_command_does_not_inherit_host_credentials(tmp_path, monkeypatch):
    commands = []
    scope = {"runtime_command": "claude", "docker": "docker", "container_name": "maestro-test",
        "image": "test@sha256:" + "1" * 64, "workdir": "/work/solver",
        "gateway_port": 37261, "limits": asdict(ResourceLimits()), "mounts": [],
        "environment": {"HOME": "/home/maestro", "ANTHROPIC_API_KEY": "phase-token"}}
    path = tmp_path / "scope.json"
    path.write_text(json.dumps(scope))
    monkeypatch.setenv("MAESTRO_GENERATION_API_KEY", "host-secret-must-stay-outside-container")
    def run(args, **kwargs):
        commands.append(args)
        return subprocess.CompletedProcess(args, 0, "", "")
    class Process:
        def wait(self, timeout=None):
            return 0
    monkeypatch.setattr(agent_container.subprocess, "run", run)
    monkeypatch.setattr(agent_container.subprocess, "Popen", lambda *args, **kwargs: Process())
    assert agent_container.main(["--scope", str(path), "--", "claude", "-p", "test"]) == 0
    create = next(command for command in commands if "create" in command)
    assert "--cap-add=KILL" in create
    assert "--network=none" in create
    assert not any("host-secret" in argument for argument in create)
    assert "ANTHROPIC_API_KEY=phase-token" in create


def test_gateway_freezes_model_and_rejects_ambiguous_output_caps(tmp_path):
    store = BudgetStore(tmp_path / "budgets.sqlite")
    try:
        store.create("phase", max_calls=3, max_request_bytes=10000,
                     max_output_tokens=100, deadline=time.time() + 60)
        sent = []
        def transport(payload):
            sent.append(payload)
            return {"usage": {"output_tokens": 1}}
        gateway = BudgetGateway(store, transport, allowed_model="pinned-model")
        token = gateway.credential("phase", "agent-1")
        with pytest.raises(BudgetExceeded, match="model"):
            gateway.complete(token, "different-model", {"model": "other-model", "max_tokens": 10})
        with pytest.raises(BudgetExceeded, match="ceiling"):
            gateway.complete(token, "ambiguous", {"model": "pinned-model", "max_tokens": 1,
                                                   "max_output_tokens": 10000})
        assert not sent and store.snapshot("phase")["calls"] == 0
        gateway.complete(token, "valid", {"model": "pinned-model", "max_tokens": 10})
        assert len(sent) == 1 and store.snapshot("phase")["calls"] == 1
    finally:
        store.close()


def test_anthropic_beta_path_and_headers_are_fixed_by_operator(monkeypatch):
    sent = []
    class Response:
        status = 200
        def read(self, limit):
            return b'{"usage":{"output_tokens":1}}'
        def getheader(self, name, default=None):
            return "application/json"
    class Connection:
        def __init__(self, host, port, timeout):
            assert host == "provider.invalid"
        def request(self, method, path, body, headers):
            sent.append((path, json.loads(body), headers))
        def getresponse(self):
            return Response()
        def close(self):
            pass
    monkeypatch.setattr("maestro.execution.gateway.http.client.HTTPSConnection", Connection)
    upstream = HTTPUpstream("https://provider.invalid", "provider-secret", auth_header="x-api-key",
                            anthropic_beta=("fixed-beta",))
    upstream({"_maestro_http_path": "/v1/messages?beta=true", "model": "pinned", "max_tokens": 1})
    assert sent[0][0] == "/v1/messages?beta=true"
    assert sent[0][2]["anthropic-beta"] == "fixed-beta"
    with pytest.raises(BudgetExceeded):
        upstream({"_maestro_http_path": "/v1/messages?redirect=https://other.invalid", "max_tokens": 1})
