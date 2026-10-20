"""Offline contracts for the single-use live diagnostic; no provider traffic."""
import http.client
import json
from pathlib import Path
import time
from types import SimpleNamespace
from urllib.parse import urlsplit

import pytest

from maestro.backends.base import BackendUnavailable
from maestro.backends.codex_session.qualification import run_qualification
from maestro.backends.codex_session.quota_proxy import CodexQuotaProxy
from maestro.types import stable_hash


@pytest.fixture(name="tmp_path")
def short_path(tmp_path_factory):
    # Git's Windows object paths otherwise exceed MAX_PATH under long test names.
    return tmp_path_factory.mktemp("q")


class Response:
    status = 200
    headers = {"Content-Type": "text/event-stream"}
    def iter_bytes(self, deadline):
        value = {"type": "response.completed", "response": {"id": "resp_offline",
            "status": "completed", "output": [], "usage": {"input_tokens": 10, "output_tokens": 5}}}
        yield b"data: "+json.dumps(value).encode()+b"\n\n"
    def close(self):
        pass


def post(base_url, model):
    url = urlsplit(base_url)
    connection = http.client.HTTPConnection(url.hostname, url.port, timeout=5)
    try:
        connection.request("POST", url.path+"/responses", json.dumps({"model": model, "input": []}),
            {"Authorization": "Bearer offline-credential", "Content-Type": "application/json"})
        response = connection.getresponse()
        status = response.status
        response.read()
        if status != 200:
            raise BackendUnavailable("Offline native turn stopped")
    finally:
        connection.close()


@pytest.fixture
def fixture():
    clients, forwarded = [], []
    settings = {"lose_receipt": False, "omit_memory_write": False, "shell": False}
    class Client:
        def __init__(self, *, model, work_root, executable=None, timeout=120, reasoning_effort=None):
            self.model, self.root = model, Path(work_root)
            self.reasoning_effort = reasoning_effort
            self.resume_ids = []
            self.phase_deadlines = []
            clients.append(self)
        def preflight(self):
            return {"authenticated": True, "auth_method": "chatgpt", "native_inference_started": False}
        def run_phase(self, prompt, *, base_url, deadline, tools, output_schema=None, session_id=None):
            self.phase_deadlines.append(deadline-time.monotonic())
            if output_schema is not None:
                post(base_url, self.model)
                answer = json.dumps({"action": "revise", "plan": {"target": "other", "mod_type": "other",
                    "diagnosis": "File-only transport check", "change": "Follow the declared file-only contract",
                    "expected_effect": "Verify memory and code tool loop", "evidence_run_ids": []}, "lessons": []})
            elif prompt.startswith("CONTINUITY_READ"):
                self.resume_ids.append(session_id)
                post(base_url, self.model)
                tools("maestro_tool", {"action": "read", "path": "private/notes.md"})
                post(base_url, self.model)  # Locally denied; never an upstream inference.
                raise AssertionError("Continuity cap failed")
            else:
                post(base_url, self.model)
                if settings["shell"]:
                    tools("maestro_tool", {"action": "shell", "command": "must-never-run"})
                read = tools("maestro_tool", {"action": "batch", "operations": [
                    {"action": "read", "path": "private/notes.md"},
                    {"action": "read", "path": "public/board.json"}]})
                memory = read["results"][0]["result"]["content"]
                board = read["results"][1]["result"]["content"]
                private = memory.strip().split("=", 1)[1]
                public = json.loads(board)["version"]
                post(base_url, self.model)
                writes = [{"action": "write", "path": "solver/probe.py",
                    "content": f'PRIVATE_MEMORY = "{private}"\nPUBLIC_MEMORY = "{public}"\n'}]
                if not settings["omit_memory_write"]:
                    writes.append({"action": "write", "path": "private/notes.md",
                        "content": f"QUALIFIED:{private}:{public}\n"})
                tools("maestro_tool", {"action": "batch", "operations": writes})
                post(base_url, self.model)
                answer = "Done"
            return SimpleNamespace(text=answer, thread_id="own-native-thread", response_hash=stable_hash(answer))
        def close(self):
            pass
    def upstream(path, headers, body, timeout):
        forwarded.append(path)
        if settings["lose_receipt"]:
            raise OSError("Synthetic lost receipt; never provider traffic")
        return Response()
    def proxy(*args, **kwargs):
        return CodexQuotaProxy(*args, upstream=upstream, **kwargs)
    return Client, proxy, clients, forwarded, settings


def test_real_backend_file_loop_and_continuity_fit_single_five_request_envelope(tmp_path, fixture):
    client, proxy, clients, forwarded, _ = fixture
    result = run_qualification(tmp_path/"qualification", model="test-model",
        client_factory=client, proxy_factory=proxy)
    assert result["status"] == "PASSED"
    assert result["actual_requests"] == len(forwarded) == 5
    assert result["unresolved_requests"] == 0
    assert all(result["checks"].values())
    assert len(result["provider_receipts"]) == 5
    assert [result["phases"][phase]["actual_requests"] for phase in
            ("PLAN_RO", "GENERATION", "CONTINUITY_READ")] == [1, 3, 1]
    assert result["phases"]["GENERATION"]["quota_denied"]
    assert result["phases"]["CONTINUITY_READ"]["quota_denied"]
    assert clients[1].resume_ids == ["own-native-thread"]
    assert not result["candidate_executed"] and not result["warm_start_started"]
    assert not result["formal_evaluation_started"]
    trace = result["tool_trace"]
    assert len(trace) == 7 and all(row["origin"] == "model" for row in trace)
    assert sum(row["action"] == "batch" for row in trace) == 2
    assert sum(row["action"] in {"read", "write"} for row in trace) == 5
    assert all("content" not in row for row in trace)
    persisted = json.loads((tmp_path/"qualification"/"result.json").read_text())
    assert persisted == result


def test_consumed_or_failed_output_root_cannot_be_automatically_reused(tmp_path, fixture):
    client, proxy, _, forwarded, _ = fixture
    root = tmp_path/"qualification"
    run_qualification(root, model="test-model", client_factory=client, proxy_factory=proxy)
    before = len(forwarded)
    with pytest.raises(FileExistsError):
        run_qualification(root, model="test-model", client_factory=client, proxy_factory=proxy)
    assert len(forwarded) == before
    manifest = json.loads((root/"manifest.json").read_text())
    assert manifest["max_total_requests"] == 5 and manifest["automatic_retry"] is False


def test_unresolved_request_stops_entire_diagnostic_without_continuity(tmp_path, fixture):
    client, proxy, clients, forwarded, settings = fixture
    settings["lose_receipt"] = True
    result = run_qualification(tmp_path/"qualification", model="test-model",
        client_factory=client, proxy_factory=proxy)
    assert result["status"] == "FAILED_UNRESOLVED"
    assert len(forwarded) == result["actual_requests"] == 1
    assert result["unresolved_requests"] == 1 and len(clients) == 1
    assert list(result["phases"]) == ["PLAN_RO"]
    assert sorted(path.name for path in tmp_path.iterdir()) == ["qualification"]
    with pytest.raises(FileExistsError):
        run_qualification(tmp_path/"qualification", model="test-model",
            client_factory=client, proxy_factory=proxy)
    assert len(forwarded) == 1 and len(clients) == 1


def test_incorrect_model_artifacts_fail_without_extra_repair_budget(tmp_path, fixture):
    client, proxy, clients, forwarded, settings = fixture
    settings["omit_memory_write"] = True
    result = run_qualification(tmp_path/"qualification", model="test-model",
        client_factory=client, proxy_factory=proxy)
    assert result["status"] == "FAILED"
    assert len(forwarded) == 4 and len(clients) == 1
    assert result["checks"]["private_memory_updated"] is False
    assert not result["unresolved_requests"]


def test_shell_attempt_is_rejected_without_executing_candidate(tmp_path, fixture):
    client, proxy, clients, forwarded, settings = fixture
    settings["shell"] = True
    result = run_qualification(tmp_path/"qualification", model="test-model",
        client_factory=client, proxy_factory=proxy)
    # Native transport receives ordinary rejected-tool feedback, but the
    # diagnostic must fail if the model attempted an out-of-contract action.
    assert result["status"] == "FAILED"
    assert not result["candidate_executed"]
    assert any(row["action"] == "rejected" and not row["success"] for row in result["tool_trace"])


def test_historical_unknown_evidence_is_hash_attested_without_reconciliation(tmp_path, fixture):
    import hashlib
    client, proxy, _, _, _ = fixture
    historical = tmp_path/"old.sqlite"
    original = b"historical unknown evidence; must never be changed"
    historical.write_bytes(original)
    expected = hashlib.sha256(original).hexdigest()
    result = run_qualification(tmp_path/"qualification", model="test-model",
        historical_path=historical, historical_sha256=expected, client_factory=client, proxy_factory=proxy)
    assert result["status"] == "PASSED" and historical.read_bytes() == original
    assert result["historical_evidence"]["status"] == "HISTORICAL_UNRESOLVED_PRESERVED"
    assert result["checks"]["historical_unknown_unchanged"]
    assert result["historical_evidence"]["refunded"] is False
    with pytest.raises(BackendUnavailable, match="hash mismatch"):
        run_qualification(tmp_path/"never-created", model="test-model",
            historical_path=historical, historical_sha256="0"*64,
            client_factory=client, proxy_factory=proxy)
    assert not (tmp_path/"never-created").exists()


def test_reasoning_and_deadlines_are_frozen_before_dispatch_not_inherited(tmp_path, fixture, monkeypatch):
    client, proxy, clients, forwarded, _ = fixture
    monkeypatch.setenv("CODEX_MODEL_REASONING_EFFORT", "ultra")
    result = run_qualification(tmp_path/"qualification", model="test-model", phase_seconds=300,
        client_factory=client, proxy_factory=proxy)
    manifest = json.loads((tmp_path/"qualification"/"manifest.json").read_text())
    assert result["status"] == "PASSED" and len(forwarded) == 5
    assert result["reasoning_effort"] == manifest["reasoning_effort"] == "low"
    assert all(item.reasoning_effort == "low" for item in clients)
    assert manifest["phase_deadline_seconds"] == {"PLAN_RO": 120, "GENERATION": 300, "CONTINUITY_READ": 120}
    planning, generation = clients[0].phase_deadlines
    assert 0 < planning <= 120 and 250 < generation <= 300
    assert 0 < clients[1].phase_deadlines[0] <= 120
