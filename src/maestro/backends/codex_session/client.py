"""Use Codex's own cached ChatGPT login without handling OAuth credentials.

Only structured proposals are returned. Neither model output nor this module
launches a candidate, applies patches, issues tickets, or changes settlement.
The strict V1 factory rejects this transport until pre-inference request quota
enforcement is available; a CLI turn is deliberately not counted as a request.
"""
from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
import math
import os
from pathlib import Path, PurePosixPath
import shutil
import signal
import subprocess
import tempfile
import time

from maestro.agents.planner import parse_json_object, parse_plan_decision
from maestro.backends.base import BackendUnavailable
from maestro.execution.grants import EditGrant, GrantAuthority, GrantError


_API_ENV = {"OPENAI_API_KEY", "CODEX_API_KEY", "CODEX_ACCESS_TOKEN",
            "MAESTRO_PLANNING_API_KEY", "MAESTRO_GENERATION_API_KEY"}
_TOOL_ITEMS = {"command_execution", "file_change", "mcp_tool_call", "web_search",
               "collab_tool_call", "dynamic_tool_call", "tool_call"}
_SAFE_ITEMS = {"agent_message", "reasoning", "todo_list", "error"}


@dataclass(frozen=True)
class SessionResponse:
    value: dict
    thread_id: str
    input_tokens: int | None
    output_tokens: int | None
    response_hash: str
    completed_turns: int = 1
    actual_inference_requests: int | None = None
    diagnostic_count: int = 0


class CompletedProposalError(ValueError):
    """A completed model turn failed host validation; keep its known receipt."""
    def __init__(self, message, response):
        super().__init__(message)
        self.session_response = response


def resolve_codex(executable: str | Path | None = None) -> Path:
    """Prefer a real executable; do not interpolate prompts through cmd.exe."""
    name = str(executable or "codex")
    # A bare command name, as layered configs keep it, is looked up on PATH.
    found = shutil.which(name) if Path(name).name == name else name
    if not found:
        raise BackendUnavailable("Codex CLI is missing; install Codex CLI and run codex login")
    path = Path(found).resolve()
    if path.suffix.lower() in {".cmd", ".bat", ".ps1"}:
        # The supported npm Windows package ships the native executable here.
        package = path.parent / "node_modules" / "@openai" / "codex"
        machine = __import__("platform").machine().lower()
        target = "aarch64-pc-windows-msvc" if "arm" in machine else "x86_64-pc-windows-msvc"
        candidates = list(package.glob(f"node_modules/@openai/codex-*/vendor/{target}/bin/codex.exe"))
        candidates += list(package.glob(f"vendor/{target}/bin/codex.exe"))
        if not candidates:
            raise BackendUnavailable("Pass the installed native codex.exe path; shell wrappers are not executed")
        path = candidates[0].resolve()
    if not path.is_file():
        raise BackendUnavailable("Configured Codex executable does not exist")
    return path


def _session_environment(source=None):
    # Codex owns token refresh and secure-store access. Preserve CODEX_HOME but
    # ensure an unrelated exported API key cannot override the intended login.
    return {key: value for key, value in dict(os.environ if source is None else source).items()
            if key.upper() not in _API_ENV}


def _run(argv, *, input_text, cwd, env, timeout, max_bytes):
    """No shell or automatic retry; terminate the local tree on timeout.

    Terminating the CLI cannot establish that an already dispatched remote
    inference stopped. This runner is not a strict compute accounting boundary.
    """
    options = {"start_new_session": True} if os.name != "nt" else {
        "creationflags": subprocess.CREATE_NO_WINDOW | subprocess.CREATE_NEW_PROCESS_GROUP}
    with tempfile.TemporaryFile() as output, tempfile.TemporaryFile() as error:
        process = subprocess.Popen(argv, cwd=cwd, env=env, stdin=subprocess.PIPE,
            stdout=output, stderr=error, **options)
        try:
            process.communicate(input_text.encode("utf-8"), timeout=timeout)
        except subprocess.TimeoutExpired:
            if os.name == "nt":
                subprocess.run(["taskkill", "/PID", str(process.pid), "/T", "/F"],
                    capture_output=True, check=False, timeout=10,
                    creationflags=subprocess.CREATE_NO_WINDOW)
            else:
                try:
                    os.killpg(process.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
            process.kill()
            process.wait(timeout=10)
            raise TimeoutError("Codex session deadline reached; remote inference status is unknown") from None
        output.seek(0)
        raw = output.read(max_bytes + 1)
        error.seek(0)
        stderr = error.read(min(max_bytes, 65536))
    if len(raw) > max_bytes:
        raise ValueError("Codex event stream exceeded the accepted response size")
    return subprocess.CompletedProcess(argv, process.returncode,
        raw.decode("utf-8", "strict"), stderr.decode("utf-8", "replace"))


class CodexSessionClient:
    """A logged-in, read-only proposal broker for an explicitly separate pilot.

    The CLI inherits its own cache via CODEX_HOME; MAESTRO never opens auth.json.
    ``runner`` and ``executable`` are injectable for interface tests. No network
    call occurs during construction or login_status(). The supplied schemas are
    model-side formatting instructions, with semantic validation by the host.
    """
    is_live = True
    planning_capabilities = {"single_request": False, "hard_request_timeout": False,
                             "mutates_input_state": False}

    def __init__(self, *, model: str | None = None, work_root: str | Path,
                 executable: str | Path | None = None, runner=None,
                 timeout: float = 120, max_response_bytes: int = 1024 * 1024,
                 environment=None):
        if model is not None and (not isinstance(model, str) or not model.strip()):
            raise ValueError("Use an explicit model name or None for a default-model probe")
        if not isinstance(timeout, (float, int)) or not math.isfinite(timeout) or timeout <= 0:
            raise ValueError("Positive finite timeout required")
        if type(max_response_bytes) is not int or max_response_bytes < 1024:
            raise ValueError("Invalid accepted response size")
        self.model, self.executable = model, resolve_codex(executable)
        self.root = Path(work_root).resolve()
        self.root.mkdir(parents=True, exist_ok=True)
        self.runner, self.timeout = runner or _run, timeout
        self.max_response_bytes = max_response_bytes
        self.env = _session_environment(environment)

    def login_status(self, *, deadline=None) -> dict:
        """Return a sanitized auth-method report, never raw CLI output."""
        remaining = min(15, self.timeout)
        if deadline is not None:
            remaining = min(remaining, deadline-time.monotonic())
            if remaining <= 0:
                raise TimeoutError("Codex login probe missed the common deadline")
        response = self.runner([str(self.executable), "login", "status"],
            input_text="", cwd=self.root, env=self.env, timeout=remaining,
            max_bytes=65536)
        output = (response.stdout + "\n" + response.stderr).lower()
        authenticated = response.returncode == 0 and "logged in using chatgpt" in output
        return {"authenticated": authenticated,
                "auth_method": "chatgpt" if authenticated else "unavailable_or_other",
                "external_api_key_required": False,
                "strict_v1_request_quota": False}

    def require_chatgpt_login(self, *, deadline=None):
        report = self.login_status(deadline=deadline)
        if not report["authenticated"]:
            raise BackendUnavailable("Local Codex ChatGPT login is unavailable; run codex login (no API token required)")
        return report

    def structured_proposal(self, context: dict, schema: dict, *, purpose: str,
                            deadline: float | None = None) -> SessionResponse:
        if purpose not in {"planning", "generation"}:
            raise ValueError("Only planning and generation proposals are supported")
        if not isinstance(context, dict) or not isinstance(schema, dict):
            raise TypeError("JSON context and output schema objects required")
        serialized = json.dumps(context, ensure_ascii=False, sort_keys=True, allow_nan=False)
        if len(serialized.encode()) > 128 * 1024:
            raise ValueError("Proposal context exceeds its accepted byte allowance")
        remaining = self.timeout
        if deadline is not None:
            if not isinstance(deadline, (int, float)) or not math.isfinite(deadline):
                raise ValueError("Finite monotonic deadline required")
            remaining = min(remaining, deadline - time.monotonic())
            if remaining <= 0:
                raise TimeoutError("Proposal deadline elapsed before CLI dispatch")
        self.require_chatgpt_login(deadline=deadline)
        if deadline is not None:
            remaining = min(remaining, deadline-time.monotonic())
            if remaining <= 0:
                raise TimeoutError("Proposal deadline elapsed during login probe")
        # A dedicated empty broker working directory avoids agent/project rules,
        # peer files, the evaluator panel, and solver execution. All useful input
        # comes from the host's already filtered frozen context.
        with tempfile.TemporaryDirectory(prefix="proposal-", dir=self.root) as directory:
            schema_path = Path(directory) / "response.schema.json"
            schema_path.write_text(json.dumps(schema, allow_nan=False), encoding="utf-8")
            argv = [str(self.executable), "exec", "--ignore-user-config", "--ephemeral",
                "--skip-git-repo-check", "--sandbox", "read-only", "--json", "--color", "never",
                "--output-schema", str(schema_path), "--cd", directory]
            if self.model is not None:
                argv.extend(["--model", self.model])
            for setting in ('forced_login_method="chatgpt"', 'approval_policy="never"',
                'features.shell_tool=false', 'features.hooks=false', 'features.memories=false',
                'features.multi_agent=false', 'agents.enabled=false', 'features.remote_plugin=false',
                'apps._default.enabled=false', 'web_search="disabled"', 'tools.view_image=false',
                'project_doc_max_bytes=0'):
                argv.extend(["-c", setting])
            argv.append("-")
            prompt = ("Return only the JSON proposal matching the provided schema. "
                "Use only the supplied frozen context. Do not call tools, inspect local files, "
                "edit files, run commands/tests, access a benchmark, or request evaluation. "
                f"Purpose: {purpose}. The host validates authorization and applies any later edits.\n" + serialized)
            response = self.runner(argv, input_text=prompt, cwd=directory, env=self.env,
                timeout=remaining, max_bytes=self.max_response_bytes)
        if response.returncode:
            # CLI diagnostics may contain sensitive account/config information.
            raise BackendUnavailable(f"Codex session failed (exit {response.returncode}); inspect local CLI health without exporting credentials")
        thread_id, answer, completed, usage, diagnostics = "", None, 0, {}, 0
        for line in response.stdout.splitlines():
            if not line.strip():
                continue
            event = json.loads(line)
            if not isinstance(event, dict):
                raise ValueError("Codex event object required")
            kind = event.get("type")
            if kind in {"turn.failed", "error"}:
                raise BackendUnavailable("Codex reported a failed proposal turn")
            if kind == "thread.started":
                thread_id = str(event.get("thread_id", ""))
            if kind == "turn.completed":
                completed += 1
                usage = event.get("usage", {})
            if kind in {"item.started", "item.updated", "item.completed"}:
                item = event.get("item", {})
                item_type = item.get("type")
                if item_type in _TOOL_ITEMS or item_type not in _SAFE_ITEMS:
                    raise BackendUnavailable("Tool activity or an unqualified item appeared in the read-only proposal broker")
                if kind == "item.completed" and item_type == "error":
                    # Observed CLI 0.156.1 emits nonfatal startup diagnostics
                    # this way, then completes a valid turn. Count only; never
                    # export their potentially sensitive message/body.
                    diagnostics += 1
                if kind == "item.completed" and item_type == "agent_message":
                    answer = item.get("text")
        if completed != 1 or not isinstance(answer, str) or not thread_id:
            raise BackendUnavailable("Codex returned no complete, identified single proposal turn")
        def token_count(name):
            amount = usage.get(name) if isinstance(usage, dict) else None
            if amount is not None and (type(amount) is not int or amount < 0):
                raise ValueError("Invalid reported Codex token count")
            return amount
        receipt = SessionResponse({}, thread_id, token_count("input_tokens"),
            token_count("output_tokens"), hashlib.sha256(answer.encode()).hexdigest(),
            diagnostic_count=diagnostics)
        try:
            value, _ = parse_json_object(answer)
        except (TypeError, ValueError) as error:
            raise CompletedProposalError("Completed Codex proposal is not valid JSON", receipt) from error
        from dataclasses import replace
        receipt = replace(receipt, value=value)
        if deadline is not None and time.monotonic() >= deadline:
            raise CompletedProposalError("Codex proposal completed after the common deadline", receipt)
        return receipt

    def plan(self, state, round_index, context, *, deadline=None, namespace="codex-session-pilot"):
        response = self.structured_proposal(context, _PLAN_SCHEMA, purpose="planning", deadline=deadline)
        try:
            decision = parse_plan_decision(json.dumps(response.value), state, round_index, namespace=namespace)
        except (TypeError, ValueError) as error:
            raise CompletedProposalError("Completed Codex plan failed host validation", response) from error
        return decision, response

    def propose_files(self, context, *, grant: EditGrant, authority: GrantAuthority,
                      episode_id: str, round_index: int, agent_id: str, plan_version_id: str,
                      parent_hash: str, epoch: int, deadline=None):
        """Verify an EditGrant BEFORE inference; return data, never execute it."""
        if not isinstance(grant, EditGrant) or not isinstance(authority, GrantAuthority):
            raise GrantError("A trusted edit authority and signed EditGrant are required")
        authority.verify(grant, agent_id=agent_id, plan_version_id=plan_version_id,
                         parent_hash=parent_hash, epoch=epoch)
        if (grant.episode_id, grant.round_index) != (episode_id, round_index):
            raise GrantError("EditGrant is for another episode or round")
        response = self.structured_proposal({"bindings": {"episode_id": episode_id,
            "round_index": round_index, "agent_id": agent_id, "plan_version_id": plan_version_id,
            "parent_hash": parent_hash, "epoch": epoch}, "context": context},
            _FILES_SCHEMA, purpose="generation", deadline=deadline)
        try:
            # Recheck expiry/revocation after waiting, before accepting edits.
            authority.verify(grant, agent_id=agent_id, plan_version_id=plan_version_id,
                             parent_hash=parent_hash, epoch=epoch)
            value = response.value
            files = value.get("files")
            if set(value) != {"files"} or not isinstance(files, list) or len(files) > 32:
                raise ValueError("Invalid proposed file manifest")
            seen, total = set(), 0
            for item in files:
                if not isinstance(item, dict) or set(item) != {"path", "content"}:
                    raise ValueError("Each proposed file requires exactly path/content")
                name, content = item["path"], item["content"]
                if not isinstance(name, str) or not isinstance(content, str):
                    raise ValueError("Proposed file path/content must be text")
                path = PurePosixPath(name)
                if (not name or path.is_absolute() or "\\" in name or ":" in name
                    or any(_unsafe_component(part) for part in name.split("/"))
                    or name != path.as_posix() or name.casefold() in seen):
                    raise ValueError("Proposed file path is unsafe or duplicated")
                seen.add(name.casefold())
                total += len(content.encode())
            if total > 128 * 1024:
                raise ValueError("Proposed file content exceeds its accepted byte allowance")
        except (TypeError, ValueError, GrantError) as error:
            raise CompletedProposalError("Completed Codex files failed host authorization/validation", response) from error
        return response


def _unsafe_component(part):
    """Portable path contract also rejects Windows device names/metadata aliases."""
    lower = part.casefold()
    reserved = {"con", "prn", "aux", "nul", *(f"com{i}" for i in range(1, 10)),
                *(f"lpt{i}" for i in range(1, 10))}
    return (not part or part.endswith((".", " ")) or lower in {
        ".", "..", ".git", ".coral", ".codex", ".claude", ".opencode", ".coral_agent_id"}
        or lower.split(".", 1)[0] in reserved)


_PLAN_SCHEMA = {"type": "object", "additionalProperties": False,
    "properties": {"action": {"type": "string", "enum": ["retain", "revise", "withdraw"]},
        "plan": {"anyOf": [{"type": "null"}, {"type": "object", "additionalProperties": False,
            "properties": {name: ({"type": "array", "items": {"type": "string"}}
                if name == "evidence_run_ids" else
                {"type": "string", "minLength": 1, "maxLength": 64} if name in {"target", "mod_type"} else
                {"type": "string"})
                for name in ("target", "mod_type", "diagnosis", "change", "expected_effect", "evidence_run_ids")},
            "required": ["target", "mod_type", "diagnosis", "change", "expected_effect", "evidence_run_ids"]}]},
        "lessons": {"type": "array", "maxItems": 0, "items": {"type": "string"}},
        "reason": {"type": "string"}},
    "required": ["action", "plan", "lessons", "reason"]}

_FILES_SCHEMA = {"type": "object", "additionalProperties": False,
    "properties": {"files": {"type": "array", "maxItems": 32, "items": {"type": "object",
        "additionalProperties": False, "properties": {"path": {"type": "string"},
            "content": {"type": "string"}}, "required": ["path", "content"]}}}, "required": ["files"]}


def strict_v1_factory(config, root=None):
    """Native autonomous Codex, metered before every actual HTTP request."""
    from .autonomous_backend import strict_factory
    return strict_factory(config, root)
