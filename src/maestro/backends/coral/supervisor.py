"""Single lifecycle owner combining Coral runtime primitives with hard phase scopes."""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import time
from maestro.backends.base import BackendUnavailable
from .workspace import AgentWorkspace


@dataclass(frozen=True)
class PhaseContext:
    agent_id: str
    phase: str
    epoch: int
    workspace: AgentWorkspace
    deadline: float
    max_calls: int
    plan_version_id: str | None = None
    parent_hash: str | None = None
    read_only_solver: bool = True
    budget_scope: str = ""
    round_index: int | None = None


_REQUIRED = {"filesystem_isolation", "process_tree_limits", "monotonic_phase_deadline",
             "gateway_only_inference", "persistent_inference_quota", "private_git_objects"}


class PhaseQuiescenceUnknown(BackendUnavailable):
    """A phase may still act; the market must not treat it as a repair failure."""
    inflight_unknown = True


class ControlledCoralSupervisor:
    """The provider supplies real containment; unproved capabilities fail closed.

    Provider API: preflight()->dict[capability,bool], prepare_phase(PhaseContext)
    -> Coral AgentSandboxSpec, stop_scope(agent_id,epoch), quiescent(agent_id,epoch).
    A provider is trusted operator infrastructure, never an agent-generated plug-in.
    Injected providers in tests must identify ``simulation=True`` in their report.
    """
    def __init__(self, runtime, provider, *, model: str, log_root: str | Path,
                 simulation: bool = False, clock=time.monotonic, sleep=time.sleep):
        self.runtime, self.provider, self.model = runtime, provider, model
        self.log_root = Path(log_root)
        self.log_root.mkdir(parents=True, exist_ok=True)
        self.simulation, self.clock, self.sleep = simulation, clock, sleep
        self._handles: dict[str, tuple[object, PhaseContext]] = {}
        self._sessions: dict[str, str] = {}
        self._epochs: dict[str, int] = {}

    def preflight(self) -> dict:
        report = self.provider.preflight()
        missing = sorted(k for k in _REQUIRED if report.get(k) is not True)
        if missing:
            raise BackendUnavailable("Coral boundary lacks enforced capabilities: " + ", ".join(missing))
        if bool(report.get("simulation")) != self.simulation:
            raise BackendUnavailable("Simulation and production containment may not be interchanged")
        return report

    def stop(self, agent_id: str) -> None:
        entry = self._handles.get(agent_id)
        if entry is None:
            return
        handle, context = entry
        try:
            try:
                handle.stop()
            finally:
                self.provider.stop_scope(agent_id, context.epoch)
            quiescent = not handle.alive and self.provider.quiescent(agent_id, context.epoch)
        except Exception as error:
            raise PhaseQuiescenceUnknown("Agent scope termination could not be established") from error
        if not quiescent:
            raise PhaseQuiescenceUnknown("Old agent process tree could not be proven stopped")
        self._handles.pop(agent_id, None)
        session = self.runtime.extract_session_id(handle.log_path)
        if session:
            self._sessions[agent_id] = session

    def run_phase(self, workspace: AgentWorkspace, *, phase: str, prompt: str,
                  deadline: float, max_calls: int, plan_version_id: str | None = None,
                  parent_hash: str | None = None, budget_scope: str = "", round_index: int | None = None) -> None:
        self.preflight()
        self.stop(workspace.agent_id)
        if self.clock() >= deadline:
            raise TimeoutError("Common phase deadline elapsed before launch")
        if phase not in ("PLAN", "GENERATE"):
            raise ValueError("Coding agents run only in PLAN or GENERATE")
        if max_calls <= 0 or (phase == "PLAN" and max_calls != 1) or max_calls > 3:
            raise ValueError("Invalid V1 inference-call cap")
        if phase == "GENERATE" and (not plan_version_id or not parent_hash):
            raise PermissionError("Generation requires an immutable plan and parent binding")
        epoch = self._epochs.get(workspace.agent_id, -1) + 1
        self._epochs[workspace.agent_id] = epoch
        # Coral runtimes read this native breadcrumb for log/session identity.
        breadcrumb = workspace.solver / ".coral_agent_id"
        if breadcrumb.is_symlink():
            breadcrumb.unlink()
        if breadcrumb.exists():
            breadcrumb.chmod(0o666)
        breadcrumb.write_text(workspace.agent_id, encoding="utf-8")
        context = PhaseContext(workspace.agent_id, phase, epoch, workspace, deadline,
                               max_calls, plan_version_id, parent_hash, phase == "PLAN", budget_scope, round_index)
        spec = self.provider.prepare_phase(context)
        if not getattr(spec, "command_prefix", None) and not self.simulation:
            raise BackendUnavailable("A production Coral command must have an enforcing sandbox prefix")
        instruction_path = workspace.root / "phase-instructions.md"
        instruction_path.write_text(prompt, encoding="utf-8")
        handle = self.runtime.start(
            worktree_path=workspace.solver, coral_md_path=instruction_path,
            model=self.model, runtime_options={}, max_turns=0,
            log_dir=self.log_root / workspace.agent_id, verbose=False,
            resume_session_id=self._sessions.get(workspace.agent_id),
            prompt=prompt, prompt_source=f"maestro:{phase.lower()}",
            task_name="MAESTRO", task_description="Bounded market-controlled phase",
            gateway_url=getattr(spec, "env", {}).get("MAESTRO_GATEWAY_URL"),
            gateway_api_key=getattr(spec, "env", {}).get("MAESTRO_GATEWAY_TOKEN"),
            sandbox=spec)
        self._handles[workspace.agent_id] = (handle, context)
        try:
            while handle.alive:
                if self.clock() >= deadline:
                    raise TimeoutError(f"{phase} deadline reached")
                self.sleep(min(0.05, max(0, deadline - self.clock())))
        finally:
            self.stop(workspace.agent_id)
        process = getattr(handle, "process", None)
        if process is not None and process.returncode not in (None, 0):
            raise RuntimeError(f"Bounded Coral runtime failed with exit code {process.returncode}")

    def close(self):
        for agent_id in list(self._handles):
            self.stop(agent_id)
