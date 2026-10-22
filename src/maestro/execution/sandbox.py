"""Fail-closed untrusted candidate containment, with aggregate cgroup CPU metering.

Docker is optional. The real runner requires a rootless Linux Docker daemon, a
pinned image containing Python 3 and readable cgroup-v2 cpu.stat. No unrestricted
native subprocess fallback exists on Windows or on an unsupported Linux host.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass, field
import json
import os
from pathlib import Path, PurePosixPath
import shutil
import subprocess
import time
from typing import Protocol


class SandboxUnavailable(RuntimeError):
    pass


class ExecutionQuiescenceUnknown(SandboxUnavailable):
    """The executor cannot prove its process tree has stopped.

    A shared CPU worker pool must retain/poison the permit on this error. A
    failed transport is not evidence that the container or its children exited.
    """

    execution_unknown = True
    inflight_unknown = True


@dataclass(frozen=True)
class ResourceLimits:
    cpu_seconds: float = 30.0
    wall_seconds: float = 45.0
    memory_mb: int = 512
    pids: int = 32
    output_bytes: int = 1_000_000
    cpus: float = 1.0

    def __post_init__(self):
        if min(self.cpu_seconds, self.wall_seconds, self.memory_mb,
               self.pids, self.output_bytes, self.cpus) <= 0:
            raise ValueError("All containment limits must be positive")


@dataclass(frozen=True)
class Mount:
    source: Path
    target: str
    read_only: bool = True


@dataclass(frozen=True)
class RunRequest:
    argv: tuple[str, ...]
    workdir: Path
    stdin: str = ""
    mounts: tuple[Mount, ...] = ()
    image: str | None = None
    container_workdir: str = "/scratch"


@dataclass(frozen=True)
class RunReceipt:
    returncode: int
    stdout: str
    stderr: str
    cpu_seconds: float
    wall_seconds: float
    timed_out: bool = False
    diagnostics: dict = field(default_factory=dict)


class ContainmentRunner(Protocol):
    def run(self, request: RunRequest, limits: ResourceLimits) -> RunReceipt: ...


class FailClosedRunner:
    def run(self, request: RunRequest, limits: ResourceLimits) -> RunReceipt:
        raise SandboxUnavailable("No verified containment runner configured; execution denied")


# This program executes *inside* the isolated container. All processes share the
# container cgroup; cpu.stat counts the whole tree including detached children.
_CONTAINER_LAUNCHER = r'''
import ctypes, json, math, os, pathlib, resource, signal, subprocess, sys, tempfile, time
limits=json.loads(sys.argv[1]); argv=json.loads(sys.argv[2])
ctypes.CDLL(None).prctl(4,0,0,0,0)  # PR_SET_DUMPABLE: protect monitor's fds/memory.
def cpu():
    data=dict(line.split() for line in pathlib.Path('/sys/fs/cgroup/cpu.stat').read_text().splitlines())
    return int(data['usage_usec'])/1e6
try:
    cg=pathlib.Path('/sys/fs/cgroup')
    quota,period=(cg/'cpu.max').read_text().split()
    assert quota!='max' and int(quota)/int(period)<=limits['cpus']+.001
    memory=(cg/'memory.max').read_text().strip();pids=(cg/'pids.max').read_text().strip()
    assert memory!='max' and int(memory)<=limits['memory_mb']*1024*1024
    assert pids!='max' and int(pids)<=limits['pids']
    initial=cpu()
except Exception as exc:
    print(json.dumps({'sandbox_unavailable':'cgroup-v2 CPU accounting unavailable'}));sys.exit(125)
resource.setrlimit(resource.RLIMIT_FSIZE,(limits['output_bytes'],limits['output_bytes']))
resource.setrlimit(resource.RLIMIT_NOFILE,(64,64))
start=time.monotonic(); timed=False; reason='COMPLETED'
sampled_pids=0
def optional_int(name):
    try:return int((cg/name).read_text().strip())
    except (OSError,ValueError):return None
with tempfile.TemporaryFile() as out, tempfile.TemporaryFile() as err:
    try:
        def drop_privileges():
            os.setgroups([]);os.setgid(65534);os.setuid(65534)
            libc=ctypes.CDLL(None);libc.prctl(38,1,0,0,0);libc.prctl(4,0,0,0,0)
        child=subprocess.Popen(argv,stdin=sys.stdin.buffer,stdout=out,stderr=err,start_new_session=True,preexec_fn=drop_privileges)
        while child.poll() is None:
            current_pids=optional_int('pids.current')
            if current_pids is not None:sampled_pids=max(sampled_pids,current_pids)
            elapsed=time.monotonic()-start; consumed=cpu()-initial
            if consumed>=limits['cpu_seconds'] or elapsed>=limits['wall_seconds']:
                timed=True;reason='CPU_LIMIT' if consumed>=limits['cpu_seconds'] else 'WALL_LIMIT'
                os.kill(-1,signal.SIGKILL);child.wait();break
            if os.fstat(out.fileno()).st_size+os.fstat(err.fileno()).st_size>limits['output_bytes']:
                reason='OUTPUT_LIMIT';os.kill(-1,signal.SIGKILL);child.wait();break
            time.sleep(.02)
        consumed=max(0,cpu()-initial);elapsed=time.monotonic()-start
        # Main-child exit cannot leave a free-running orphan in this container.
        try:os.kill(-1,signal.SIGKILL)
        except ProcessLookupError:pass
        out.seek(0);err.seek(0)
        stdout=out.read(limits['output_bytes']).decode('utf-8','replace')
        stderr=err.read(max(0,limits['output_bytes']-len(stdout.encode()))).decode('utf-8','replace')
        print(json.dumps({'returncode':child.returncode,'stdout':stdout,'stderr':stderr,'cpu_seconds':consumed,'wall_seconds':elapsed,'timed_out':timed,'diagnostics':{'reason':reason,'aggregate_cgroup_cpu':True,'memory_peak_bytes':optional_int('memory.peak'),'pids_peak':optional_int('pids.peak'),'sampled_pids_peak':sampled_pids or None,'stdout_file_bytes':os.fstat(out.fileno()).st_size,'stderr_file_bytes':os.fstat(err.fileno()).st_size}}))
    except Exception as exc:
        print(json.dumps({'returncode':126,'stdout':'','stderr':str(exc),'cpu_seconds':max(0,cpu()-initial),'wall_seconds':time.monotonic()-start,'timed_out':False,'diagnostics':{'reason':'LAUNCH_FAILED'}}))
'''


class DockerRunner:
    """Networkless immutable-root candidate runner with no host socket mounts.

    Image provisioning is an operator action: this class never pulls an image.
    ``preflight`` checks the installed image and daemon, not production science.
    """
    def __init__(self, image: str | None = None, *, executable: str = "docker",
                 require_rootless: bool = True):
        self.image, self.executable, self.require_rootless = image, executable, require_rootless

    @staticmethod
    def _docker_environment() -> dict[str, str]:
        # Twenty attached Go clients share the controller's task quota. Bound
        # client scheduler parallelism; this does not enter candidate env/limits.
        return {**os.environ, "GOMAXPROCS": "2"}

    def _docker(self, args: list[str], *, timeout: float = 15) -> subprocess.CompletedProcess:
        try:
            return subprocess.run([self.executable, *args], capture_output=True, text=True,
                                  timeout=timeout, check=False, env=self._docker_environment())
        except (FileNotFoundError, subprocess.TimeoutExpired, OSError) as error:
            raise SandboxUnavailable(f"Docker unavailable: {error}") from error

    def _remove_and_confirm(self, container_id: str) -> None:
        """Do not release execution capacity until removal is acknowledged.

        A successful force-removal acknowledges that the container has stopped
        and its identity no longer exists. If that operation loses its response,
        only a successful daemon listing with no matching identity confirms
        absence. An inspect/list error by itself never proves disappearance.
        """
        removal_error = None
        try:
            removed = self._docker(["rm", "--force", container_id])
            if removed.returncode == 0:
                return
            removal_error = SandboxUnavailable("Docker force-removal was not acknowledged")
        except Exception as error:
            removal_error = error
        try:
            remaining = self._docker([
                "container", "ls", "--all", "--no-trunc", "--quiet",
                "--filter", f"id={container_id}"])
            if remaining.returncode == 0 and not remaining.stdout.strip():
                return
        except Exception as error:
            raise ExecutionQuiescenceUnknown(
                f"Cannot confirm contained execution stopped: {container_id}") from error
        raise ExecutionQuiescenceUnknown(
            f"Cannot confirm contained execution stopped: {container_id}") from removal_error

    def preflight(self, image: str | None = None) -> dict:
        image = image or self.image
        if not image or "@sha256:" not in image:
            raise SandboxUnavailable("A digest-pinned, locally installed candidate image is required")
        info = self._docker(["info", "--format", "{{json .}}"])
        if info.returncode:
            raise SandboxUnavailable("Docker daemon is unavailable")
        try:
            data = json.loads(info.stdout)
        except (ValueError, TypeError) as error:
            raise SandboxUnavailable("Cannot verify Docker daemon security settings") from error
        if data.get("OSType") != "linux":
            raise SandboxUnavailable("Candidate images require a Linux isolation kernel")
        if self.require_rootless and not any("rootless" in s for s in data.get("SecurityOptions", [])):
            raise SandboxUnavailable("Rootless Docker is required by the default containment profile")
        if str(data.get("CgroupVersion")) != "2":
            raise SandboxUnavailable("Aggregate CPU limits require cgroup v2")
        inspected = self._docker(["image", "inspect", image])
        if inspected.returncode:
            raise SandboxUnavailable("Pinned image is not installed; automatic pulling is disabled")
        return {"image": image, "os": "linux", "cgroup_version": 2,
                "rootless": self.require_rootless}

    def run(self, request: RunRequest, limits: ResourceLimits) -> RunReceipt:
        image = request.image or self.image
        self.preflight(image)
        if not request.argv or not all(isinstance(arg, str) for arg in request.argv):
            raise ValueError("Candidate argv must be a nonempty sequence of strings")
        if request.container_workdir != "/scratch":
            raise SandboxUnavailable("Only the bounded writable /scratch workdir is supported")
        command = ["create", "--interactive", "--network=none", "--cgroupns=private", "--read-only", "--cap-drop=ALL",
                   "--security-opt=no-new-privileges:true", "--user=0:0",
                   "--cap-add=SETUID", "--cap-add=SETGID", "--cap-add=KILL",
                   f"--memory={limits.memory_mb}m", f"--memory-swap={limits.memory_mb}m",
                   f"--pids-limit={limits.pids}", f"--cpus={limits.cpus}",
                   "--ulimit", "core=0", "--ulimit", "nofile=64:64",
                   "--tmpfs", f"/scratch:rw,nosuid,size={limits.memory_mb}m,mode=1777",
                   "--workdir=/scratch", "--entrypoint=python3"]
        targets = set()
        for mount in request.mounts:
            source = Path(mount.source).resolve(strict=True)
            target = PurePosixPath(mount.target)
            if not target.is_absolute() or ".." in target.parts or target.parts[1:2] not in (("candidate",), ("input",)):
                raise SandboxUnavailable("Only /candidate and /input mounts are allowed")
            if not mount.read_only:
                raise SandboxUnavailable("Host mounts must be read-only")
            if str(target) in targets or "," in str(source):
                raise SandboxUnavailable("Duplicate target or unsupported mount path")
            targets.add(str(target))
            command.extend(["--mount", f"type=bind,source={source},target={target},readonly"])
        command.extend([str(image), "-c", _CONTAINER_LAUNCHER,
                        json.dumps(asdict(limits)), json.dumps(request.argv)])
        created = self._docker(command)
        if created.returncode:
            raise SandboxUnavailable(f"Cannot create contained execution: {created.stderr.strip()}")
        container_id = created.stdout.strip()
        if len(container_id) < 12 or not all(ch in "0123456789abcdef" for ch in container_id):
            raise SandboxUnavailable("Docker returned an invalid container identity")
        start = time.monotonic()
        try:
            try:
                completed = subprocess.run([self.executable, "start", "--attach", "--interactive", container_id],
                                           input=request.stdin, capture_output=True, text=True,
                                           timeout=limits.wall_seconds + 20, check=False,
                                           env=self._docker_environment())
            except subprocess.TimeoutExpired:
                # A failed kill is recoverable only if the finally block can
                # independently confirm force-removal. Never infer quiescence
                # from the watchdog or a failed Docker command alone.
                try:
                    killed = self._docker(["kill", container_id])
                    kill_confirmed = killed.returncode == 0
                except Exception:
                    kill_confirmed = False
                return RunReceipt(-9, "", "Host containment watchdog elapsed", 0.0,
                                  time.monotonic() - start, True,
                                  {"reason": "CONTAINMENT_WATCHDOG", "container_id": container_id,
                                   "cpu_measurement_incomplete": True,
                                   "watchdog_kill_confirmed": kill_confirmed})
            try:
                data = json.loads(completed.stdout)
            except ValueError as error:
                raise SandboxUnavailable("Container did not provide a valid metering receipt") from error
            if "sandbox_unavailable" in data:
                raise SandboxUnavailable(data["sandbox_unavailable"])
            data.setdefault("diagnostics", {})["container_id"] = container_id
            return RunReceipt(**data)
        finally:
            self._remove_and_confirm(container_id)
