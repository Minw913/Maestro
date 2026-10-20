"""Trusted host command-prefix shim used by Coral's runtime start().

Run as a file with ``--scope <private-json> -- <runtime argv>``. The scope is
created only by DockerAgentSandboxProvider and never mounted for the agent.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import subprocess
import sys
import tarfile
import tempfile
import shutil


_MONITOR = r'''
import ctypes,json,os,pathlib,resource,select,signal,socket,socketserver,subprocess,sys,threading,time,shutil,tarfile
settings=json.loads(sys.argv[1]);argv=json.loads(sys.argv[2]);limits=settings['limits']
ctypes.CDLL(None).prctl(4,0,0,0,0)
resource.setrlimit(resource.RLIMIT_NOFILE,(128,128))
def cpu():
    data=dict(line.split() for line in pathlib.Path('/sys/fs/cgroup/cpu.stat').read_text().splitlines())
    return int(data['usage_usec'])/1e6
def receipt(data):
    target=pathlib.Path('/authority/receipt.json');tmp=target.with_suffix('.tmp')
    with tmp.open('w') as f:json.dump(data,f);f.flush();os.fsync(f.fileno())
    tmp.replace(target)
try:
    cg=pathlib.Path('/sys/fs/cgroup');quota,period=(cg/'cpu.max').read_text().split()
    assert quota!='max' and int(quota)/int(period)<=limits['cpus']+.001
    memory=(cg/'memory.max').read_text().strip();pids=(cg/'pids.max').read_text().strip()
    assert memory!='max' and int(memory)<=limits['memory_mb']*1024*1024
    assert pids!='max' and int(pids)<=limits['pids']
    initial=cpu()
except Exception:
    receipt({'status':'UNKNOWN','cpu_seconds':limits['cpu_seconds'],'reason':'NO_CGROUP_ACCOUNTING'});sys.exit(125)
started=time.monotonic();finished=threading.Event()
def hard_watchdog():
    while not finished.wait(.01):
        if cpu()-initial>=limits['cpu_seconds'] or time.monotonic()-started>=limits['wall_seconds']:
            receipt({'status':'UNKNOWN','cpu_seconds':limits['cpu_seconds'],'reason':'COMPUTE_LIMIT_DURING_SCOPE_OR_EXPORT','wall_seconds':time.monotonic()-started})
            try:os.kill(-1,signal.SIGKILL)
            except ProcessLookupError:pass
            os._exit(125)
threading.Thread(target=hard_watchdog,daemon=True).start()
for target,source in settings.get('writable',[]):
    shutil.copytree(source,target,dirs_exist_ok=True,symlinks=False)
    for item in [pathlib.Path(target),*pathlib.Path(target).rglob('*')]:
        if item.is_symlink():raise RuntimeError('Symlinks are forbidden in writable views')
        item.chmod(0o777 if item.is_dir() or item.stat().st_mode&0o111 else 0o666)
class Relay(socketserver.BaseRequestHandler):
    def handle(self):
        peer=socket.socket(socket.AF_UNIX,socket.SOCK_STREAM)
        try:
            peer.connect('/gateway/socket')
            sockets=[self.request,peer]
            while True:
                readable,_,_=select.select(sockets,[],[],60)
                if not readable:return
                for source in readable:
                    data=source.recv(65536)
                    if not data:return
                    (peer if source is self.request else self.request).sendall(data)
        finally:peer.close()
class Server(socketserver.ThreadingMixIn,socketserver.TCPServer):daemon_threads=True
server=Server(('127.0.0.1',settings['gateway_port']),Relay)
threading.Thread(target=server.serve_forever,daemon=True).start()
start=time.monotonic();count=0;output_exceeded=False;lock=threading.Lock()
def stream(pipe,target):
    global count,output_exceeded
    while True:
        chunk=os.read(pipe.fileno(),8192)
        if not chunk:return
        with lock:
            remaining=max(0,limits['output_bytes']-count);count+=len(chunk)
            if count>limits['output_bytes']:output_exceeded=True
            if remaining:target.buffer.write(chunk[:remaining]);target.flush()
def drop():
    resource.setrlimit(resource.RLIMIT_FSIZE,(limits['output_bytes'],limits['output_bytes']))
    os.setgroups([]);os.setgid(65534);os.setuid(65534)
    libc=ctypes.CDLL(None);libc.prctl(38,1,0,0,0);libc.prctl(4,0,0,0,0)
status='COMPLETED';reason='EXITED'
try:
    child=subprocess.Popen(argv,stdout=subprocess.PIPE,stderr=subprocess.PIPE,stdin=sys.stdin,
                           start_new_session=True,preexec_fn=drop)
    threads=[threading.Thread(target=stream,args=(child.stdout,sys.stdout),daemon=True),
             threading.Thread(target=stream,args=(child.stderr,sys.stderr),daemon=True)]
    for thread in threads:thread.start()
    while child.poll() is None:
        used=cpu()-initial;wall=time.monotonic()-start
        if used>=limits['cpu_seconds'] or wall>=limits['wall_seconds'] or output_exceeded:
            status='LIMIT';reason='OUTPUT' if output_exceeded else ('CPU' if used>=limits['cpu_seconds'] else 'WALL')
            os.kill(-1,signal.SIGKILL);child.wait();break
        time.sleep(.02)
    try:os.kill(-1,signal.SIGKILL)
    except ProcessLookupError:pass
    for thread in threads:thread.join(timeout=1)
    exported=0;files=0
    export_limit=settings['workspace_bytes']+settings['workspace_files']*4096
    resource.setrlimit(resource.RLIMIT_FSIZE,(export_limit,export_limit))
    with tarfile.open('/authority/export.tar','w') as archive:
        for target,source in settings.get('writable',[]):
            base=pathlib.Path(target);name=settings['export_names'][target]
            for item in base.rglob('*'):
                if item.is_symlink() or not (item.is_dir() or item.is_file()):raise RuntimeError('Unsupported exported artifact type')
                files+=1
                if files>settings['workspace_files'] or len(str(item.relative_to(base)).encode())>4096:raise RuntimeError('Export entry/path quota exceeded')
                if item.is_file():
                    exported+=item.stat().st_size
                    if exported>settings['workspace_bytes'] or files>settings['workspace_files']:raise RuntimeError('Writable artifact quota exceeded')
                archive.add(item,arcname=name+'/'+item.relative_to(base).as_posix(),recursive=False)
    receipt({'status':status,'reason':reason,'returncode':child.returncode,
             'cpu_seconds':max(0,cpu()-initial),'wall_seconds':time.monotonic()-start,'output_bytes':count,
             'exported_bytes':exported,'exported_files':files})
    finished.set()
    sys.exit(child.returncode if child.returncode>=0 else 128-child.returncode)
except BaseException as exc:
    if not isinstance(exc,SystemExit):
        receipt({'status':'UNKNOWN','reason':type(exc).__name__,'cpu_seconds':limits['cpu_seconds'],
                 'wall_seconds':time.monotonic()-start});sys.exit(125)
    raise
'''


def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--scope", required=True)
    parser.add_argument("command", nargs=argparse.REMAINDER)
    args = parser.parse_args(argv)
    command = args.command[1:] if args.command[:1] == ["--"] else args.command
    scope = json.loads(Path(args.scope).read_text())
    if not command or Path(command[0]).name != scope["runtime_command"]:
        raise PermissionError("Runtime command is not allowed by this phase scope")
    command[0] = scope["runtime_command"]
    limits = scope["limits"]
    docker = scope["docker"]
    create = [docker, "create", "--name", scope["container_name"], "--interactive",
              "--network=none", "--cgroupns=private", "--read-only", "--cap-drop=ALL", "--cap-add=SETUID",
              "--cap-add=SETGID", "--cap-add=KILL", "--security-opt=no-new-privileges:true", "--user=0:0",
              f"--cpus={limits['cpus']}", f"--memory={limits['memory_mb']}m",
              f"--memory-swap={limits['memory_mb']}m", f"--pids-limit={limits['pids']}",
              "--ulimit", "core=0", "--ulimit", "nofile=128:128",
              "--tmpfs", f"/tmp:rw,nosuid,size={limits['memory_mb']}m,mode=1777",
              "--workdir", scope["workdir"], "--entrypoint=python3"]
    for destination, source in scope.get("writable", []):
        create += ["--tmpfs", f"{destination}:rw,nosuid,size={scope['workspace_bytes']//4},nr_inodes={scope['workspace_files']//4},mode=1777"]
    for source, target, readonly in scope["mounts"]:
        if "," in source or "," in target:
            raise ValueError("Unsupported mount path")
        create += ["--mount", f"type=bind,source={source},target={target}" + (",readonly" if readonly else "")]
    for key, value in scope["environment"].items():
        create += ["--env", f"{key}={value}"]
    create += [scope["image"], "-c", _MONITOR,
               json.dumps({"limits": limits, "gateway_port": scope["gateway_port"],
                    "writable": scope.get("writable", []), "export_names": scope.get("export_names", {}),
                    "workspace_bytes": scope.get("workspace_bytes", 64*1024*1024),
                    "workspace_files": scope.get("workspace_files", 8192)}), json.dumps(command)]
    made = subprocess.run(create, capture_output=True, text=True, timeout=30)
    if made.returncode:
        raise RuntimeError("Cannot provision the bounded Coral container")
    try:
        process = subprocess.Popen([docker, "start", "--attach", "--interactive", scope["container_name"]])
        try:
            result = process.wait(timeout=limits["wall_seconds"] + 15)
            if scope.get("writable"):
                sync_export(scope)
            return result
        except subprocess.TimeoutExpired:
            subprocess.run([docker, "kill", scope["container_name"]], capture_output=True, timeout=15)
            process.wait(timeout=10)
            return 124
    finally:
        # The supervisor also removes the named scope after signals or a crash.
        subprocess.run([docker, "rm", "--force", scope["container_name"]], capture_output=True, timeout=15)


def sync_export(scope):
    """Validate bounded root-monitor archive before updating this agent's files."""
    authority = Path(scope["authority_dir"])
    receipt = authority / "receipt.json"
    if not receipt.exists() or json.loads(receipt.read_text()).get("status") not in ("COMPLETED", "LIMIT"):
        raise RuntimeError("No trustworthy stopped-scope export receipt")
    archive_path = authority / "export.tar"
    if archive_path.stat().st_size > scope["workspace_bytes"] + scope["workspace_files"]*4096:
        raise RuntimeError("Export archive exceeds its bounded metadata/data size")
    with tempfile.TemporaryDirectory(prefix="sync-", dir=authority) as folder:
        stage = Path(folder)
        total, count, seen = 0, 0, set()
        with tarfile.open(archive_path, "r:") as archive:
            for member in archive:
                relative = Path(member.name)
                if relative.is_absolute() or ".." in relative.parts or not relative.parts or relative.parts[0] not in scope["export_names"].values():
                    raise PermissionError("Unsafe exported artifact path")
                if member.name in seen or not (member.isfile() or member.isdir()):
                    raise PermissionError("Duplicate, symlink, hardlink, or special export entry")
                seen.add(member.name)
                destination = stage / relative
                count += 1
                if count > scope["workspace_files"] or len(member.name.encode()) > 4096:
                    raise RuntimeError("Export entry/path quota exceeded")
                if member.isdir():
                    destination.mkdir(parents=True, exist_ok=True)
                    continue
                total += member.size
                if total > scope["workspace_bytes"] or count > scope["workspace_files"]:
                    raise RuntimeError("Export data/file quota exceeded")
                destination.parent.mkdir(parents=True, exist_ok=True)
                with archive.extractfile(member) as source, destination.open("xb") as target:
                    shutil.copyfileobj(source, target, length=65536)
                destination.chmod(0o755 if member.mode & 0o111 else 0o644)
        for destination, _ in scope["writable"]:
            target = Path(scope["host_export_targets"][destination])
            source = stage / scope["export_names"][destination]
            source.mkdir(exist_ok=True)
            if target.exists():
                shutil.rmtree(target)
            shutil.copytree(source, target)
    archive_path.unlink()


if __name__ == "__main__":
    raise SystemExit(main())
