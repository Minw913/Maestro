"""Physical verifier-only qualification on real graphs and diagnostic solutions.

No candidate execution receipts, formal evaluations or market results are created.
Diagnostic solution files can exceed the candidate-output cap; their successful
verification does not qualify those files as candidate outputs.
"""
import argparse
from dataclasses import asdict
import hashlib
import json
from pathlib import Path
import shutil

from maestro.execution.sandbox import DockerRunner, RunRequest, ResourceLimits, Mount
from maestro.tasks.pace.manifest import VERIFIER_SOURCE_FILES, verifier_source_digest


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, required=True)
    parser.add_argument('--panel', type=Path, required=True)
    parser.add_argument('--solutions', type=Path, required=True)
    parser.add_argument('--image', required=True)
    args = parser.parse_args()
    from maestro.storage.paths import diagnostic_root
    root, panel_path = diagnostic_root(args.root), args.panel.resolve()
    root.mkdir(parents=True, exist_ok=False)
    source = Path(__file__).resolve().parents[1] / 'src'
    captured = root / 'trusted-source'
    for name in VERIFIER_SOURCE_FILES:
        target = captured / name
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(source / name, target)
    captured_hash = verifier_source_digest(captured)
    panel = json.loads(panel_path.read_text())
    items, mounts = [], [Mount(captured, '/input/source')]
    for index, item in enumerate(panel['instances']):
        graph_path = (panel_path.parent / item['path']).resolve()
        graph = graph_path.read_bytes()
        assert hashlib.sha256(graph).hexdigest() == item['sha256']
        # These prepared public files start directly with their standard header.
        header = graph.split(b'\n', 1)[0].split()
        assert header[:2] == [b'p', b'ds']
        target = f'/input/graphs/{index}.gr'
        mounts.append(Mount(graph_path, target))
        items.append({**item, 'graph_path': target, 'n': int(header[2]),
            'stdout': (args.solutions / (item['instance_id'] + '.sol')).read_text(),
            'returncode': 0, 'timed_out': False})
    limits = ResourceLimits(cpu_seconds=20, wall_seconds=30, memory_mb=512,
                            pids=8, output_bytes=1000000, cpus=1)
    receipt = DockerRunner(args.image).run(RunRequest(
        ('python3', '-I', '/input/source/maestro/tasks/pace/verification_worker.py'),
        root, stdin=json.dumps({'schema_version': 1, 'instances': items}),
        mounts=tuple(mounts)), limits)
    passed = False
    try:
        response = json.loads(receipt.stdout)
        passed = (receipt.returncode == 0 and not receipt.timed_out and
            len(response['instances']) == 20 and
            all(record['valid'] is True for record in response['instances']))
    except (ValueError, KeyError):
        pass
    result = {'purpose': 'verifier-only real-panel physical acceptance, not formal candidate evaluation',
        'verification_code_hash': captured_hash, 'limits': asdict(limits),
        'receipt': asdict(receipt), 'passed': passed,
        'candidate_output_cap_qualified': False}
    (root / 'result.json').write_text(json.dumps(result, indent=2))
    print(json.dumps(result, indent=2))
    return 0 if passed else 2


if __name__ == '__main__':
    raise SystemExit(main())
