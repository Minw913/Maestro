"""Exercise actual pinned Coral classes and its actual subprocess grader worker.

Source-only testing supplies ephemeral distribution metadata required by Coral's
__init__; no fake Coral classes or modified Coral code are used. This is a
library/serialization contract test, not a live model/container qualification.
"""
import json
import os
from pathlib import Path
import subprocess
import sys

import pytest

from maestro.backends.coral import CORAL_REVISION


def checkout():
    explicit = os.environ.get("MAESTRO_CORAL_SOURCE")
    path = Path(explicit) if explicit else Path(__file__).resolve().parents[2] / "vendor/coral"
    if not (path / "coral" / "types.py").is_file():
        pytest.skip("Pinned Coral source not available; initialize vendor/coral or MAESTRO_CORAL_SOURCE")
    commit = subprocess.run(["git", "-C", str(path), "rev-parse", "HEAD"], capture_output=True, text=True, check=True).stdout.strip()
    assert commit == CORAL_REVISION
    return path


def test_real_coral_subprocess_preserves_metadata_and_scorebundle(tmp_path):
    source = checkout()
    metadata = tmp_path / "coral-0.0.0.dist-info"; metadata.mkdir()
    (metadata / "METADATA").write_text("Metadata-Version: 2.1\nName: coral\nVersion: 0.0.0\n")
    (tmp_path / "worker_fixture.py").write_text('''
from coral.grader.task_grader import TaskGrader
from coral.types import ScoreBundle
class MetadataGrader(TaskGrader):
    def evaluate(self):
        assert self.tasks[0].metadata['maestro']['ticket_id']=='bound-ticket'
        assert self.tasks[0].metadata['maestro']['panel_hash']=='frozen-panel'
        return ScoreBundle(scores={},aggregated=.75,is_public=False,metadata=self.tasks[0].metadata)
''')
    script = '''
import asyncio,inspect,json,sys
from pathlib import Path
from coral.config import GraderConfig
from coral.types import Task,ScoreBundle
from coral.grader.subprocess_grader import SubprocessGrader
from coral.grader.task_grader import TaskGrader
from maestro.backends.coral.grading import TicketedGrader
assert issubclass(TicketedGrader,TaskGrader)
task=Task('job','name','description',{'maestro':{'ticket_id':'bound-ticket','panel_hash':'frozen-panel'}})
task=Task.from_dict(json.loads(json.dumps(task.to_dict())))
grader=SubprocessGrader('worker_fixture:MetadataGrader',Path(sys.executable),GraderConfig(timeout=20),sys.argv[1])
bundle=asyncio.run(grader.grade('unused-trusted-codebase',[task],ticket_id='kwargs-do-not-convey-authority'))
assert bundle.aggregated==.75 and bundle.is_public is False
assert bundle.metadata['maestro']['ticket_id']=='bound-ticket'
assert ScoreBundle.from_dict(bundle.to_dict()).aggregated==.75
print(json.dumps({'ticket_id':bundle.metadata['maestro']['ticket_id'],'aggregated':bundle.aggregated,'actual_coral_worker':True}))
'''
    env = dict(os.environ)
    env["PYTHONPATH"] = os.pathsep.join([str(tmp_path), str(source), str(Path(__file__).resolve().parents[2] / "src")])
    result = subprocess.run([sys.executable, "-c", script, str(tmp_path)], env=env,
                            capture_output=True, text=True, timeout=60)
    assert result.returncode == 0, result.stderr + result.stdout
    assert json.loads(result.stdout.strip().splitlines()[-1])["actual_coral_worker"] is True
