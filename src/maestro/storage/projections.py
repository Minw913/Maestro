"""Explicit privacy-preserving exports, rebuildable from settled state."""
from pathlib import Path
import os
import tempfile
from maestro.types import canonical_json, to_jsonable


def _atomic_json(path,value):
    path=Path(path)
    path.parent.mkdir(parents=True,exist_ok=True)
    fd,name=tempfile.mkstemp(dir=path.parent,prefix='.pending-')
    try:
        with os.fdopen(fd,'w',encoding='utf-8') as stream:
            stream.write(canonical_json(value))
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(name,path)
    finally:
        Path(name).unlink(missing_ok=True)


def publish_views(root,episode_id,round_index,states,board,artifacts):
    base=Path(root)/'arms'/episode_id
    public=base/'public'/f'board-{round_index}'
    # Whitelist, do not expose all event/ScoreBundle fields.
    allowed=('version','round_index','champion_hash','champion_score','champion_artifact','panel_hash','contract_hash','results')
    safe={k:board[k] for k in allowed if k in board}
    result_fields=('experiment_id','candidate_hash','agent_id','score','valid','status','verified')
    safe['results']=[{k:result[k] for k in result_fields if k in result}
        for result in board.get('results',()) if result.get('verified') is True]
    _atomic_json(public/'summaries.json',safe)
    champion=board['champion_hash']
    try:
        artifacts.export(champion,public/'champion'/'artifact.json')
    except Exception:
        # Real source trees are materialized by WorkspaceStore instead of CAS
        # JSON. A reference is explicit and must be resolved before agent launch.
        if artifacts.path(champion).exists():
            raise
        _atomic_json(public/'champion'/'reference.json',{'candidate_hash':champion})
    for a,state in states.items():
        own=base/'private'/a/'memory'
        _atomic_json(own/'experiments'/'history.json',state.history)
        _atomic_json(own/'notes'/'lessons.json',state.lessons)
        _atomic_json(base/'private'/a/'plans'/'pending.json',state.pending_plan)
