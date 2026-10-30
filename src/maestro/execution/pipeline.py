"""Execution policy versus immutable historical pipeline identification."""
import json
from contextlib import closing
from pathlib import Path
import sqlite3
from maestro.types import MaestroError

PREBID = 'prebid_v2'
RETIRED = 'postbid_v1'


def pipeline_version(config):
    """Missing historical wire fields always mean postbid; never migrate them."""
    coral=config.get('coral',{}) if isinstance(config,dict) else config.coral
    return coral.get('candidate_pipeline',RETIRED)


def require_prebid(config):
    if pipeline_version(config)!=PREBID:
        raise MaestroError('postbid_v1 execution is retired; bootstrap/run/resume require prebid_v2. '
            'Keep the original experiment for replay, reports, figures or frozen-champion final-eval; '
            'do not relabel its checkpoint as prebid_v2.')


def new_pipeline_settings(coral):
    if not isinstance(coral,dict):raise ValueError('coral must be a mapping')
    settings={'candidate_pipeline':PREBID,**coral}
    if settings['candidate_pipeline']!=PREBID:
        raise ValueError('postbid_v1 execution is retired; new configurations require candidate_pipeline: prebid_v2')
    return settings


def read_episode(root,episode):
    path=Path(root).resolve()/'authority/ledger.sqlite'
    if not path.is_file():raise ValueError('Missing experiment ledger: '+str(path))
    with closing(sqlite3.connect(path.as_uri()+'?mode=ro',uri=True)) as db:
        db.row_factory=sqlite3.Row
        row=db.execute('SELECT * FROM episodes WHERE id=?',(episode,)).fetchone()
        if row is None:raise ValueError('Unknown episode')
        return dict(row)


def require_prebid_root(root):
    path=Path(root).resolve()/'authority/ledger.sqlite'
    if not path.is_file():return
    with closing(sqlite3.connect(path.as_uri()+'?mode=ro',uri=True)) as db:
        exists=db.execute("SELECT 1 FROM sqlite_master WHERE name='episodes'").fetchone()
        if exists:
            for raw, in db.execute("SELECT config_json FROM episodes WHERE policy!='heldout'"):
                require_prebid(json.loads(raw))
