import json
from maestro.storage import ArtifactStore
from maestro.storage.projections import publish_views


def test_nested_verified_summary_projection_drops_private_fields_and_unverified_records(tmp_path):
    artifacts=ArtifactStore(tmp_path/'cas')
    champion=artifacts.put_json({'solver':'public champion'})
    board={'version':1,'champion_hash':champion,'champion_artifact':champion,
        'results':[{'experiment_id':'verified','score':.7,'valid':True,'verified':True,
                    'private_plan':{'bid':88},'raw_stdout':'private debug material'},
                   {'experiment_id':'unverified','verified':False,'score':1}]}
    publish_views(tmp_path,'episode',0,{},board,artifacts)
    result=json.loads((tmp_path/'arms'/'episode'/'public'/'board-0'/'summaries.json').read_text())
    assert result['results']==[{'experiment_id':'verified','score':.7,'valid':True,'verified':True}]
