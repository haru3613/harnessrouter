"""HARU fork: only the dedicated subscription route may launch the host driver."""
import json
import pathlib
import sys
import time
import threading
from types import SimpleNamespace

import pytest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
import herdr_backend as hb


def test_builder_keeps_token_out_of_job_and_argv_and_binds_idempotency(tmp_path):
    env = {}
    args = dict(provider='local_subscription', auth=SimpleNamespace(api_key='secret-source-token',base_url='https://dispatch.example/mcp'),
                model='haru-codex',prompt='Read a fact',cwd=str(tmp_path),env=env,
                workspace='fixture',request_key='response-1',agent_doc='Only read',timeout_seconds=60)
    cmd = hb.build_command(**args)
    job = json.loads(pathlib.Path(cmd[-1]).read_text())
    assert env['HR_HERDR_TOKEN']=='secret-source-token'
    assert 'secret-source-token' not in json.dumps(cmd)+json.dumps(job)
    assert job['workspace']=='fixture' and job['model']=='haru-codex'
    assert pathlib.Path(cmd[-1]).stat().st_mode & 0o777 == 0o600
    assert hb.build_command(**args)==cmd
    with pytest.raises(ValueError,match='different'):
        hb.build_command(**{**args,'prompt':'a different operation'})


@pytest.mark.parametrize('provider,model', [('openai','haru-codex'),('anthropic','haru-claude'),('local_subscription','gpt-any')])
def test_no_paid_or_unknown_route(provider,model,tmp_path):
    with pytest.raises(ValueError):
        hb.build_command(provider,SimpleNamespace(api_key='t',base_url='https://x.test/mcp'),model,'x',str(tmp_path),{},workspace='fixture',request_key='r')


def test_workspace_is_an_alias_not_a_host_path(tmp_path):
    with pytest.raises(ValueError,match='workspace'):
        hb.build_command('local_subscription',SimpleNamespace(api_key='t',base_url='https://x.test/mcp'),'haru-auto','x',str(tmp_path),{},workspace='/Users/private',request_key='r')


@pytest.mark.parametrize('alias', ['vault:project-secret','secret:project-secret'])
def test_workspace_references_cannot_reach_a_job_file(tmp_path, alias):
    with pytest.raises(ValueError,match='workspace'):
        hb.build_command('local_subscription',SimpleNamespace(api_key='t',base_url='https://x.test/mcp'),'haru-auto','x',str(tmp_path),{},workspace=alias,request_key='r')
    assert not (tmp_path/'.harness').exists()


def test_a_resolved_secret_annotation_is_rejected_even_when_value_looks_like_an_alias():
    with pytest.raises(ValueError,match='literal'):
        hb.validate_request(SimpleNamespace(env={'HARU_WORKSPACE':'looks-like-an-alias'},env_secret=['HARU_WORKSPACE']))


@pytest.mark.parametrize('field,value',[('files',[{'filename':'x'}]),('mcp_servers',[{}]),('skills',[{}]),('plugins',[{}]),('tools_disabled',['Bash']),('environment_id','e'),('env',{'HOME':'x'})])
def test_unsupported_features_rejected_before_dispatch(field,value):
    req=SimpleNamespace(env={'HARU_WORKSPACE':'fixture'})
    setattr(req,field,value)
    with pytest.raises(ValueError):hb.validate_request(req)


def test_events_distinguish_progress_result_and_unknown_stop():
    state={}
    assert hb.normalize({'m':'__hr_init','p':{'session_id':'haru:one'}},state)[0]['session_id']=='haru:one'
    notice=hb.normalize({'m':'notice','p':{'text':'Host queued'}},state)[0]
    assert notice['type']=='system' and notice['subtype']=='notice'
    result=hb.normalize({'m':'__hr_result','p':{'status':'completed','final':'done','remote':{'return_state':'waiting_origin','resource_state':'cleanup_pending'}}},state)[0]
    assert result['subtype']=='success' and result['result']=='done'
    assert state['remote']['return_state']=='waiting_origin'
    stopped=hb.normalize({'m':'__hr_result','p':{'status':'incomplete','reason':'stop_unknown','final':'Remote stop not verified'}},{})[0]
    assert stopped['subtype']=='incomplete' and not stopped['is_error']
    assert 'may still be running' in hb.normalize.eof({},1)[0]['result']


def test_adoption_grant_never_writes_capability_to_job(tmp_path, monkeypatch):
    import hmac
    import hashlib
    monkeypatch.setenv("HARNESS_INTERNAL_KEY", "internal")
    cap = "capability_123456789"
    payload = {"task_id":"task-1","fingerprint":"a" * 64,"response_id":"resp-1","harness_id":"herdr-1","capability_sha256":hashlib.sha256(cap.encode()).hexdigest()}
    proof = hmac.new(b"internal", json.dumps(payload, sort_keys=True, separators=(",", ":")).encode(), hashlib.sha256).hexdigest()
    env = {"HR_HARU_ROUTE_CAPABILITY":cap, "HR_HARU_ROUTE_MCP_ENDPOINT":"https://x.test/mcp"}
    command = hb.build_command("local_subscription", SimpleNamespace(api_key="ordinary", base_url="https://x.test/mcp"),
                               "haru-codex", "read", str(tmp_path), env, workspace="fixture", request_key="route",
                               haru_route={**payload,"proof":proof})
    job = json.loads(pathlib.Path(command[-1]).read_text())
    assert cap not in json.dumps(job)
    assert job["haru_route"] == payload


def test_runner_registry_and_unsupported_input_refusal(monkeypatch,tmp_path):
    import server as rs
    from fastapi.testclient import TestClient
    assert rs.BACKENDS['herdr']['providers']==['local_subscription']
    assert rs.BACKENDS['herdr']['remote'] is True
    # Upstream defaults this *capability hint* true even on text-only requests;
    # actual input files/image credentials, rather than this hint, are refused.
    hb.validate_request(rs.TurnReq(backend='herdr',prompt='read',env={'HARU_WORKSPACE':'fixture'}))
    monkeypatch.setattr(rs,'_SESSION_UIDS',False)
    monkeypatch.setattr(rs,'WORKSPACE_ROOT',str(tmp_path/'workspaces'))
    with TestClient(rs.app) as client:
        response=client.post('/turn?identifier=herdr-test',json={
            'backend':'herdr','provider':'local_subscription','model':'haru-codex',
            'prompt':'Read a fact','files':[{'filename':'x','content_b64':'eA=='}],
            'env':{'HARU_WORKSPACE':'fixture'}})
    assert response.status_code==400
    assert 'no remote task was submitted' in response.json()['detail']
    assert not (tmp_path/'workspaces').exists()


def test_route_descriptor_is_refused_for_any_non_herdr_runner_backend(monkeypatch,tmp_path):
    import server as rs
    from fastapi.testclient import TestClient
    monkeypatch.setattr(rs,'_SESSION_UIDS',False)
    monkeypatch.setattr(rs,'WORKSPACE_ROOT',str(tmp_path/'workspaces'))
    with TestClient(rs.app) as client:
        response=client.post('/turn?identifier=route-poison',json={
            'backend':'claude','prompt':'x','haru_route':{'task_id':'task-1'},
            'haru_route_capability':'capability_123456789'})
    assert response.status_code == 400
    assert 'only valid for the herdr backend' in response.json()['detail']
    assert not (tmp_path/'workspaces').exists()


def test_pre_spawn_cancel_waits_for_herdr_driver_readiness(monkeypatch,tmp_path):
    import server as rs
    rec={'status':'running','events':[],'result':'','done':False,'started':time.time(),
         'backend':'herdr','cancelled':True}
    monkeypatch.setitem(rs._turns,'herdr-ready-race',rec)
    monkeypatch.setattr(rs,'_REMOTE_STOP_GRACE_S',.2)
    child=("import json,signal,time; "
           "signal.signal(signal.SIGTERM,lambda *_:exit(0)); "
           "print(json.dumps({'m':'__hr_ready','p':{}}),flush=True); time.sleep(5)")
    rs._run_turn_bg('herdr-ready-race',[sys.executable,'-c',child],{},str(tmp_path),
                    lambda obj,state: [],'haru-codex',30)
    assert rec.get('driver_ready') is True
    assert 'cancel_after_driver_ready' not in rec


def test_api_cancel_after_popen_defers_until_herdr_ready(monkeypatch,tmp_path):
    import server as rs
    rec={'status':'running','events':[],'result':'','done':False,'started':time.time(),'backend':'herdr'}
    monkeypatch.setitem(rs._turns,'herdr-api-race',rec)
    monkeypatch.setattr(rs,'_REMOTE_STOP_GRACE_S',.2)
    original_stop=rs._stop_proc; stops=[]
    def counted_stop(proc, record):
        stops.append(proc.pid)
        return original_stop(proc, record)
    monkeypatch.setattr(rs,'_stop_proc',counted_stop)
    child=("import json,signal,time; "
           "signal.signal(signal.SIGTERM,lambda *_:exit(0)); time.sleep(.25); "
           "print(json.dumps({'m':'__hr_ready','p':{}}),flush=True); time.sleep(5)")
    thread=threading.Thread(target=rs._run_turn_bg,args=('herdr-api-race',[sys.executable,'-c',child],{},str(tmp_path),lambda obj,state: [],'haru-codex',30))
    thread.start()
    for _ in range(100):
        if rec.get('proc') is not None: break
        time.sleep(.01)
    assert rec.get('proc') is not None
    proc=rec['proc']
    assert rs.cancel_turn('herdr-api-race')['cancelled'] is True
    assert rs.cancel_turn('herdr-api-race')['cancelled'] is True
    rs._kill_capped(proc,rec)
    for _ in range(100):
        if rec.get('cancel_after_driver_ready'): break
        time.sleep(.01)
    assert rec.get('cancel_after_driver_ready') is True
    thread.join(3)
    assert not thread.is_alive() and rec.get('driver_ready') is True
    # Repeated API cancellation and the watchdog before/after readiness send
    # exactly one SIGTERM to the initialized driver.
    rs._kill_capped(proc,rec)
    assert len(stops) == 1 and rec.get('stop_signal_sent') is True
