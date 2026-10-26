#!/usr/bin/env python3
"""Exercise staged HARU -> real HTTP HarnessRouter -> adopted task, with a fake host worker.

No provider CLI is launched. The fixture owns its source ACK, broker, ports and data.
Run with the fork's test dependencies and --haru-source pointing at the paired checkout.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import secrets
import socket
import sys
import tempfile
import threading
import time
import urllib.request


def port():
    with socket.socket() as sock:
        sock.bind(('127.0.0.1', 0))
        return sock.getsockname()[1]


def http(base, method, path, body=None, headers=None):
    req = urllib.request.Request(base+path, json.dumps(body).encode() if body is not None else None,
                                 {'Content-Type':'application/json', **(headers or {})}, method=method)
    with urllib.request.urlopen(req, timeout=15) as response:
        return json.load(response)


def main():
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--haru-source',required=True)
    args=parser.parse_args();repo=Path(__file__).resolve().parents[1]
    root=Path(tempfile.mkdtemp(prefix='haru-route-contract-'));root.chmod(0o700)
    gp,rp,mp=port(),port(),port();gateway=f'http://127.0.0.1:{gp}';mcp=f'http://127.0.0.1:{mp}/mcp'
    internal,owner_token,worker_token,router_token=[secrets.token_hex(24) for _ in range(4)]
    os.environ.update(HR_BACKING='local',HR_DATA_DIR=str(root/'hr-data'),HARNESS_WORKSPACE=str(root/'workspaces'),
                      HR_WS_MARKER_DIR=str(root/'markers'),HR_ENV_ROOT=str(root/'environments'),
                      HR_IDENTITY_MODE='off',HR_CREDIT_GATE='off',HR_POOL_AUTH='none',
                      POOL_MGMT_ENDPOINT=f'http://127.0.0.1:{rp}',HR_SANDBOX_TRUST='owner',
                      HARNESS_INTERNAL_KEY=internal,HR_SESSION_UIDS='0',HR_SANDBOX_PER_SESSION='0',
                      HR_BROKER_IMAGES='0',HR_BUILTIN_SKILLS_DIR=str(root/'no-skills'),
                      ROUTE_TEST_OWNER=owner_token,ROUTE_TEST_WORKER=worker_token,ROUTE_TEST_ROUTER=router_token)
    sys.path[:0]=[str(repo/'gateway'),str(repo/'runner'),str(Path(args.haru_source).resolve())]
    import uvicorn
    import app as gw
    import server as runner
    from haru_dispatch.bridge import serve
    from haru_dispatch.config import load_config
    from haru_dispatch.host_worker import MCPClient
    from haru_dispatch.session_api import SessionDispatchAPI
    from haru_dispatch.router_pump import pump_once
    config={
      'dispatch':{'enabled':True,'workspaces':['fixture'],'submitters':['haru'],'workers':['worker'],'max_pending':5,'lease_seconds':90},
      'session_dispatch':{'enabled':True,'origins':{'grok-test':{'principal':'haru','adapter':'controlled-fixture','session_id':'original-source'}}},
      'bridge':{'tokens':{'haru':'ROUTE_TEST_OWNER','worker':'ROUTE_TEST_WORKER','router':'ROUTE_TEST_ROUTER'}},
      'router':{'enabled':True,'origins':['grok-test'],'principals':['router'],'gateway_url':gateway,
                'api_key_env':'ROUTE_TEST_API_KEY','local_gateway_hosts':['127.0.0.1'],
                'harnesses':{'fixture':'chrn_placeholder'},'timeout_seconds':30,'retry_seconds':1}}
    cpath=root/'config.json';cpath.write_text(json.dumps(config));cpath.chmod(0o600)
    config=load_config(root/'broker',cpath)
    broker=serve(root/'broker',config,host='127.0.0.1',port=mp)
    bt=threading.Thread(target=broker.serve_forever,daemon=True);bt.start()
    services=[];threads=[]
    for app,p in [(runner.app,rp),(gw.app,gp)]:
        svc=uvicorn.Server(uvicorn.Config(app,host='127.0.0.1',port=p,log_level='error',access_log=False))
        thread=threading.Thread(target=svc.run,daemon=True);thread.start();services.append(svc);threads.append(thread)
    try:
        for _ in range(100):
            if all(s.started for s in services):break
            time.sleep(.05)
        else:raise RuntimeError('fixture server startup failed')
        admin={'x-harness-internal':internal,'x-harness-org':'local','x-harness-member':'route-test'}
        ws=http(gateway,'POST','/v1/hr/workspaces',{'name':'Grok route fixture'},admin)
        wsid=ws.get('id') or ws.get('workspace',{}).get('id');assert wsid, list(ws)
        scoped={**admin,'x-harness-workspace':wsid}
        http(gateway,'PUT','/v1/admin/integrations',{'integrations':[{'name':'route-fixture','provider':'local_subscription',
             'config':{'base_url':mcp,'api_key':router_token},'models':['haru-auto','haru-codex','haru-claude']}]},admin)
        h=http(gateway,'POST','/v1/harnesses',{'name':'Grok fixture','base':'herdr','default_model':'haru-codex',
             'env':{'HARU_WORKSPACE':'fixture'},'system_prompt':'Original Grok task authority remains on HARU.','timeout_seconds':120},scoped)
        hid=h['id'];config['router']['harnesses']['fixture']=hid
        minted=http(gateway,'POST','/v1/orgs/local/keys',{'name':'route-fixture','member_id':'route-test','workspace':wsid},admin)
        key=minted['key']
        os.environ.update(HR_HARU_ROUTE_API_KEY_SHA256=hashlib.sha256(key.encode()).hexdigest(),
                          HR_HARU_ROUTE_WORKSPACE=wsid,HR_HARU_ROUTE_HARNESSES=hid,ROUTE_TEST_API_KEY=key)
        owner=MCPClient(mcp,owner_token);worker=MCPClient(mcp,worker_token)
        submitted=owner.call('submit_session_task',{'task':'Preserve this original request and wait for a fixture answer.',
             'request_id':'route-proof-1','workspace':'fixture','mode':'analysis','requested_provider':'codex',
             'complexity_hint':'standard','required_capabilities':['read'],'result_contract':'response_only'})['task']
        tid=submitted['id'];assert submitted['status']=='waiting_route',submitted
        assert worker.call('claim_session_task',{})['task'] is None
        pumped=pump_once(root/'broker',config);assert pumped['submitted']==1,pumped
        task=owner.call('get_task',{'task_id':tid})['task'];rid=task['route']['response_id']
        assert rid
        claim=None
        for _ in range(100):
            claim=worker.call('claim_session_task',{})['task']
            if claim:break
            time.sleep(.1)
        assert claim and claim['id']==tid, 'host task did not activate'
        assert claim['task']=='Preserve this original request and wait for a fixture answer.'
        assert claim['result_contract']=='response_only'
        lease=claim['lease_id']
        def report(event,kind,body):
            return worker.call('report_session_event',{'task_id':tid,'lease_id':lease,'event_id':event,'kind':kind,'body':body})
        report('started','started',{'provider':'codex'})
        question=report('question','question',{'question':'Confirm this fixture can finish.','evidence':['fixture only']})
        time.sleep(1.5)
        api_headers={'Authorization':'Bearer '+key}
        pending=http(gateway,'GET','/v1/responses/'+rid,headers=api_headers)
        assert pending['status'] in {'queued','in_progress','running'},pending['status']
        adapter=SessionDispatchAPI(root/'broker','fixture-delivery',config,trusted_delivery_adapter=True)
        qc=adapter.delivery_context(question['message_id'])
        owner.call('reply_session_task',{'task_id':tid,'question_message_id':question['message_id'],
             'request_id':'reply-1','text':'Confirmed','reply_capability':qc['reply_capability']})
        report('input','input_consumed',{'request_id':'reply-1'})
        result=report('result','result',{'status':'succeeded','result':'ROUTE_INTEGRATION_OK','evidence':['fake worker transport verification']})
        rc=adapter.delivery_context(result['message_id'])
        owner.call('ack_task_return',{'task_id':tid,'message_id':result['message_id'],'ack_capability':rc['ack_capability']})
        response=None
        for _ in range(100):
            response=http(gateway,'GET','/v1/responses/'+rid,headers=api_headers)
            if response['status'] not in {'queued','in_progress','running'}:break
            time.sleep(.1)
        assert response['status']=='completed',response
        assert 'ROUTE_INTEGRATION_OK' in json.dumps(response)
        final=owner.call('get_task',{'task_id':tid})['task'];assert final['return_state']=='handled'
        assert len(owner.call('list_tasks',{})['tasks'])==1
        assert owner.call('submit_session_task',{'task':'Preserve this original request and wait for a fixture answer.',
             'request_id':'route-proof-1','workspace':'fixture','mode':'analysis','requested_provider':'codex',
             'complexity_hint':'standard','required_capabilities':['read'],'result_contract':'response_only'})['task']['id']==tid
        # A revoked pinned key must not borrow an internal/BFF identity on replay.
        http(gateway,'DELETE','/v1/orgs/local/keys/'+minted['id'],headers=admin)
        rejected=False
        try:
            http(gateway,'POST','/v1/responses',{'model':'haru-codex','input':'x','background':True,'metadata':{'harness_id':hid}},
                 {**scoped,'Authorization':'Bearer '+key,'Idempotency-Key':'haru-route-'+tid,
                  'X-Haru-Route-Task':tid,'X-Haru-Route-Fingerprint':'a'*64,'X-Haru-Route-Capability':'route_'+'b'*64})
        except urllib.error.HTTPError as exc:
            rejected=exc.code==403
        assert rejected,'revoked key reused trusted internal identity'
        from haru_dispatch.session_api import RouteStore
        store=RouteStore(root/'broker',config)
        with store.api._connect() as con: activation=store.api._route_capability(store.api._route(con,tid))
        for tree in [root/'workspaces',root/'hr-data'/'blobs']:
            if tree.exists():
                for file in tree.rglob('*'):
                    if file.is_file() and not file.is_symlink() and file.stat().st_size<4*1024*1024:
                        raw=file.read_bytes()
                        assert activation.encode() not in raw and router_token.encode() not in raw, 'private route material leaked to artifact'
        output=json.dumps(response)
        for secret in [owner_token,worker_token,router_token,key,qc['reply_capability'],rc['ack_capability']]:assert secret not in output
        receipt={'passed':True,'root':str(root),'task_id':tid,'response_id':rid,'workspace':wsid,
                 'same_task_count':1,'waiting_input_preserved':True,'source_ack':'controlled-fixture',
                 'real_model_execution':False,'revoked_key_rejected':True,'private_material_not_in_artifacts':True,'response_status':response['status']}
        (root/'receipt.json').write_text(json.dumps(receipt,indent=2));print(json.dumps(receipt),flush=True)
    finally:
        for rec in list(runner._turns.values()):
            proc=rec.get('proc')
            if proc is not None and proc.poll() is None:
                runner._stop_proc(proc,rec)
        broker.shutdown();broker.server_close();bt.join(timeout=3)
        for svc in services:svc.should_exit=True
        for thread in threads:thread.join(timeout=5)


if __name__=='__main__':main()
