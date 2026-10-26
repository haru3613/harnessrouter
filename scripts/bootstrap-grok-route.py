#!/usr/bin/env python3
"""Configure an existing local HarnessRouter container for Grok routing, without enabling HARU.

Credentials remain in private runtime files. The operator separately deploys the
paired broker and applies the generated proposal only after verification.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess


BOOT = r'''
import json,os,sys,urllib.request
x=json.load(sys.stdin)
base='http://127.0.0.1:8080'
admin={'x-harness-internal':os.environ['HARNESS_INTERNAL_KEY'],'x-harness-org':'local','x-harness-member':'local@localhost','Content-Type':'application/json'}
def call(method,path,body=None,headers=None):
 req=urllib.request.Request(base+path,json.dumps(body).encode() if body is not None else None,headers or admin,method=method)
 try:
  with urllib.request.urlopen(req,timeout=15) as r:return json.load(r)
 except urllib.error.HTTPError as e:raise RuntimeError('bootstrap HTTP '+str(e.code)+' at '+path) from None
wsid=x.get('workspace')
if not wsid:
 created=call('POST','/v1/hr/workspaces',{'name':'Grok Dispatch'})
 wsid=created.get('id') or created.get('workspace',{}).get('id')
 if not wsid:raise RuntimeError('workspace id absent')
scoped={**admin,'x-harness-workspace':wsid}
current=call('GET','/v1/admin/integrations')
rows=current.get('integrations',[])
name='haru-grok-router'
if any(i.get('name')==name for i in rows) and not x.get('workspace'):
 raise RuntimeError('named router integration already exists without local ownership receipt')
rows=[i for i in rows if i.get('name')!=name]
rows.append({'name':name,'provider':'local_subscription','config':{'base_url':x['endpoint'],'api_key':x['router_token']},'models':['haru-auto','haru-codex','haru-claude']})
call('PUT','/v1/admin/integrations',{'integrations':rows})
harnesses=dict(x.get('harnesses') or {})
for alias in x['aliases']:
 if alias in harnesses:continue
 h=call('POST','/v1/harnesses',{'name':'Grok · '+alias,'base':'herdr','default_model':'haru-auto','env':{'HARU_WORKSPACE':alias},'timeout_seconds':21600,'system_prompt':'Observe the existing Grok-authorized HARU task. Host policy and original source remain authoritative.','source':'haru-grok-route'},scoped)
 harnesses[alias]=h['id']
key=x.get('api_key')
keyid=x.get('api_key_id')
if not key:
 k=call('POST','/v1/orgs/local/keys',{'name':'Grok route pump','member_id':'local@localhost','workspace':wsid})
 key,keyid=k['key'],k['id']
print(json.dumps({'workspace':wsid,'harnesses':harnesses,'api_key':key,'api_key_id':keyid}))
'''


def private_write(path, value):
    tmp=path.with_suffix(path.suffix+'.tmp');tmp.write_text(value);tmp.chmod(0o600);os.replace(tmp,path)


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--container',required=True);p.add_argument('--runtime-dir',required=True)
    p.add_argument('--haru-runtime-dir',required=True);p.add_argument('--mcp-endpoint',required=True)
    p.add_argument('--alias',action='append',default=[])
    a=p.parse_args();runtime=Path(a.runtime_dir).expanduser().resolve();haru=Path(a.haru_runtime_dir).expanduser().resolve()
    runtime.mkdir(mode=0o700,parents=True,exist_ok=True)
    envfile=runtime/'runtime.env';assert envfile.is_file() and envfile.stat().st_mode & 0o077==0
    tokenfile=haru/'router-token';assert tokenfile.is_file() and tokenfile.stat().st_mode & 0o077==0
    previous=runtime/'grok-routing.json'
    owned=json.loads(previous.read_text()) if previous.exists() else {}
    config=json.loads((haru/'config.json').read_text())
    aliases=list(dict.fromkeys(config['dispatch']['workspaces']+a.alias))
    payload={'endpoint':a.mcp_endpoint,'router_token':tokenfile.read_text().strip(),'aliases':aliases,
             'workspace':owned.get('workspace'),'harnesses':owned.get('harnesses',{}),'api_key_id':owned.get('api_key_id')}
    keyfile=haru/'router-api-key'
    if owned and keyfile.exists():payload['api_key']=keyfile.read_text().strip()
    proc=subprocess.run(['docker','exec','-i',a.container,'python','-c',BOOT],input=json.dumps(payload),text=True,capture_output=True)
    if proc.returncode:
        raise RuntimeError('container bootstrap failed; no credential output displayed')
    created=json.loads(proc.stdout)
    key=created.pop('api_key');private_write(keyfile,key+'\n')
    created.update(endpoint=a.mcp_endpoint,aliases=aliases,container=a.container)
    private_write(previous,json.dumps(created,ensure_ascii=False,indent=2)+'\n')
    env={}
    for line in envfile.read_text().splitlines():
        if line and not line.startswith('#') and '=' in line:
            k,v=line.split('=',1);env[k]=v
    env.update(HARU_ROUTE_API_KEY_SHA256=hashlib.sha256(key.encode()).hexdigest(),
               HARU_ROUTE_WORKSPACE=created['workspace'],HARU_ROUTE_HARNESSES=','.join(created['harnesses'].values()),
               HARU_ROUTE_INTEGRATION='haru-grok-router',HARU_ROUTE_MCP_ENDPOINT=a.mcp_endpoint)
    private_write(envfile,''.join(k+'='+v+'\n' for k,v in env.items()))
    proposal={'enabled':False,'origins':['haru-main'],'principals':['harnessrouter'],
              'gateway_url':'http://harnessrouter:3000/api/harness','local_gateway_hosts':['harnessrouter'],
              'api_key_env':'HARU_ROUTER_API_KEY','harnesses':created['harnesses'],'timeout_seconds':30,'retry_seconds':5}
    private_write(runtime/'haru-router-proposal.json',json.dumps(proposal,indent=2)+'\n')
    print(json.dumps({'workspace':created['workspace'],'harnesses':created['harnesses'],
                      'routing_enabled':False,'runtime_config':str(envfile),'proposal':str(runtime/'haru-router-proposal.json')},ensure_ascii=False))


if __name__=='__main__':main()
