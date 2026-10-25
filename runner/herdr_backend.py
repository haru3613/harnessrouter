"""HARU fork: read-only subscription Runner boundary; native authority stays on Mac."""
from __future__ import annotations
import hashlib
import json
import math
import os
from pathlib import Path
import re
import sys
import tempfile

PROVIDERS = {'local_subscription'}
MODELS = ('haru-auto', 'haru-codex', 'haru-claude')
DEFAULT_MODEL = 'haru-auto'
_ALIAS = re.compile(r'^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$')


def validate_request(req) -> None:
    """Reject features the remote host cannot enforce before staging or spawning."""
    for name in ('files', 'mcp_servers', 'skills', 'plugins', 'tools_disabled',
                 'environment', 'environment_id', 'image_auth', 'vision_auth'):
        if getattr(req, name, None):
            raise ValueError(f'Herdr does not yet support {name}; no remote task was submitted')
    env = getattr(req, 'env', None) or {}
    if not isinstance(env, dict) or set(env) - {'HARU_WORKSPACE'}:
        raise ValueError('Herdr accepts only HARU_WORKSPACE in harness environment settings')


def build_command(provider, auth, model, prompt, cwd, env, *, workspace, request_key,
                  resume_session_id=None, agent_doc='', timeout_seconds=600):
    from herdr_driver import validate_endpoint
    if provider != 'local_subscription':
        raise ValueError('Herdr requires local_subscription; API provider fallback is forbidden')
    if model not in MODELS:
        raise ValueError('Unknown Herdr subscription selector')
    if not isinstance(workspace,str) or not _ALIAS.fullmatch(workspace):
        raise ValueError('HARU_WORKSPACE must be a registered workspace alias, never a path')
    token = getattr(auth, 'api_key', None)
    if not isinstance(token,str) or not token.strip():
        raise ValueError('A scoped Dispatch access token is required')
    endpoint = validate_endpoint(getattr(auth, 'base_url', '') or '')
    if not isinstance(request_key,str) or not request_key:
        raise ValueError('A stable request key is required')
    if not isinstance(timeout_seconds,(int,float)) or not math.isfinite(timeout_seconds) or not 1 <= timeout_seconds <= 21600:
        raise ValueError('Herdr timeout must be between 1 and 21600 seconds')
    root=Path(cwd).resolve()
    for folder in (root/'.harness', root/'.harness'/'herdr'):
        if folder.is_symlink():
            raise ValueError('Herdr private state must not be a symlink')
        folder.mkdir(exist_ok=True, mode=0o700)
        folder.chmod(0o700)
    jobs=root/'.harness'/'herdr'/'jobs'
    if not jobs.resolve().is_relative_to(root):
        raise ValueError('Herdr job directory escapes the session workspace')
    jobs.mkdir(parents=True,exist_ok=True,mode=0o700)
    request_id='hr-'+hashlib.sha256(request_key.encode()).hexdigest()[:48]
    job={'cwd':str(root),'prompt':prompt,'agent_doc':agent_doc or '', 'model':model,
         'workspace':workspace,'base_url':endpoint,'request_id':request_id,
         'resume_session_id':resume_session_id or '', 'timeout_seconds':timeout_seconds}
    path=jobs/(request_id+'.json');encoded=json.dumps(job,sort_keys=True,ensure_ascii=False)
    if path.exists():
        if path.is_symlink() or path.read_text()!=encoded:
            raise ValueError('Request key belongs to a different Herdr job')
    else:
        fd,tmp=tempfile.mkstemp(prefix='.job-',dir=jobs)
        try:
            with os.fdopen(fd,'w') as f:
                f.write(encoded);f.flush();os.fsync(f.fileno())
            try:os.link(tmp,path)
            except FileExistsError:
                if path.is_symlink() or path.read_text()!=encoded:
                    raise ValueError('Request key belongs to a different Herdr job')
        finally:os.unlink(tmp)
    env['HR_HERDR_TOKEN']=token
    env['HARU_WORKSPACE']=workspace
    return [sys.executable,str(Path(__file__).with_name('herdr_driver.py')),str(path)]


def normalize(obj: dict, state: dict) -> list[dict]:
    kind=obj.get('m');p=obj.get('p') if isinstance(obj.get('p'),dict) else {}
    if kind=='__hr_init' and p.get('session_id'):
        return [{'type':'system','subtype':'init','session_id':str(p['session_id']), 'model':state.get('model')}]
    if kind=='notice':
        text=str(p.get('text') or '')
        return [{'type':'system','subtype':'notice','message':text,'text':text}] if text else []
    if kind=='text':
        text=str(p.get('text') or '');state['final']=state.get('final','')+text
        return [{'type':'assistant','message':{'content':[{'type':'text','text':text}]}}] if text else []
    if kind=='__hr_result':
        final=str(p.get('final') or state.get('final') or '');reason=str(p.get('reason') or '')
        remote=p.get('remote') if isinstance(p.get('remote'),dict) else {};state['remote']=remote
        event={'type':'result','usage':{},'remote':remote}
        if p.get('status')=='completed' and final:
            state['final']=final;event.update(subtype='success',is_error=False,result=final)
        elif p.get('status')=='incomplete':
            state['final']=final;event.update(subtype='incomplete',is_error=False,result=final or reason,reason=reason)
        else:event.update(subtype='error',is_error=True,result=reason or final or 'Herdr task failed',reason=reason)
        return [event]
    return []


def eof(state: dict, code: int) -> list[dict]:
    return [{'type':'result','subtype':'incomplete','is_error':False,'usage':{},'reason':'remote_outcome_unknown',
             'result':f'Herdr driver exited {code} without a verified result; the host task may still be running. Inspect the same task before retrying.'}]

normalize.eof=eof
