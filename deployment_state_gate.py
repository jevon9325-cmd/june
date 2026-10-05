"""Read-only affirmative restart gate. Does not stop/restart or change any state.

Use alongside exact HEAD/service/control gates immediately before a restart.
A pass is a snapshot, not authorization to restart later or while trading races.
"""
import json
from pathlib import Path
import subprocess
import requests
import redis
from live_state_integrity import evaluate_restart_gate


def main():
    try:
        pid = subprocess.check_output(['systemctl', 'show', 'june', '-p', 'MainPID', '--value'], text=True).strip()
        env = dict(s.split('=', 1) for s in Path('/proc', pid, 'environ').read_text().split('\0') if '=' in s)
        client = redis.Redis(host=env['REDIS_HOST'], port=int(env.get('REDIS_PORT', 15074)),
                             password=env.get('REDIS_PASSWORD'), socket_timeout=15)
        session = requests.Session()
        base = env['IG_LIVE_BASE_URL'].rstrip('/')
        headers = {'X-IG-API-KEY': env['IG_LIVE_API_KEY'], 'Version': '2', 'Accept': 'application/json'}
        auth = session.post(base+'/session', headers=headers, json={
            'identifier': env['IG_LIVE_USERNAME'], 'password': env['IG_LIVE_PASSWORD'],
            'encryptedPassword': False}, timeout=30)
        auth.raise_for_status()
        if auth.json().get('currentAccountId') != 'HT2Q8':
            raise ValueError('wrong account')
        headers.update({'CST': auth.headers['CST'], 'X-SECURITY-TOKEN': auth.headers['X-SECURITY-TOKEN']})
        def broker(path, field):
            response = session.get(base+path, headers=headers, timeout=30)
            response.raise_for_status()
            return response.json().get(field)
        allowed, local, reason = evaluate_restart_gate(
            lambda: client.get('june_live_state'),
            lambda: broker('/positions', 'positions'),
            lambda: broker('/workingorders', 'workingOrders'))
        print(json.dumps({'restart_gate_passed': allowed, 'local_classification': local.kind, 'reason': reason}))
        return 0 if allowed else 2
    except Exception:
        # Credential-bearing HTTP responses and process environment are never printed.
        print(json.dumps({'restart_gate_passed': False, 'local_classification': 'ERROR', 'reason': 'gate read failed'}))
        return 2


if __name__ == '__main__':
    raise SystemExit(main())
