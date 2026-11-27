"""Compact storage screenshot: saved SHA-256 evidence and read-only cluster state."""
import argparse
import json
import subprocess
from datetime import datetime, timezone
from pathlib import Path

from set_memory import read_log


def read_state(context, namespace):
    base = ['kubectl', '--context', context, '-n', namespace, 'get']
    def get(*args):
        return json.loads(subprocess.check_output(base + list(args) + ['-o', 'json'],
                                                 encoding='utf-8', timeout=30))
    return {'observed_at': datetime.now(timezone.utc).isoformat(),
            'pods': get('pods', '-l', 'app=rustfs'),
            'pvc': get('pvc', 'rustfs-data'), 'service': get('service', 'rustfs')}


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--context', default='hw5')
    p.add_argument('--namespace', default='hw5')
    p.add_argument('--checksums', default='report/logs/storage-checksums.json')
    p.add_argument('--state-file', default='report/logs/storage-state.json')
    p.add_argument('--saved-state', action='store_true', help='Use an existing snapshot, without kubectl')
    args = p.parse_args()
    transfer = json.loads(read_log(args.checksums))
    originals = json.loads(read_log('reference/copied-files.json'))
    original = next(item for item in originals if item['destination'] == 'data/it_vacancies_2024.csv')
    match = (transfer.get('ok') is True and
             transfer['source_sha256'] == transfer['target_sha256'] == original['sha256'] and
             transfer['bytes'] == original['bytes'])
    if not match:
        raise ValueError('Saved source, target and original input checksums/size do not match')
    state_path = Path(args.state_file)
    if args.saved_state:
        state = json.loads(read_log(state_path))
    else:
        state = read_state(args.context, args.namespace)
        state_path.parent.mkdir(parents=True, exist_ok=True)
        state_path.write_text(json.dumps(state, indent=2), encoding='utf-8')
    pods = state['pods']['items']
    if len(pods) != 1:
        raise ValueError('Expected one RustFS Pod; inspect the rollout before the screenshot')
    pod = pods[0]
    statuses = pod.get('status', {}).get('containerStatuses', [])
    ready = next((s for s in statuses if s['name'] == 'rustfs'), {})
    pvc = state['pvc']
    bound = pvc.get('status', {}).get('phase') == 'Bound'
    rows = {
        'Snapshot (UTC)': state['observed_at'],
        'Namespace': pod['metadata']['namespace'],
        'RustFS Pod': pod['metadata']['name'],
        'Pod phase / Ready': f"{pod['status'].get('phase', '?')} / {ready.get('ready', False)}",
        'Container restarts': ready.get('restartCount', '?'),
        'PVC / Status': f"{pvc['metadata']['name']} / {pvc.get('status', {}).get('phase', '?')}",
        'Service': state['service']['metadata']['name'],
        'Source S3': transfer['source_endpoint'],
        'Target S3 (forward)': transfer['target_endpoint'],
        'Object': transfer['key'],
        'Size (bytes)': transfer['bytes'],
        'SHA-256': transfer['target_sha256'],
        'Source = target': 'YES',
        'Matches HW3 input': 'YES',
    }
    print('S3 migration: saved checksums + Kubernetes snapshot')
    for label, value in rows.items():
        print(f'{label:20}: {value}')
    ok = bound and pod['status'].get('phase') == 'Running' and ready.get('ready') is True
    print('\nStorage verification: ' + ('PASS' if ok else 'NOT READY'))
    print('Full evidence: ' + args.checksums + ' and ' + args.state_file)
    return 0 if ok else 1


if __name__ == '__main__':
    raise SystemExit(main())
