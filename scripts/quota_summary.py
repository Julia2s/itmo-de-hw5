"""Compact completion and released namespace quota evidence, without rerunning Jobs."""
import argparse
import json
import subprocess
from datetime import datetime, timezone
from pathlib import Path

from results_summary import completed_job
from set_memory import read_log
from verify_results import verify


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--context', default='hw5')
    p.add_argument('--namespace', default='hw5-quota')
    p.add_argument('--capture', action='store_true', help='Read current ResourceQuota instead of saved snapshot')
    p.add_argument('--checkpoint', default='report/logs/quota-released.json')
    args = p.parse_args()
    mapper = completed_job('report/logs/quota-map/job.yaml')
    reducer = completed_job('report/logs/quota-reduce/job.yaml')
    total = mapper['spec']['completions']
    if mapper['status'].get('succeeded', 0) < total:
        raise ValueError('Not all quota-map indices succeeded')
    report = verify('reference/hw3/results_v2', 'report/results/quota')
    if not report['ok']:
        raise ValueError('Quota calculation does not match HW3')
    events = json.loads(read_log('report/logs/quota-map/events.json'))['items']
    rejected = [event for event in events
                if event.get('involvedObject', {}).get('uid') == mapper['metadata']['uid']
                and event.get('reason') == 'FailedCreate' and 'exceeded quota' in event.get('message', '')]
    if not rejected:
        raise ValueError('FailedCreate/exceeded quota evidence for this Job is missing')
    checkpoint = Path(args.checkpoint)
    if args.capture or not checkpoint.exists():
        command = ['kubectl', '--context', args.context, '-n', args.namespace,
                   'get', 'resourcequota', 'hw5-quota', '-o', 'json']
        quota = json.loads(subprocess.check_output(command, encoding='utf-8', timeout=30))
        state = {'observed_at': datetime.now(timezone.utc).isoformat(), 'quota': quota}
        checkpoint.parent.mkdir(parents=True, exist_ok=True)
        checkpoint.write_text(json.dumps(state, indent=2), encoding='utf-8')
    else:
        state = json.loads(read_log(checkpoint))
    quota = state['quota']
    used, hard = quota['status']['used'], quota['status']['hard']
    print('Namespace quota: completion and released resources')
    print(f"Snapshot (UTC): {state['observed_at']}")
    print(f"Namespace: {quota['metadata']['namespace']}")
    print(f"Map: {mapper['metadata']['name']} Complete {mapper['status']['succeeded']}/{total}; "
          f"indexes {mapper['status'].get('completedIndexes', '?')}")
    print(f"Reducer: {reducer['metadata']['name']} Complete")
    print('Earlier admission rejection: FailedCreate / exceeded quota confirmed\n')
    print(f"{'Resource':20} {'Used':10} {'Hard':10}")
    for resource in ('requests.cpu', 'requests.memory', 'limits.cpu', 'limits.memory', 'pods'):
        print(f"{resource:20} {used.get(resource, '?'):10} {hard.get(resource, '?'):10}")
    released = all(str(used.get(key)) == '0' for key in ('requests.cpu', 'requests.memory', 'limits.cpu', 'limits.memory', 'pods'))
    print('\nResources released: ' + ('YES' if released else 'NOT FULLY RELEASED'))
    print('Full CSV, SHA-256 and top-10 vs HW3: PASS 3/3')
    print('Evidence: report/logs/quota-map/, quota-reduce/, quota-verify.json, quota-released.json')
    return 0 if released else 1


if __name__ == '__main__':
    raise SystemExit(main())
