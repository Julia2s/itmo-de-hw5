"""Compact preemption evidence with victim/event/urgent Pod UID validation."""
import argparse
import json
import subprocess
import textwrap
from pathlib import Path

import yaml
from set_memory import read_log


def evidence(root):
    job = yaml.safe_load(read_log(root / 'low-map/job.yaml'))
    for path in sorted(root.glob('victim-*.json')):
        victim = json.loads(read_log(path))
        if not any(o.get('uid') == job['metadata']['uid'] for o in victim['metadata'].get('ownerReferences', [])):
            continue
        condition = next((c for c in victim['status'].get('conditions', [])
                          if c['type'] == 'DisruptionTarget' and c.get('reason') == 'PreemptionByScheduler'
                          and c.get('status') == 'True'), None)
        event_path = root / f"preempted-{victim['metadata']['name']}.json"
        if condition and event_path.exists():
            event = json.loads(read_log(event_path))
            if event.get('reason') == 'Preempted' and event['involvedObject']['uid'] == victim['metadata']['uid']:
                return victim, event, condition
    raise ValueError('A matching victim Pod with scheduler preemption condition and Preempted event is required')


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--context', default='hw5')
    p.add_argument('--namespace', default='hw5')
    p.add_argument('--checkpoint', default='report/logs/preemption-checkpoint.json')
    p.add_argument('--capture', action='store_true', help='Capture the urgent Pod state using read-only kubectl')
    args = p.parse_args()
    root = Path('report/logs')
    checkpoint = Path(args.checkpoint)
    if args.capture or not checkpoint.exists():
        victim, event, condition = evidence(root)
        cmd = ['kubectl', '--context', args.context, '-n', args.namespace,
               'get', 'pods', '-l', 'job-name=urgent', '-o', 'json']
        pods = json.loads(subprocess.check_output(cmd, encoding='utf-8', timeout=30))['items']
        urgent = next((pod for pod in pods if pod['metadata']['uid'] in event['message']), None)
        if not urgent:
            raise ValueError('The urgent Pod UID does not match the Preempted event')
        snapshot = {'victim': victim, 'event': event, 'condition': condition, 'urgent': urgent}
        checkpoint.parent.mkdir(parents=True, exist_ok=True)
        checkpoint.write_text(json.dumps(snapshot, ensure_ascii=False, indent=2), encoding='utf-8')
    else:
        snapshot = json.loads(read_log(checkpoint))
    victim, urgent = snapshot['victim'], snapshot['urgent']
    event, condition = snapshot['event'], snapshot['condition']
    if urgent['spec']['priority'] <= victim['spec']['priority']:
        raise ValueError('Urgent Pod must have a higher numeric priority')
    print('Priority preemption (saved Kubernetes evidence)')
    print(f"{'Role':7} {'Pod':25} {'PriorityClass':16} {'Priority':9} {'CPU request':12}")
    for role, pod in [('Low', victim), ('Urgent', urgent)]:
        worker = next(c for c in pod['spec']['containers'] if c['name'] == 'worker')
        print(f"{role:7} {pod['metadata']['name']:25} {pod['spec']['priorityClassName']:16} "
              f"{pod['spec']['priority']:<9} {worker['resources']['requests']['cpu']:12}")
    print(f"\nVictim: {victim['metadata']['name']}")
    print(f"Condition: {condition['type']} / {condition['reason']} = {condition['status']}")
    print(f"Event: {event['reason']} at {event.get('lastTimestamp') or event.get('eventTime')}")
    print(textwrap.fill(event['message'], width=90))
    print(f"Urgent Pod phase (snapshot): {urgent['status']['phase']}")
    print('Victim and urgent Pod UIDs match the event: YES')
    print('\nPriority preemption confirmed: YES')
    print(f'Full evidence: {checkpoint}')


if __name__ == '__main__':
    main()
