"""Display compact OOM evidence from saved Kubernetes state; run no Jobs."""
import argparse
import json
from pathlib import Path

import yaml
from set_memory import read_log


def summarize(folder):
    folder = Path(folder)
    job = yaml.safe_load(read_log(folder / 'job.yaml'))
    pods = json.loads(read_log(folder / 'pods.json'))['items']
    failed = any(c['type'] == 'Failed' and str(c['status']).lower() == 'true'
                 for c in job.get('status', {}).get('conditions', []))
    rows = []
    for pod in pods:
        owners = pod['metadata'].get('ownerReferences', [])
        if not any(o.get('uid') == job['metadata']['uid'] and o.get('kind') == 'Job' for o in owners):
            raise ValueError('Saved Pod does not belong to the saved Job')
        for container in pod.get('status', {}).get('containerStatuses', []):
            if container['name'] != 'worker':
                continue
            terminated = container.get('state', {}).get('terminated', {})
            spec = next(c for c in pod['spec']['containers'] if c['name'] == 'worker')
            rows.append({
                'Job': job['metadata']['name'],
                'Namespace': pod['metadata']['namespace'],
                'Job status': 'Failed' if failed else 'Not confirmed Failed',
                'Pod': pod['metadata']['name'],
                'Pod phase': pod['status'].get('phase', '?'),
                'Container': 'worker',
                'Termination reason': terminated.get('reason', 'Not terminated'),
                'Exit code': terminated.get('exitCode', '?'),
                'Memory request': spec['resources']['requests']['memory'],
                'Memory limit': spec['resources']['limits']['memory'],
                'Finished (UTC)': terminated.get('finishedAt', '?'),
            })
    return rows, failed


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--state-dir', default='report/logs/skills-oom')
    args = p.parse_args()
    rows, failed = summarize(args.state_dir)
    print('OOM experiment (saved Kubernetes state)')
    for row in rows:
        for label, value in row.items():
            print(f'{label:20}: {value}')
        print()
    confirmed = failed and any(row['Termination reason'] == 'OOMKilled' for row in rows)
    print('OOMKilled confirmed: ' + ('YES' if confirmed else 'NO'))
    print('Full evidence: ' + str(Path(args.state_dir) / 'pods.json'))
    return 0 if confirmed else 1


if __name__ == '__main__':
    raise SystemExit(main())
