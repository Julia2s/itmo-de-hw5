"""Align minikube allocatable with its Docker limits; dry-run unless --apply."""
import argparse
import base64
import json
import math
import subprocess
import time
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

import yaml
from prepare_scheduling import quantity


def aligned_config(config, node, docker):
    # Capacity may expose all WSL VM resources instead of the outer Docker quota.
    capacity = node['status']['capacity']
    result = dict(config)
    system = dict(config.get('systemReserved', {}))
    kube = config.get('kubeReserved', {})
    limits = {}
    nano = docker.get('NanoCpus', 0)
    if nano:
        limits['cpu'] = Decimal(nano) / Decimal(10**9)
    elif docker.get('CpuQuota', 0) > 0:
        limits['cpu'] = Decimal(docker['CpuQuota']) / Decimal(docker['CpuPeriod'])
    if docker.get('Memory', 0) > 0:
        limits['memory'] = Decimal(docker['Memory'])
    if not limits:
        raise ValueError('No finite Docker resource limits detected; automatic alignment is not applicable')
    for resource, limit in limits.items():
        reserve = max(quantity(system.get(resource, '0')),
                      quantity(capacity[resource]) - limit - quantity(kube.get(resource, '0')), Decimal(0))
        if reserve > 0:
            system[resource] = f'{math.ceil(reserve * 1000)}m' if resource == 'cpu' else str(math.ceil(reserve))
    result['systemReserved'] = system
    return result, limits


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--profile', default='hw5')
    p.add_argument('--context', default='hw5')
    p.add_argument('--output', default='report/logs/node-resources')
    p.add_argument('--apply', action='store_true', help='Back up config, update reservations and restart kubelet')
    args = p.parse_args()
    def run(command):
        return subprocess.check_output(command, encoding='utf-8', timeout=60)
    base = ['kubectl', '--context', args.context]
    nodes = json.loads(run(base + ['get', 'nodes', '-o', 'json']))['items']
    if len(nodes) != 1:
        raise ValueError('This repair supports exactly one local minikube node')
    node = nodes[0]
    if node['metadata']['name'] != args.profile:
        raise ValueError('Node name must match the local Docker minikube profile')
    active = json.loads(run(base + ['get', 'pods', '--all-namespaces', '-o', 'json']))['items']
    if any(p.get('status', {}).get('phase') not in ('Succeeded', 'Failed') and
           p['metadata'].get('labels', {}).get('app') == 'skills-experiment' for p in active):
        raise ValueError('Stop the stage and archive/remove active experimental Jobs before alignment')
    docker = json.loads(run(['docker', 'inspect', args.profile]))[0]['HostConfig']
    original = run(['minikube', '-p', args.profile, 'ssh', '--', 'sudo cat /var/lib/kubelet/config.yaml'])
    config, limits = aligned_config(yaml.safe_load(original), node, docker)
    stamp = datetime.now(timezone.utc).strftime('%Y%m%d-%H%M%S')
    output = Path(args.output) / stamp
    output.mkdir(parents=True, exist_ok=True)
    (output / 'before.yaml').write_text(original, encoding='utf-8')
    proposed = yaml.safe_dump(config, sort_keys=False)
    (output / 'proposed.yaml').write_text(proposed, encoding='utf-8')
    (output / 'node-before.json').write_text(json.dumps(node, indent=2), encoding='utf-8')
    print('Reported capacity:', node['status']['capacity'])
    print('Current allocatable:', node['status']['allocatable'])
    print('Docker limits:', {k: str(v) for k, v in limits.items()})
    print('Proposed systemReserved:', config['systemReserved'])
    print('Config evidence:', output)
    if not args.apply:
        print('Dry-run. Use --apply after stopping the stage and removing failed experiment Jobs.')
        return
    encoded = base64.b64encode(proposed.encode()).decode('ascii')
    remote = (f'sudo cp /var/lib/kubelet/config.yaml /var/lib/kubelet/config.yaml.resources-backup-{stamp} && '
              f"printf %s '{encoded}' | base64 -d | sudo tee /var/lib/kubelet/config.yaml >/dev/null && "
              'sudo systemctl restart kubelet')
    run(['minikube', '-p', args.profile, 'ssh', '--', remote])
    deadline = time.monotonic() + 120
    while time.monotonic() < deadline:
        current = json.loads(run(base + ['get', 'node', args.profile, '-o', 'json']))
        ready = any(c['type'] == 'Ready' and c['status'] == 'True' for c in current['status']['conditions'])
        if ready and all(quantity(current['status']['allocatable'][k]) <= v for k, v in limits.items()):
            (output / 'node-after.json').write_text(json.dumps(current, indent=2), encoding='utf-8')
            print('Ready; corrected allocatable:', current['status']['allocatable'])
            return
        time.sleep(2)
    raise RuntimeError('Node did not report corrected allocatable in 120s; inspect kubelet before continuing')


if __name__ == '__main__':
    main()
