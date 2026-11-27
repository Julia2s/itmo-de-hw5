"""Compact CPU Pending evidence, matched by Pod UID; no workloads are started."""
import argparse
import json
import shutil
import textwrap
from decimal import Decimal
from pathlib import Path

from prepare_scheduling import quantity
from set_memory import read_log
from results_summary import completed_job


def load_evidence(folder, budget_file):
    pods = json.loads(read_log(folder / 'pods.json'))['items']
    events = json.loads(read_log(folder / 'events.json'))['items']
    running = [p for p in pods if p.get('status', {}).get('phase') == 'Running' and
               any(c.get('state', {}).get('running') for c in p['status'].get('containerStatuses', []))]
    pending = [p for p in pods if p.get('status', {}).get('phase') == 'Pending']
    matched = [(pod, event) for pod in pending for event in events
               if event.get('involvedObject', {}).get('uid') == pod['metadata']['uid']
               and event.get('reason') == 'FailedScheduling'
               and 'Insufficient cpu' in event.get('message', '')]
    if not running or not matched:
        raise ValueError('Saved snapshot must contain a Running worker and a Pending worker with its own Insufficient cpu event')
    budget = dict(line.split('=', 1) for line in read_log(budget_file).splitlines()
                  if '=' in line and ';' not in line)
    return running[0], matched[0][0], matched[0][1], budget


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--checkpoint', default='report/logs/cpu-pending')
    p.add_argument('--capture', action='store_true', help='Capture the current saved cpu-map snapshot')
    p.add_argument('--completed', action='store_true', help='Show the former Pending worker after successful completion')
    args = p.parse_args()
    checkpoint = Path(args.checkpoint)
    if args.capture or not checkpoint.exists():
        source = Path('report/logs/cpu-map')
        budget = Path('k8s/generated/cpu-budget.txt')
        load_evidence(source, budget)  # Refuse to capture an invalid/finished experiment.
        checkpoint.mkdir(parents=True, exist_ok=True)
        for name in ('job.yaml', 'pods.json', 'events.json'):
            shutil.copyfile(source / name, checkpoint / name)
        shutil.copyfile(budget, checkpoint / 'cpu-budget.txt')
    running, pending, event, budget = load_evidence(checkpoint, checkpoint / 'cpu-budget.txt')
    if args.completed:
        mapper = completed_job('report/logs/cpu-map/job.yaml')
        reducer = completed_job('report/logs/cpu-reduce/job.yaml')
        current = json.loads(read_log('report/logs/cpu-map/pods.json'))['items']
        former = next((pod for pod in current if pod['metadata']['uid'] == pending['metadata']['uid']), None)
        if not former or former['status'].get('phase') != 'Succeeded':
            raise ValueError('The previously Pending Pod has not been confirmed Succeeded')
        report = json.loads(read_log('report/logs/cpu-verify.json'))
        if report.get('ok') is not True or not all(
            report.get('checks', {}).get(f'skills_{k}.csv', {}).get('sha256_match') is True for k in (1, 2, 3)):
            raise ValueError('Successful HW3 comparison is not confirmed')
        expected = mapper['spec']['completions']
        succeeded = mapper['status'].get('succeeded', 0)
        if succeeded < expected:
            raise ValueError('Not all indices succeeded')
        print('CPU scheduling: completion after releasing CPU')
        print(f"Map: {mapper['metadata']['name']} Complete {succeeded}/{expected}")
        print(f"Reducer: {reducer['metadata']['name']} Complete")
        print(f"Former Pending Pod: {former['metadata']['name']}")
        print('Same Pod UID: YES; Pending -> Succeeded\n')
        print(f"{'Pod':24} {'Index':6} {'Phase':10} {'Started (UTC)':21} {'Finished (UTC)':21}")
        for pod in sorted(current, key=lambda p: p['metadata']['name']):
            if pod['status'].get('phase') != 'Succeeded':
                continue
            status = next(c for c in pod['status']['containerStatuses'] if c['name'] == 'worker')
            terminated = status['state']['terminated']
            index = pod['metadata']['annotations']['batch.kubernetes.io/job-completion-index']
            print(f"{pod['metadata']['name']:24} {index:6} {pod['status']['phase']:10} "
                  f"{terminated['startedAt']:21} {terminated['finishedAt']:21}")
        print('\nHW3 full CSV + SHA256: PASS 3/3')
        print('Evidence: report/logs/cpu-pending/, cpu-map/, cpu-reduce/, cpu-verify.json')
        return
    workers = [next(c for c in pod['spec']['containers'] if c['name'] == 'worker')
               for pod in (running, pending)]
    required = sum((quantity(c['resources']['requests']['cpu']) for c in workers), Decimal(0))
    free = Decimal(budget['free_cpu_m']) / 1000
    if required <= free or any(quantity(c['resources']['requests']['cpu']) > free for c in workers):
        raise ValueError('CPU budget does not prove that one worker fits and two do not')
    print('CPU scheduling: saved Running + Pending checkpoint')
    print(f"Node: {budget['node']}; allocatable: {budget['allocatable_cpu']} CPU")
    print(f"Background requests: {budget['used_requests_cpu']} CPU; free before map: {free} CPU")
    print(f'Two workers need: {required} CPU > {free} CPU available\n')
    print(f"{'Pod':24} {'Index':6} {'Phase':10} {'CPU request':12} {'CPU limit':10}")
    for pod, worker in zip((running, pending), workers):
        index = pod['metadata'].get('annotations', {}).get('batch.kubernetes.io/job-completion-index', '?')
        resources = worker['resources']
        print(f"{pod['metadata']['name']:24} {index:6} {pod['status']['phase']:10} "
              f"{resources['requests']['cpu']:12} {resources['limits'].get('cpu', '-'):10}")
    print(f"\nEvent for {pending['metadata']['name']}: {event['reason']}")
    print(textwrap.fill(event['message'], width=90))
    print('\nCPU shortage confirmed: YES')
    print(f'Full evidence: {checkpoint}/pods.json, events.json, cpu-budget.txt')


if __name__ == '__main__':
    main()
