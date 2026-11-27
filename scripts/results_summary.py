"""Show compact parallel Job completion, HW3 equality and top-10 pairs."""
import argparse
import json
from pathlib import Path

import yaml
from set_memory import read_log
from verify_results import verify


def completed_job(path):
    job = yaml.safe_load(read_log(path))
    complete = any(c['type'] == 'Complete' and str(c['status']).lower() == 'true'
                   for c in job.get('status', {}).get('conditions', []))
    if not complete:
        raise ValueError(f'Job completion not confirmed in {path}')
    return job


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--run', default='parallel')
    p.add_argument('--map-job')
    p.add_argument('--reduce-job')
    args = p.parse_args()
    mapper_name = args.map_job or ('skills-parallel' if args.run == 'parallel' else f'{args.run}-map')
    reducer_name = args.reduce_job or ('skills-reduce' if args.run == 'parallel' else f'{args.run}-reduce')
    logs = Path('report/logs')
    results = Path('report/results') / args.run
    mapper = completed_job(logs / mapper_name / 'job.yaml')
    reducer = completed_job(logs / reducer_name / 'job.yaml')
    summary = json.loads(read_log(results / 'summary.json'))
    report = verify('reference/hw3/results_v2', results)
    count = mapper['spec']['completions']
    succeeded = mapper['status'].get('succeeded', 0)
    if not report['ok'] or succeeded < count or summary['parts'] != count:
        raise ValueError('Complete partition processing and HW3 equality not confirmed')
    for k in (1, 2, 3):
        name = f'skills_{k}.csv'
        if summary['sha256'][name] != report['checks'][name]['sha256']:
            raise ValueError(f'Result summary/file hash mismatch: {name}')
    print('Parallel calculation: completion and HW3 verification')
    print(f"Map: {mapper['metadata']['name']} Complete {succeeded}/{count}; "
          f"indexes {mapper['status'].get('completedIndexes', '?')}")
    print(f"Reducer: {reducer['metadata']['name']} Complete")
    stats = summary['stats']
    print(f"Input lines: {stats['lines_total']}; matched: {stats['lines_passed_prefilter']}; "
          f"with skills: {stats['lines_with_skills']}\n")
    print('Table          Unique    Total frequency    Full CSV + SHA256')
    for k in (1, 2, 3):
        name = f'skills_{k}.csv'
        check = report['checks'][name]
        print(f"{name:14} {check['rows']:>6}    {check['total_frequency']:>15}    PASS")
    print('\nTop-10 skill pairs (identical to HW3):')
    for combo, frequency in report['checks']['skills_2.csv']['top10']:
        print(f'{combo:28} {frequency:>5}')
    print('\nRESULT: PASS - full tables, SHA-256 and top-10 match HW3')
    print(f'Full evidence: {results}/ and report/logs/{args.run}-verify.json')


if __name__ == '__main__':
    main()
