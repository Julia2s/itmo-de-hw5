"""Display the tuned limit, three successful runs and HW3 verification."""
import argparse
import json
import math
from pathlib import Path

import yaml
from memory_summary import summarize
from set_memory import measured_peaks, read_log


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--logs-root', default='report/logs')
    p.add_argument('--runs', nargs='+', default=['tuned-1', 'tuned-2', 'tuned-3'])
    p.add_argument('--manifest', default='k8s/job-tuned.yaml')
    args = p.parse_args()
    root = Path(args.logs_root)
    template = yaml.safe_load(read_log(args.manifest))
    annotations = template['metadata']['annotations']
    peak = float(annotations['hw5/peak-mib'])
    margin = float(annotations['hw5/margin'])
    computed_limit = math.ceil(peak * margin / 16) * 16
    worker = next(c for c in template['spec']['template']['spec']['containers'] if c['name'] == 'worker')
    selected_limit = worker['resources']['limits']['memory']
    if selected_limit != f'{computed_limit}Mi':
        raise ValueError('Manifest memory limit differs from its annotated peak and formula')
    rows = []
    for name in args.runs:
        logs = list((root / name).glob('*.log'))
        if len(logs) != 1:
            raise ValueError(f'Expected exactly one saved worker log in {root / name}')
        row = summarize(logs)[0]
        if row[0] != name or row[1] != 'Complete' or row[3] != str(computed_limit):
            raise ValueError(f'{name}: successful Job with the selected memory limit not confirmed')
        report = json.loads(read_log(root / f'{name}-verify.json'))
        checks = [report.get('checks', {}).get(f'skills_{k}.csv', {}) for k in (1, 2, 3)]
        matched = report.get('ok') is True and all(
            c.get('full_match') is True and c.get('sha256_match') is True and c.get('top10_match') is True
            for c in checks)
        if not matched:
            raise ValueError(f'{name}: full HW3 match not confirmed')
        rows.append(row + ['PASS 3/3'])
    print('Tuned memory: successful runs and HW3 comparison')
    print(f'Baseline maximum: {peak:.6f} MiB')
    print(f'Limit: ceil({peak:.6f} * {margin} / 16) * 16 = {computed_limit} MiB\n')
    headers = ['Run', 'Status', 'Peak MiB', 'Limit MiB', 'Metric source', 'HW3 CSV + SHA256']
    widths = [max(len(row[i]) for row in [headers] + rows) for i in range(len(headers))]
    for row in [headers, ['-' * width for width in widths]] + rows:
        print('  '.join(value.ljust(width) for value, width in zip(row, widths)))
    print('\nAll listed Jobs completed; full tables and top-10 match HW3.')
    print('Full evidence: report/logs/tuned-*/ and tuned-*-verify.json')


if __name__ == '__main__':
    main()
