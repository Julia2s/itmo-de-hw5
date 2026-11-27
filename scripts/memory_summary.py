"""Print a screenshot-sized table from existing memory logs; run no Jobs."""
import argparse
import json
import math
from pathlib import Path

import yaml
from set_memory import measured_peaks, read_log


def summarize(paths):
    rows = []
    for path in paths:
        path = Path(path)
        log = read_log(path)
        peak = measured_peaks([log])[0]
        records = [json.loads(line.split('MEMORY_METRICS=', 1)[1])
                   for line in log.splitlines() if 'MEMORY_METRICS=' in line]
        if len(records) != 1:
            raise ValueError(f'Expected one MEMORY_METRICS record in {path}')
        metrics = records[0]
        limit = metrics.get('memory_limit')
        limit_mib = f'{int(limit) / 1024**2:.0f}' if str(limit).isdigit() else str(limit or '-')
        state_path = path.parent / path.stem / 'job.yaml'
        if (path.parent / 'job.yaml').exists():
            state_path = path.parent / 'job.yaml'
        run_name = path.stem
        status = 'Worker OK'
        if state_path.exists():
            job = yaml.safe_load(state_path.read_text(encoding='utf-8-sig'))
            run_name = job['metadata']['name']
            conditions = job.get('status', {}).get('conditions', [])
            if any(c['type'] == 'Failed' and str(c['status']).lower() == 'true' for c in conditions):
                raise ValueError(f'Job {path.stem} failed; see {state_path}')
            if any(c['type'] == 'Complete' and str(c['status']).lower() == 'true' for c in conditions):
                status = 'Complete'
        rows.append([run_name, status, f'{peak:.2f}', limit_mib, metrics.get('source', '?')])
    return rows


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--log', nargs='+', required=True)
    p.add_argument('--margin', type=float, default=1.30)
    args = p.parse_args()
    if not math.isfinite(args.margin) or args.margin < 1:
        p.error('Margin must be finite and >= 1')
    rows = summarize(args.log)
    headers = ['Run', 'Status', 'Peak MiB', 'Limit MiB', 'Metric source']
    widths = [max(len(row[i]) for row in [headers] + rows) for i in range(len(headers))]
    def show(row):
        print('  '.join(value.ljust(width) for value, width in zip(row, widths)))
    print('Baseline memory measurements (existing logs)')
    show(headers)
    print('  '.join('-' * width for width in widths))
    for row in rows:
        show(row)
    # Calculate from the unrounded log values, not displayed table cells.
    peak = max(measured_peaks([read_log(path) for path in args.log]))
    limit = math.ceil(peak * args.margin / 16) * 16
    print(f'\nMax peak: {peak:.6f} MiB')
    print(f'Proposed limit: ceil({peak:.6f} * {args.margin} / 16) * 16 = {limit} MiB')
    print('Full evidence: report/logs/<run>.log and report/logs/<run>/job.yaml')


if __name__ == '__main__':
    main()
