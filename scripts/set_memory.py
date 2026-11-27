"""Generate a NEW Job with limit derived from a successful baseline log."""
import argparse
import math
import re
from pathlib import Path

import yaml


def read_log(path):
    raw = Path(path).read_bytes()
    return raw.decode("utf-16" if raw.startswith((b'\xff\xfe', b'\xfe\xff')) else "utf-8-sig")


def measured_peaks(logs):
    peaks = []
    for log in logs:
        if "WORKER_COMPLETE" not in log:
            raise ValueError("Each baseline must have a successful WORKER_COMPLETE marker")
        values = re.findall(r"MEMORY_PEAK_MIB=([0-9]+(?:\.[0-9]+)?)", log)
        if len(values) != 1 or float(values[0]) <= 0:
            raise ValueError("Each log must contain exactly one positive measured peak")
        peaks.append(float(values[0]))
    return peaks


def tune(log, job, name, margin=1.30):
    if "WORKER_COMPLETE" not in log:
        raise ValueError("Successful WORKER_COMPLETE marker missing")
    peaks = [float(v) for v in re.findall(r"MEMORY_PEAK_MIB=([0-9]+(?:\.[0-9]+)?)", log)]
    if not peaks or max(peaks) <= 0 or not math.isfinite(margin) or margin < 1:
        raise ValueError("Positive measured peak and margin >= 1 required")
    if name == job["metadata"]["name"] or not re.fullmatch(r"[a-z0-9](?:[-a-z0-9]*[a-z0-9])?", name) or len(name) > 63:
        raise ValueError("Use a new valid Kubernetes Job name")
    peak = max(peaks)
    limit = math.ceil(peak * margin / 16) * 16
    job["metadata"]["name"] = name
    job["metadata"]["labels"] = {"experiment": "tuned"}
    job["metadata"].setdefault("annotations", {}).update({
        "hw5/peak-mib": str(peak), "hw5/margin": str(margin),
        "hw5/formula": "ceil(peak * margin / 16) * 16 MiB"})
    template = job["spec"]["template"]
    template["metadata"]["labels"] = {"experiment": "tuned"}
    worker = next(c for c in template["spec"]["containers"] if c["name"] == "worker")
    worker["resources"]["limits"]["memory"] = f"{limit}Mi"
    worker["resources"]["requests"]["memory"] = f"{min(128, limit)}Mi"
    args = worker["args"]
    args[args.index("--prefix") + 1] = "hw5/tuned"
    return job, peak, limit


if __name__ == "__main__":
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--log", required=True, nargs='+', help="One or more successful baseline logs")
    p.add_argument("--template", required=True)
    p.add_argument("--output", required=True)
    p.add_argument("--name", required=True)
    p.add_argument("--margin", type=float, default=1.30)
    args = p.parse_args()
    if Path(args.output).resolve() == Path(args.template).resolve():
        p.error("Output must not overwrite the baseline template")
    logs = [read_log(path) for path in args.log]
    peaks = measured_peaks(logs)
    job, peak, limit = tune('\n'.join(logs),
                            yaml.safe_load(Path(args.template).read_text(encoding="utf-8")),
                            args.name, args.margin)
    Path(args.output).parent.mkdir(parents=True, exist_ok=True)
    Path(args.output).write_text(yaml.safe_dump(job, sort_keys=False), encoding="utf-8")
    print(f"peak={peak} MiB; ceil({peak} * {args.margin} / 16) * 16 = {limit} MiB")
    print(f"Successful measurements={len(peaks)}; min={min(peaks)}; max={max(peaks)} MiB")
    print(f"Written {args.output}; no Kubernetes objects have been changed")
