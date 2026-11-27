"""Create independent calculation pipelines from the versioned Job templates."""
import copy
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]


def load_template(name):
    return yaml.safe_load((ROOT / "k8s" / name).read_text(encoding="utf-8"))


def worker_of(job):
    return next(c for c in job["spec"]["template"]["spec"]["containers"] if c["name"] == "worker")


def set_argument(worker, flag, value):
    args = worker.setdefault("args", [])
    if flag in args:
        args[args.index(flag) + 1] = str(value)
    else:
        args.extend([flag, str(value)])


def configure(job, name, prefix, namespace="hw5", memory="1Gi", cpu="500m",
              priority=None, hostname=None, experiment=None):
    job = copy.deepcopy(job)
    job["metadata"].update(name=name, namespace=namespace)
    labels = {"app": "skills-experiment", "experiment": experiment or name}
    job["metadata"]["labels"] = labels.copy()
    spec = job["spec"]["template"]
    spec.setdefault("metadata", {})["labels"] = labels.copy()
    spec["spec"]["terminationGracePeriodSeconds"] = 10
    if hostname:
        spec["spec"]["nodeSelector"] = {"kubernetes.io/hostname": hostname}
    if priority:
        spec["spec"]["priorityClassName"] = priority
    worker = worker_of(job)
    set_argument(worker, "--prefix", prefix)
    worker["resources"] = {"requests": {"cpu": str(cpu), "memory": memory},
                           "limits": {"cpu": str(cpu), "memory": memory}}
    return job


def pipeline(name, cpu_m, memory, namespace="hw5", hostname=None, priority=None,
             pace_seconds=0, parts=4):
    prefix = f"hw5/{name}"
    partition = configure(load_template("job-partition.yaml"), f"{name}-partition", prefix,
                          memory="256Mi", cpu="100m", experiment=name)
    set_argument(worker_of(partition), "--parts", parts)
    mapper = configure(load_template("job-parallel.yaml"), f"{name}-map", prefix,
                       namespace, memory, f"{cpu_m}m", priority, hostname, name)
    mapper["spec"].update(completions=parts, parallelism=2, backoffLimit=2,
                          podFailurePolicy={"rules": [
                              {"action": "Ignore", "onPodConditions": [{"type": "DisruptionTarget"}]},
                              {"action": "FailJob", "onExitCodes": {
                                  "containerName": "worker", "operator": "In", "values": [137]}}]})
    set_argument(worker_of(mapper), "--pace-seconds", pace_seconds)
    reducer = configure(load_template("job-reduce.yaml"), f"{name}-reduce", prefix,
                        namespace, memory, "500m", priority, hostname, name)
    return {f"{name}-partition.yaml": partition, f"{name}-map.yaml": mapper,
            f"{name}-reduce.yaml": reducer}
