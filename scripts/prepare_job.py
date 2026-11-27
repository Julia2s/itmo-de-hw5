"""Write a uniquely named sequential Job for a memory experiment."""
import argparse
from pathlib import Path

import yaml
from manifests import configure, worker_of


def make_job(template, name, prefix, memory=None, endpoint=None):
    original = worker_of(template)["resources"]
    job = configure(template, name, prefix, memory=memory or original["limits"]["memory"])
    worker_of(job)["resources"]["requests"]["cpu"] = original["requests"]["cpu"]
    worker_of(job)["resources"]["limits"]["cpu"] = original["limits"]["cpu"]
    if endpoint:
        env = worker_of(job).setdefault("env", [])
        env[:] = [v for v in env if v["name"] != "S3_ENDPOINT"]
        env.append({"name": "S3_ENDPOINT", "value": endpoint})
    return job


if __name__ == "__main__":
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--template", required=True)
    p.add_argument("--name", required=True)
    p.add_argument("--prefix", required=True)
    p.add_argument("--memory")
    p.add_argument("--endpoint")
    p.add_argument("--output", required=True)
    a = p.parse_args()
    template = yaml.safe_load(Path(a.template).read_text(encoding="utf-8"))
    job = make_job(template, a.name, a.prefix, a.memory, a.endpoint)
    Path(a.output).parent.mkdir(parents=True, exist_ok=True)
    Path(a.output).write_text(yaml.safe_dump(job, sort_keys=False), encoding="utf-8")
    print(a.output)
