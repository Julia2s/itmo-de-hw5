"""Read RustFS cgroup memory through kubectl exec; no workload is started."""
import argparse
import json
import subprocess
from datetime import datetime, timezone
from pathlib import Path


def inspect(context="hw5", namespace="hw5"):
    base = ["kubectl", "--context", context, "-n", namespace]
    result = json.loads(subprocess.check_output(base + ["get", "pods", "-l", "app=rustfs", "-o", "json"],
                                               encoding="utf-8", timeout=30))
    pods = [pod for pod in result["items"] if pod.get("status", {}).get("phase") == "Running"]
    if len(pods) != 1:
        raise ValueError("Expected one Running RustFS Pod")
    pod = pods[0]
    # RustFS's own container, not a sidecar: /sys/fs/cgroup is its namespaced controller.
    cmd = ["sh", "-c", "for f in memory.current memory.peak memory.max memory.events memory.stat "
           "memory/memory.usage_in_bytes memory/memory.max_usage_in_bytes memory/memory.limit_in_bytes; "
           "do echo FILE:$f; cat /sys/fs/cgroup/$f 2>/dev/null || true; done"]
    text = subprocess.check_output(base + ["exec", pod["metadata"]["name"], "-c", "rustfs", "--"] + cmd,
                                   encoding="utf-8", timeout=30)
    return {"at": datetime.now(timezone.utc).isoformat(), "pod": pod["metadata"]["name"],
            "container_statuses": pod["status"].get("containerStatuses", []), "cgroup": text}


if __name__ == "__main__":
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--context", default="hw5")
    p.add_argument("--namespace", default="hw5")
    p.add_argument("--output", default="report/logs/storage-memory.jsonl")
    args = p.parse_args()
    record = inspect(args.context, args.namespace)
    Path(args.output).parent.mkdir(parents=True, exist_ok=True)
    with Path(args.output).open("a", encoding="utf-8") as stream:
        stream.write(json.dumps(record, ensure_ascii=False) + "\n")
    print(json.dumps(record, ensure_ascii=False, indent=2))
