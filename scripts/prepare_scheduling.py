"""Generate real calculation Jobs for CPU, preemption and quota experiments."""
import argparse
import json
import math
import subprocess
from decimal import Decimal
from pathlib import Path

import yaml
from manifests import configure, load_template, pipeline, set_argument, worker_of


def quantity(value):
    value = str(value)
    for suffix, factor in (("Ki", 1024), ("Mi", 1024**2), ("Gi", 1024**3),
                           ("Ti", 1024**4), ("Pi", 1024**5), ("Ei", 1024**6),
                           ("n", 1e-9), ("u", 1e-6), ("m", 1e-3),
                           ("k", 1000), ("M", 1e6), ("G", 1e9), ("T", 1e12)):
        if value.endswith(suffix):
            return Decimal(value[:-len(suffix)]) * Decimal(str(factor))
    return Decimal(value)


def pod_request(spec, resource):
    def request(container):
        return quantity(container.get("resources", {}).get("requests", {}).get(resource, "0"))
    apps = sum((request(c) for c in spec.get("containers", [])), Decimal(0))
    sidecars, init_peak = Decimal(0), Decimal(0)
    for container in spec.get("initContainers", []):
        req = request(container)
        if container.get("restartPolicy") == "Always":
            sidecars += req
            init_peak = max(init_peak, sidecars)
        else:
            init_peak = max(init_peak, sidecars + req)
    effective = max(apps + sidecars, init_peak)
    pod_level = spec.get("resources", {}).get("requests", {}).get(resource)
    if pod_level is not None:
        effective = quantity(pod_level)
    return effective + quantity(spec.get("overhead", {}).get(resource, "0"))


def generate(nodes, pods, namespace, memory="1Gi", pace_seconds=1):
    nodes = [n for n in nodes["items"] if not n.get("spec", {}).get("unschedulable")]
    if len(nodes) != 1:
        raise ValueError("This demonstration requires exactly one schedulable node")
    node = nodes[0]
    if not any(c["type"] == "Ready" and c["status"] == "True" for c in node["status"]["conditions"]):
        raise ValueError("Node is not Ready")
    if any(t["effect"] in ("NoSchedule", "NoExecute") for t in node.get("spec", {}).get("taints", [])):
        raise ValueError("Node has blocking taints; resolve them before generation")
    name = node["metadata"]["name"]
    hostname = node["metadata"]["labels"]["kubernetes.io/hostname"]
    active = [p for p in pods["items"] if p.get("status", {}).get("phase") not in ("Succeeded", "Failed")]
    if any(p["metadata"].get("labels", {}).get("app") in ("hw5-scheduling", "skills-experiment") for p in active):
        raise ValueError("Remove previous scheduling demo Pods first")
    if any(not p.get("spec", {}).get("nodeName") for p in active):
        raise ValueError("Unscheduled active Pods exist; wait/resolve them first")
    bound = [p for p in active if p["spec"]["nodeName"] == name]
    if any(p["spec"].get("priority", 0) < -500 for p in bound):
        raise ValueError("Existing Pods have priority below hw5-high; isolate the demo first")
    alloc = node["status"]["allocatable"]
    used_cpu = sum((pod_request(p["spec"], "cpu") for p in bound), Decimal(0))
    used_memory = sum((pod_request(p["spec"], "memory") for p in bound), Decimal(0))
    free_m = math.floor((quantity(alloc["cpu"]) - used_cpu) * 1000)
    free_memory = quantity(alloc["memory"]) - used_memory
    if free_m < 1200 or free_memory < 2 * quantity(memory) or int(alloc["pods"]) - len(bound) < 2:
        raise ValueError("Insufficient CPU, memory or Pod slots for a controlled demo")
    request = math.floor(free_m * 0.60)
    if not request <= free_m < 2 * request:
        raise ValueError("Unable to construct CPU contention")
    budget = (f"node={name}\nallocatable_cpu={alloc['cpu']}\nused_requests_cpu={used_cpu}\n"
              f"free_cpu_m={free_m}\nrequest_each_m={request}\n"
              f"one_fits={request <= free_m}; two_fit={2 * request <= free_m}\n"
              f"free_memory_mib={free_memory / Decimal(1024**2)}\n"
              "Snapshot only: regenerate if cluster workloads change.\n")
    manifests = pipeline("cpu", request, memory, namespace, hostname,
                         pace_seconds=pace_seconds)
    manifests.update(pipeline("low", request, memory, namespace, hostname,
                              priority="hw5-low", pace_seconds=pace_seconds))
    urgent = configure(load_template("job-baseline.yaml"), "urgent", "hw5/urgent",
                       namespace, memory, f"{request}m", "hw5-high", hostname, "urgent")
    manifests["urgent.yaml"] = urgent
    worker_of(urgent)["env"] = [{"name": "S3_ENDPOINT", "value": "http://rustfs.hw5.svc.cluster.local:9000"}]
    # Two map Pods fit on the node, but 600m+600m exceeds the namespace's 1 CPU quota.
    manifests.update(pipeline("quota", 600, memory, "hw5-quota", hostname,
                              pace_seconds=pace_seconds))
    config = list(yaml.safe_load_all((Path(__file__).resolve().parents[1] / "k8s/config.yaml").read_text()))
    for doc in config:
        doc["metadata"]["namespace"] = "hw5-quota"
    manifests["quota-config.yaml"] = config
    return manifests, budget


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--context", default="hw5")
    p.add_argument("--namespace", default="hw5")
    p.add_argument("--output", default="k8s/generated")
    p.add_argument("--memory", default="1Gi", help="Measured conservative memory per worker/reducer")
    p.add_argument("--pace-seconds", type=float, default=1,
                   help="Explicit per-chunk delay for observing scheduling; not a benchmark")
    args = p.parse_args()
    def fetch(kind):
        cmd = ["kubectl", "--context", args.context, "get", kind, "-o", "json"]
        if kind == "pods":
            cmd.append("--all-namespaces")
        return json.loads(subprocess.check_output(cmd, encoding="utf-8", timeout=60))
    nodes, pods = fetch("nodes"), fetch("pods")
    # A Docker node can expose all WSL CPUs even when its parent cgroup is capped.
    # Validate before writing a limit that a cgroup-v1 parent would reject.
    if len(nodes['items']) == 1:
        node = nodes['items'][0]
        try:
            raw = subprocess.check_output(['docker', 'inspect', node['metadata']['name']],
                                          encoding='utf-8', stderr=subprocess.DEVNULL, timeout=30)
        except (OSError, subprocess.SubprocessError):
            raw = None  # Non-Docker clusters remain supported.
        if raw:
            host = json.loads(raw)[0]['HostConfig']
            limit = Decimal(host.get('NanoCpus', 0)) / Decimal(10**9)
            if limit and quantity(node['status']['allocatable']['cpu']) > limit:
                raise ValueError('Node allocatable exceeds its Docker CPU limit. Run '
                                 'scripts/align_node_resources.py --apply after archiving failed Jobs, '
                                 'then regenerate manifests.')
    if args.pace_seconds < 0 or quantity(args.memory) <= 0:
        p.error("Memory must be positive and pace nonnegative")
    manifests, budget = generate(nodes, pods, args.namespace, args.memory, args.pace_seconds)
    output = Path(args.output)
    output.mkdir(parents=True, exist_ok=True)
    for name, manifest in manifests.items():
        docs = manifest if isinstance(manifest, list) else [manifest]
        (output / name).write_text(yaml.safe_dump_all(docs, sort_keys=False), encoding="utf-8")
    (output / "cpu-budget.txt").write_text(budget, encoding="utf-8")
    # Preserve the evidence used for requests; this command does not mutate the cluster.
    (output / "snapshot.json").write_text(json.dumps({"nodes": nodes, "pods": pods}, indent=2), encoding="utf-8")
    print(budget)


if __name__ == "__main__":
    main()
