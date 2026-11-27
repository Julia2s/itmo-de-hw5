"""Range processing and provenance shared by the HW5 command line tools."""
from __future__ import annotations

import atexit
import hashlib
import json
import sys
import signal
import threading
import time
from collections import Counter
from pathlib import Path, PurePosixPath

from common import S3_BUCKET, VARIANT_FILTER_MODE
from mapreduce_v2 import CHUNK_SIZE_BYTES, STACK_SIZES, decode_lines, map_chunk


def put_json(cli, key, value):
    cli.put_object(key, json.dumps(value, ensure_ascii=False, indent=2).encode(),
                   content_type="application/json")


def get_json(cli, key):
    return json.loads(cli.get_object(key))


def input_identity(cli, key):
    info = cli.head_object(key)
    if info is None:
        raise FileNotFoundError(key)
    return {"bucket": S3_BUCKET, "key": key, "size": info.size,
            "etag": info.etag, "last_modified": info.last_modified,
            "filter_mode": VARIANT_FILTER_MODE}


def assert_input(cli, expected):
    if input_identity(cli, expected["key"]) != expected:
        raise ValueError("Input object or variant changed; create new partitions")


def validate_manifest(manifest):
    size = manifest["input"]["size"]
    parts = manifest["parts"]
    if not manifest.get("run_id") or not parts or size < 0:
        raise ValueError("Invalid manifest")
    cursor = 0
    for index, part in enumerate(parts):
        if part["index"] != index or part["start"] != cursor or not cursor <= part["end"] <= size:
            raise ValueError("Partitions must cover the input exactly once, in order")
        cursor = part["end"]
    if cursor != size:
        raise ValueError("Partitions do not cover the entire input")


def iter_range_chunks(cli, key, start, end, chunk_bytes=CHUNK_SIZE_BYTES):
    """Read [start,end); boundaries must be aligned to physical lines."""
    if chunk_bytes <= 0 or not 0 <= start <= end:
        raise ValueError("Invalid range/chunk size")
    pending = b""
    offset = start
    while offset < end:
        stop = min(offset + chunk_bytes, end)
        data = cli.get_object(key, start=offset, end=stop - 1)
        if len(data) != stop - offset:
            raise IOError("Short S3 Range read")
        offset = stop
        buf = pending + data
        if offset == end:
            yield buf
            pending = b""
        else:
            nl = buf.rfind(b"\n")
            if nl < 0:
                pending = buf
            else:
                yield buf[:nl + 1]
                pending = buf[nl + 1:]


def empty_stats():
    return {"lines_total": 0, "lines_prefiltered": 0,
            "lines_passed_prefilter": 0, "lines_with_skills": 0,
            "lines_malformed": 0, "events_per_k": {str(k): 0 for k in STACK_SIZES}}


def add_stats(target, source):
    for field in target:
        if field != "events_per_k":
            target[field] += source[field]
    for k in STACK_SIZES:
        target["events_per_k"][str(k)] += source["events_per_k"].get(
            str(k), source["events_per_k"].get(k, 0))


def calculate(cli, key, start, end, chunk_bytes=CHUNK_SIZE_BYTES, pace_seconds=0):
    if pace_seconds < 0:
        raise ValueError("pace_seconds must be nonnegative")
    counters = {k: Counter() for k in STACK_SIZES}
    stats = empty_stats()
    for data in iter_range_chunks(cli, key, start, end, chunk_bytes):
        mapped, chunk_stats = map_chunk(decode_lines(data))
        for k in STACK_SIZES:
            counters[k].update(mapped[k])
        add_stats(stats, chunk_stats)
        if pace_seconds:
            time.sleep(pace_seconds)
    return counters, stats


def sha256(data):
    return hashlib.sha256(data).hexdigest()


def safe_prefix(value):
    value = value.strip("/")
    if not value or "\\" in value or any(p in ("..", ".") for p in value.split("/")):
        raise ValueError("Expected a nonempty S3 prefix without relative path components")
    return value


def memory_metrics():
    """Resolve memory controller using /proc, including cgroup namespaces."""
    result = {}
    try:
        import resource
        rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        result["rss_peak_mib"] = rss / (1024**2 if sys.platform == "darwin" else 1024)
    except ImportError:
        pass
    try:
        groups = [line.split(":", 2) for line in Path("/proc/self/cgroup").read_text().splitlines()]
        mounts = Path("/proc/self/mountinfo").read_text().splitlines()
        for mount in mounts:
            left, right = mount.split(" - ", 1)
            fields, fs = left.split(), right.split()
            version = 2 if fs[0] == "cgroup2" else 1 if fs[0] == "cgroup" and "memory" in fs[2].split(",") else 0
            if not version:
                continue
            group = next((path for _, controllers, path in groups
                          if (version == 2 and not controllers) or
                          (version == 1 and "memory" in controllers.split(","))), None)
            if group is None:
                continue
            root, mountpoint = fields[3], fields[4]
            # cgroup namespaces often expose '/' even when mount root is deeper.
            rel = str(PurePosixPath(group).relative_to(root)) if PurePosixPath(group).is_relative_to(root) else group.lstrip("/")
            folder = Path(mountpoint) / rel
            peak_name = "memory.peak" if version == 2 else "memory.max_usage_in_bytes"
            max_name = "memory.max" if version == 2 else "memory.limit_in_bytes"
            if (folder / peak_name).exists():
                result.update(source=f"cgroup_v{version}",
                              peak_mib=int((folder / peak_name).read_text()) / 1024**2,
                              memory_limit=(folder / max_name).read_text().strip())
                optional = {"current": "memory.current" if version == 2 else "memory.usage_in_bytes",
                            "stat": "memory.stat", "events": "memory.events", "cpu": "cpu.stat"}
                for name, filename in optional.items():
                    path = folder / filename
                    if not path.exists():
                        continue
                    text = path.read_text()
                    if name == "current":
                        result["current_mib"] = int(text) / 1024**2
                    else:
                        values = {line.split()[0]: int(line.split()[1]) for line in text.splitlines()}
                        if name == "stat":
                            for field in ("anon", "file", "kernel"):
                                if field in values:
                                    result[f"{field}_mib"] = values[field] / 1024**2
                        else:
                            result[name] = values
                return result
    except (OSError, ValueError, StopIteration):
        pass
    if "rss_peak_mib" in result:
        result.update(source="process_rss_fallback", peak_mib=result["rss_peak_mib"])
    else:
        result["source"] = "unavailable_on_this_platform"
    return result


def enable_memory_log(interval=1.0):
    """Sample cgroup memory while running and handle SIGTERM even as PID 1."""
    if interval <= 0:
        raise ValueError("interval must be positive")
    stop = threading.Event()
    started = time.monotonic()
    def sample():
        while not stop.is_set():
            metrics = memory_metrics()
            metrics["elapsed_s"] = round(time.monotonic() - started, 3)
            print("MEMORY_SAMPLE=" + json.dumps(metrics), flush=True)
            stop.wait(interval)
    thread = threading.Thread(target=sample, name="memory-monitor", daemon=True)
    thread.start()
    def emit():
        stop.set()
        thread.join(timeout=2)
        metrics = memory_metrics()
        print("MEMORY_METRICS=" + json.dumps(metrics), flush=True)
        if "peak_mib" in metrics:
            print(f"MEMORY_PEAK_MIB={metrics['peak_mib']:.6f}", flush=True)
    atexit.register(emit)
    def terminate(signum, _frame):
        print(f"TERMINATED_BY_SIGNAL={signal.Signals(signum).name}", flush=True)
        raise SystemExit(128 + signum)
    signal.signal(signal.SIGTERM, terminate)
    signal.signal(signal.SIGINT, terminate)
