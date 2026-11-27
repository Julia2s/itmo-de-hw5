"""Run the original HW3 mapper on a full object or an Indexed Job part."""
from __future__ import annotations

from runtime import enable_memory_log

import argparse
import json
import os
import time
from pathlib import Path

from common import VACANCIES_KEY, get_s3_client, write_frequency_results
from runtime import (assert_input, calculate, get_json, input_identity,
                         put_json, safe_prefix, sha256, validate_manifest)


def run(cli, mode="sequential", prefix="hw5/baseline", index=None,
        key=VACANCIES_KEY, output="data/results_hw5", chunk_bytes=8 * 1024 * 1024,
        pace_seconds=0):
    t0 = time.perf_counter()
    prefix = safe_prefix(prefix)
    if mode == "part":
        manifest = get_json(cli, f"{prefix}/partitions.json")
        validate_manifest(manifest)
        if index is None or not 0 <= index < len(manifest["parts"]):
            raise ValueError("Valid --index or JOB_COMPLETION_INDEX required")
        part = manifest["parts"][index]
        identity = manifest["input"]
        assert_input(cli, identity)
        key = identity["key"]
        start, end = part["start"], part["end"]
        target = f"{prefix}/parts/{index}"
        run_id = manifest["run_id"]
    elif mode == "sequential":
        identity = input_identity(cli, key)
        start, end = 0, identity["size"]
        target, run_id = prefix, None
    else:
        raise ValueError(mode)
    print(json.dumps({"phase": "start", "mode": mode, "index": index,
                      "run_id": run_id, "start": start, "end": end,
                      "input": identity, "pace_seconds": pace_seconds}, ensure_ascii=False), flush=True)
    counters, stats = calculate(cli, key, start, end, chunk_bytes, pace_seconds)
    assert_input(cli, identity)
    written = write_frequency_results(cli, counters, target, output)
    hashes = {f"skills_{k}.csv": sha256((Path(output) / f"skills_{k}.csv").read_bytes())
              for k in counters}
    summary = {"run_id": run_id, "index": index, "input": identity,
               "start": start, "end": end, "stats": stats, "sha256": hashes,
               "results": written, "pace_seconds": pace_seconds,
               "elapsed_s": round(time.perf_counter() - t0, 3)}
    # Written last: reducer requires this marker plus matching CSV hashes.
    put_json(cli, f"{target}/summary.json", summary)
    print(json.dumps(summary, ensure_ascii=False, indent=2), flush=True)
    print("WORKER_COMPLETE", flush=True)
    return summary


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--mode", choices=("sequential", "part"), default="sequential")
    p.add_argument("--prefix", default="hw5/baseline")
    p.add_argument("--index", type=int, default=os.getenv("JOB_COMPLETION_INDEX"))
    p.add_argument("--key", default=VACANCIES_KEY)
    p.add_argument("--output", default="data/results_hw5")
    p.add_argument("--pace-seconds", type=float, default=0,
                   help="Explicit per-chunk pause for scheduler observation, never benchmarking")
    args = p.parse_args()
    enable_memory_log()
    run(get_s3_client(), **vars(args))


if __name__ == "__main__":
    main()
