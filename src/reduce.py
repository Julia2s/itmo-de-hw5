"""Validate and merge all full frequency tables from the current manifest."""
from __future__ import annotations

import argparse
import csv
import io
import json
import time
from collections import Counter
from pathlib import Path

from common import get_s3_client, write_frequency_results
from runtime import (add_stats, assert_input, empty_stats, enable_memory_log,
                         get_json, put_json, safe_prefix, sha256, validate_manifest)


def reduce_parts(cli, prefix="hw5/parallel", output="data/results_hw5"):
    t0 = time.perf_counter()
    prefix = safe_prefix(prefix)
    manifest = get_json(cli, f"{prefix}/partitions.json")
    validate_manifest(manifest)
    assert_input(cli, manifest["input"])
    counters = {k: Counter() for k in (1, 2, 3)}
    stats = empty_stats()
    for part in manifest["parts"]:
        base = f"{prefix}/parts/{part['index']}"
        summary = get_json(cli, f"{base}/summary.json")
        expected = {"run_id": manifest["run_id"], "index": part["index"],
                    "input": manifest["input"], "start": part["start"], "end": part["end"]}
        if any(summary.get(k) != v for k, v in expected.items()):
            raise ValueError(f"Stale/mismatched part {part['index']}")
        for k in counters:
            name = f"skills_{k}.csv"
            data = cli.get_object(f"{base}/{name}")
            if sha256(data) != summary["sha256"][name]:
                raise ValueError(f"Uncommitted/corrupt {base}/{name}")
            reader = csv.DictReader(io.StringIO(data.decode("utf-8")))
            if reader.fieldnames != ["combo", "frequency"]:
                raise ValueError("Invalid frequency CSV schema")
            seen, total = set(), 0
            for row in reader:
                combo, freq = tuple(row["combo"].split(";")), int(row["frequency"])
                if len(combo) != k or combo in seen or freq <= 0:
                    raise ValueError("Invalid/duplicate combination")
                seen.add(combo)
                counters[k][combo] += freq
                total += freq
            if total != summary["stats"]["events_per_k"][str(k)]:
                raise ValueError("Part event count mismatch")
        add_stats(stats, summary["stats"])
    assert_input(cli, manifest["input"])
    # Do not publish if a new partition run was started during reduction.
    if get_json(cli, f"{prefix}/partitions.json") != manifest:
        raise ValueError("Manifest changed during reduction")
    target = f"{prefix}/final"
    written = write_frequency_results(cli, counters, target, output)
    result = {"run_id": manifest["run_id"], "input": manifest["input"],
              "parts": len(manifest["parts"]), "stats": stats, "results": written,
              "sha256": {f"skills_{k}.csv": sha256((Path(output) / f"skills_{k}.csv").read_bytes())
                         for k in counters}, "elapsed_s": round(time.perf_counter() - t0, 3)}
    put_json(cli, f"{target}/summary.json", result)
    print(json.dumps(result, ensure_ascii=False, indent=2), flush=True)
    return result


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--prefix", default="hw5/parallel")
    p.add_argument("--output", default="data/results_hw5")
    args = p.parse_args()
    enable_memory_log()
    reduce_parts(get_s3_client(), **vars(args))


if __name__ == "__main__":
    main()
