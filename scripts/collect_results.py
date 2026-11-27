"""Download only committed, checksum-verified final result files."""
import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from common import get_s3_client
from runtime import get_json, safe_prefix, sha256


def collect(cli, prefix, output):
    prefix, output = safe_prefix(prefix), Path(output)
    summary = get_json(cli, f"{prefix}/summary.json")
    if prefix.endswith("/final"):
        manifest = get_json(cli, f"{prefix[:-6]}/partitions.json")
        if summary.get("run_id") != manifest["run_id"] or summary.get("input") != manifest["input"]:
            raise ValueError("Final result belongs to an older partition run")
    # Verify all objects before updating any local result.
    objects = {}
    for k in (1, 2, 3):
        name = f"skills_{k}.csv"
        data = cli.get_object(f"{prefix}/{name}")
        if sha256(data) != summary["sha256"][name]:
            raise ValueError(f"Checksum mismatch: {name}")
        objects[name] = data
    output.mkdir(parents=True, exist_ok=True)
    for name, data in objects.items():
        (output / name).write_bytes(data)
    (output / "summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"Downloaded and verified {len(objects)} CSV files to {output}")


if __name__ == "__main__":
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--prefix", required=True)
    p.add_argument("--output", required=True)
    args = p.parse_args()
    collect(get_s3_client(), args.prefix, args.output)
