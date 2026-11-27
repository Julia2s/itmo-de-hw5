"""Compare full HW5 tables with HW3, including ordering and byte hashes."""
import argparse
import csv
import hashlib
import json
from pathlib import Path


def read_table(path, k):
    with Path(path).open(encoding="utf-8", newline="") as stream:
        reader = csv.DictReader(stream)
        if reader.fieldnames != ["combo", "frequency"]:
            raise ValueError(f"Invalid columns in {path}")
        rows = [(row["combo"], int(row["frequency"])) for row in reader]
    if len({combo for combo, _ in rows}) != len(rows):
        raise ValueError(f"Duplicate combinations in {path}")
    if any(freq <= 0 or len(combo.split(";")) != k for combo, freq in rows):
        raise ValueError(f"Invalid combination/frequency in {path}")
    if rows != sorted(rows, key=lambda row: (-row[1], tuple(row[0].split(";")))):
        raise ValueError(f"Incorrect ordering in {path}")
    return rows


def verify(reference, actual):
    checks = {}
    for k in (1, 2, 3):
        name = f"skills_{k}.csv"
        ref, got = Path(reference) / name, Path(actual) / name
        ref_rows, got_rows = read_table(ref, k), read_table(got, k)
        ref_hash = hashlib.sha256(ref.read_bytes()).hexdigest()
        got_hash = hashlib.sha256(got.read_bytes()).hexdigest()
        checks[name] = {
            "rows": len(got_rows), "expected_rows": len(ref_rows),
            "total_frequency": sum(v for _, v in got_rows),
            "expected_total_frequency": sum(v for _, v in ref_rows),
            "top10_match": got_rows[:10] == ref_rows[:10],
            "full_match": got_rows == ref_rows, "sha256_match": ref_hash == got_hash,
            "sha256": got_hash,
        }
        if k == 2:
            checks[name]["top10"] = got_rows[:10]
    ok = all(c["full_match"] and c["sha256_match"] for c in checks.values())
    return {"ok": ok, "checks": checks}


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--reference", required=True)
    p.add_argument("--actual", required=True)
    p.add_argument("--json-output")
    args = p.parse_args()
    try:
        result = verify(args.reference, args.actual)
    except (OSError, ValueError, KeyError) as exc:
        result = {"ok": False, "error": str(exc)}
    text = json.dumps(result, ensure_ascii=False, indent=2)
    print(text)
    if args.json_output:
        Path(args.json_output).parent.mkdir(parents=True, exist_ok=True)
        Path(args.json_output).write_text(text, encoding="utf-8")
    return 0 if result["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
