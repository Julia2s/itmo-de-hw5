"""Create newline-aligned partitions of the unchanged HW3 S3 object."""
from __future__ import annotations

import argparse
import json
import uuid

from common import VACANCIES_KEY, get_s3_client
from runtime import assert_input, input_identity, put_json, safe_prefix, validate_manifest


def next_line_start(cli, key, offset, size, scan_bytes=64 * 1024):
    if offset == 0 or offset >= size:
        return min(offset, size)
    if cli.get_object(key, start=offset - 1, end=offset - 1) == b"\n":
        return offset
    while offset < size:
        stop = min(offset + scan_bytes, size)
        data = cli.get_object(key, start=offset, end=stop - 1)
        if len(data) != stop - offset:
            raise IOError("Short read while finding line boundary")
        nl = data.find(b"\n")
        if nl >= 0:
            return offset + nl + 1
        offset = stop
    return size


def partition(cli, key, parts=4, prefix="hw5/parallel"):
    if parts < 1:
        raise ValueError("parts must be positive")
    prefix = safe_prefix(prefix)
    identity = input_identity(cli, key)
    size = identity["size"]
    boundaries = [0] + [next_line_start(cli, key, size * i // parts, size)
                        for i in range(1, parts)] + [size]
    manifest = {"version": 1, "run_id": uuid.uuid4().hex, "input": identity,
                "parts": [{"index": i, "start": boundaries[i], "end": boundaries[i + 1]}
                          for i in range(parts)]}
    validate_manifest(manifest)
    assert_input(cli, identity)
    put_json(cli, f"{prefix}/partitions.json", manifest)
    return manifest


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--key", default=VACANCIES_KEY)
    p.add_argument("--parts", type=int, default=4)
    p.add_argument("--prefix", default="hw5/parallel")
    args = p.parse_args()
    print(json.dumps(partition(get_s3_client(), args.key, args.parts, args.prefix), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
