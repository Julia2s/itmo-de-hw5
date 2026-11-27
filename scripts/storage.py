"""Migrate an S3 input and verify its full SHA-256, without trusting multipart ETag."""
import argparse
import hashlib
import json
import os
import sys
from pathlib import Path

import boto3
from boto3.s3.transfer import TransferConfig
from botocore.config import Config
from botocore.exceptions import ClientError

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from common import S3_ACCESS_KEY, S3_SECRET_KEY, S3_BUCKET, S3_ENDPOINT, S3_REGION, VACANCIES_KEY

BLOCK = 8 * 1024 * 1024


class HashingReader:
    def __init__(self, stream):
        self.stream, self.digest, self.size = stream, hashlib.sha256(), 0

    def read(self, size=-1):
        data = self.stream.read(size)
        self.digest.update(data)
        self.size += len(data)
        return data


def object_hash(client, bucket, key):
    body = client.get_object(Bucket=bucket, Key=key)["Body"]
    digest, size = hashlib.sha256(), 0
    try:
        while data := body.read(BLOCK):
            digest.update(data)
            size += len(data)
    finally:
        body.close()
    return digest.hexdigest(), size


def identity(head):
    return {k: str(head.get(k)) for k in ("ContentLength", "ETag", "LastModified", "VersionId")}


def migrate(source, target, bucket, key):
    before = source.head_object(Bucket=bucket, Key=key)
    body = source.get_object(Bucket=bucket, Key=key)["Body"]
    reader = HashingReader(body)
    try:
        target.upload_fileobj(reader, bucket, key, Config=TransferConfig(
            multipart_threshold=BLOCK, multipart_chunksize=BLOCK, use_threads=False))
    finally:
        body.close()
    after = source.head_object(Bucket=bucket, Key=key)
    if identity(before) != identity(after):
        raise ValueError("Source changed during migration; do not use this copy")
    actual, size = object_hash(target, bucket, key)
    ok = reader.digest.hexdigest() == actual and reader.size == size == before["ContentLength"]
    if not ok:
        raise ValueError("S3 copy checksum or length mismatch")
    return {"key": key, "bytes": size, "source_sha256": reader.digest.hexdigest(),
            "target_sha256": actual, "ok": True, "source_identity": identity(before)}


def make_client(endpoint, access, secret):
    return boto3.client("s3", endpoint_url=endpoint, aws_access_key_id=access,
                        aws_secret_access_key=secret, region_name=S3_REGION,
                        config=Config(s3={"addressing_style": "path"},
                                      connect_timeout=10, read_timeout=180))


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--source-endpoint", required=True)
    p.add_argument("--target-endpoint", default=S3_ENDPOINT)
    p.add_argument("--bucket", default=S3_BUCKET)
    p.add_argument("--key", default=VACANCIES_KEY)
    p.add_argument("--source-access-key", default=os.getenv("SOURCE_S3_ACCESS_KEY", S3_ACCESS_KEY))
    p.add_argument("--source-secret-key", default=os.getenv("SOURCE_S3_SECRET_KEY", S3_SECRET_KEY))
    p.add_argument("--json-output", default="report/logs/storage-checksums.json")
    p.add_argument("--expected-sha256", help="Expected original local input hash")
    args = p.parse_args()
    if args.source_endpoint.rstrip('/') == args.target_endpoint.rstrip('/'):
        p.error("Source and target must be different S3 services")
    source = make_client(args.source_endpoint, args.source_access_key, args.source_secret_key)
    target = make_client(args.target_endpoint, S3_ACCESS_KEY, S3_SECRET_KEY)
    try:
        target.head_bucket(Bucket=args.bucket)
    except ClientError as exc:
        if str(exc.response["Error"]["Code"]) not in ("404", "NoSuchBucket", "NotFound"):
            raise
        target.create_bucket(Bucket=args.bucket)
    result = migrate(source, target, args.bucket, args.key)
    if args.expected_sha256 and result["source_sha256"] != args.expected_sha256.lower():
        raise ValueError("Migrated object differs from the original homework input")
    result.update(source_endpoint=args.source_endpoint, target_endpoint=args.target_endpoint)
    text = json.dumps(result, ensure_ascii=False, indent=2)
    Path(args.json_output).parent.mkdir(parents=True, exist_ok=True)
    Path(args.json_output).write_text(text, encoding="utf-8")
    print(text)


if __name__ == "__main__":
    main()
