#!/usr/bin/env python3
import re
import sys
import time
from typing import List, Optional, Dict, Any

import boto3
import click
from concurrent.futures import ThreadPoolExecutor, as_completed
from botocore.config import Config
from botocore.exceptions import ClientError

def make_session(profile: Optional[str], region: Optional[str]) -> boto3.Session:
    if profile:
        return boto3.Session(profile_name=profile, region_name=region)
    return boto3.Session(region_name=region)

def batched(iterable, n):
    batch = []
    for item in iterable:
        batch.append(item)
        if len(batch) >= n:
            yield batch
            batch = []
    if batch:
        yield batch

def list_bucket_names(s3) -> List[str]:
    resp = s3.list_buckets()
    return [b["Name"] for b in resp.get("Buckets", [])]

def filter_buckets(names: List[str], include_prefix: Optional[str], exclude_regex: Optional[str]) -> List[str]:
    out = names
    if include_prefix:
        out = [n for n in out if n.startswith(include_prefix)]
    if exclude_regex:
        rx = re.compile(exclude_regex)
        out = [n for n in out if not rx.search(n)]
    return out

def empty_bucket(s3_client, bucket: str, batch_size: int = 1000, max_delete_retries: int = 5) -> Dict[str, Any]:
    paginator = s3_client.get_paginator("list_objects_v2")
    total_deleted = 0
    pages = paginator.paginate(Bucket=bucket, PaginationConfig={"PageSize": 1000})

    for page in pages:
        contents = page.get("Contents", [])
        if not contents:
            continue
        keys = [{"Key": obj["Key"]} for obj in contents]

        for batch in batched(keys, batch_size):
            for attempt in range(max_delete_retries):
                try:
                    resp = s3_client.delete_objects(
                        Bucket=bucket,
                        Delete={"Objects": batch, "Quiet": True},
                    )
                    deleted = resp.get("Deleted", [])
                    total_deleted += len(deleted)

                    errors = resp.get("Errors", [])
                    if errors:
                        batch = [{"Key": e["Key"]} for e in errors]
                        time.sleep(min(2 ** attempt * 0.25, 4.0))
                        continue
                    break
                except ClientError as e:
                    code = e.response.get("Error", {}).get("Code")
                    if code in ("NoSuchBucket",):
                        return {"bucket": bucket, "deleted": total_deleted, "status": "gone"}
                    time.sleep(min(2 ** attempt * 0.5, 4.0))
            else:
                raise RuntimeError(f"Failed to delete some objects in {bucket} after retries.")

    return {"bucket": bucket, "deleted": total_deleted, "status": "emptied"}

def delete_bucket(s3_client, bucket: str, retries: int = 5) -> str:
    for i in range(retries):
        try:
            s3_client.delete_bucket(Bucket=bucket)
            return "deleted"
        except ClientError as e:
            code = e.response.get("Error", {}).get("Code")
            if code in ("NoSuchBucket",):
                return "gone"
            time.sleep(min(2 ** i * 0.5, 5.0))
    try:
        empty_bucket(s3_client, bucket)
        s3_client.delete_bucket(Bucket=bucket)
        return "deleted"
    except Exception:
        return "failed"
    
def worker(session, bucket: str, batch_size: int, do_delete_bucket: bool) -> Dict[str, Any]:
    cfg = Config(
        retries={"max_attempts": 10, "mode": "standard"},
        max_pool_connections=64,
        read_timeout=120,
        connect_timeout=10,
    )
    s3 = session.client("s3", config=cfg)
    summary = empty_bucket(s3, bucket, batch_size=batch_size)
    if do_delete_bucket:
        summary["final"] = delete_bucket(s3, bucket)
    else:
        summary["final"] = "emptied"
    return summary

@click.command()
@click.option("--profile", help="AWS SSO profile name.")
@click.option("--region", help="AWS region name.")
@click.option("--include-prefix", help="target bucket name prefix filter.")
@click.option("--exclude-regex", help="target bucket name exclude regex filter.")
@click.option("--buckets-file", type=click.Path(exists=True), help="list of buckets to delete (one per line).")
@click.option("--max-workers", type=int, default=16, show_default=True, help="number of buckets to process in parallel.")
@click.option("--batch-size", type=int, default=1000, show_default=True, help="number of objects to delete in each batch (max 1000).")
@click.option("--dry-run", is_flag=True, help="only show target buckets without deleting.")
@click.option("--delete-bucket", is_flag=True, default=False, help="delete the bucket itself after emptying (default: keep bucket).")
def main(profile, region, include_prefix, exclude_regex, buckets_file, max_workers, batch_size, dry_run, delete_bucket):
    """Tool for quickly deleting a large number of S3 buckets (without versioning).

    By default, this tool empties the buckets (deletes all objects) but keeps the buckets themselves.
    Use --delete-bucket to delete the bucket itself after emptying.
    """

    session = make_session(profile, region)

    if buckets_file:
        with open(buckets_file) as f:
            candidates = [l.strip() for l in f if l.strip()]  # noqa: E741
    else:
        s3 = session.client("s3")
        candidates = list_bucket_names(s3)

    targets = filter_buckets(candidates, include_prefix, exclude_regex)

    if not targets:
        click.echo("No buckets matched filters.", err=True)
        sys.exit(1)

    click.echo(f"Target buckets: {len(targets)}")
    for n in targets:
        click.echo(f" - {n}")

    if dry_run:
        sys.exit(0)

    results = []
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futs = {ex.submit(worker, session, b, batch_size, delete_bucket): b for b in targets}
        for fut in as_completed(futs):
            b = futs[fut]
            try:
                res = fut.result()
                results.append(res)
                click.echo(f"[{b}] deleted={res['deleted']} final={res['final']}")
            except Exception as e:
                click.echo(f"[{b}] ERROR: {e}", err=True)

    ok = [r for r in results if r.get("final") in ("deleted", "gone")]
    ng = [r for r in results if r.get("final") not in ("deleted", "gone")]

    click.echo("\nSummary:")
    click.echo(f"  success: {len(ok)} / {len(targets)}")
    if ng:
        click.echo("  failed buckets:")
        for r in ng:
            click.echo(f"   - {r.get('bucket')} (status={r.get('final')})")

    sys.exit(0 if not ng else 2)


if __name__ == "__main__":
    main()