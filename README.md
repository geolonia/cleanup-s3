# cleanup s3

This script provides a command-line interface for cleaning up Amazon S3 buckets by deleting all objects within them and optionally deleting the buckets themselves.

## Usage

```bash
uv sync
uv run main.py [OPTIONS]
```

## Options

```
$ uv run main.py --help
Usage: main.py [OPTIONS]

  Tool for quickly deleting a large number of S3 buckets (without versioning).

  By default, this tool empties the buckets (deletes all objects) but keeps
  the buckets themselves. Use --delete-bucket to delete the bucket itself
  after emptying.

Options:
  --profile TEXT         AWS SSO profile name.
  --region TEXT          AWS region name.
  --include-prefix TEXT  target bucket name prefix filter.
  --exclude-regex TEXT   target bucket name exclude regex filter.
  --buckets-file PATH    list of buckets to delete (one per line).
  --max-workers INTEGER  number of buckets to process in parallel.  [default:
                         16]
  --batch-size INTEGER   number of objects to delete in each batch (max 1000).
                         [default: 1000]
  --dry-run              only show target buckets without deleting.
  --delete-bucket        delete the bucket itself after emptying (default:
                         keep bucket).
  --help                 Show this message and exit.
```

## Example

```bash
uv run main.py --profile my-profile --region ap-northeast-1 --include-prefix my-bucket --dry-run
```