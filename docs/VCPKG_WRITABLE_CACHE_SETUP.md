# Writable vcpkg Binary Cache Setup (GitHub Actions, key/secret mode)

This guide explains how to configure a writable vcpkg binary cache for CI using AWS access keys.

## Goal

- Reuse dependency binaries across runs.
- Keep PR jobs safe (read-only cache credentials).
- Allow only protected-branch jobs to write to cache.

## Security model (recommended)

- `pull_request` runs: read-only cache mode + read-only AWS credentials.
- `push` to `main`: read-write cache mode + read-write AWS credentials.
- DuckDB public cache remains a read-only fallback in all modes.

## Prerequisites

- AWS account access (or an S3-compatible endpoint).
- IAM permissions to create users, policies, and access keys.
- Admin access to repository `Settings -> Secrets and variables -> Actions`.
- CLI option: `aws`, `gh`, and `jq` installed and authenticated.

## 1. Create S3 bucket and retention

1. Create a bucket (example: `duckhog-vcpkg-cache`).
2. Use a dedicated prefix (example: `duckhog/`).
3. Add lifecycle expiration (recommended: 30-90 days).
4. Block public access and enable default encryption.

## 2. Create IAM users and policies

Create two IAM users:

- RW user for `main` pushes (can read/write cache objects).
- RO user for PRs (can read cache objects only).

### RW policy (main)

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["s3:ListBucket"],
      "Resource": "arn:aws:s3:::duckhog-vcpkg-cache",
      "Condition": {
        "StringLike": {
          "s3:prefix": ["duckhog/*"]
        }
      }
    },
    {
      "Effect": "Allow",
      "Action": ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"],
      "Resource": "arn:aws:s3:::duckhog-vcpkg-cache/duckhog/*"
    }
  ]
}
```

### RO policy (PR)

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["s3:ListBucket"],
      "Resource": "arn:aws:s3:::duckhog-vcpkg-cache",
      "Condition": {
        "StringLike": {
          "s3:prefix": ["duckhog/*"]
        }
      }
    },
    {
      "Effect": "Allow",
      "Action": ["s3:GetObject"],
      "Resource": "arn:aws:s3:::duckhog-vcpkg-cache/duckhog/*"
    }
  ]
}
```

Never grant `s3:PutObject` or `s3:DeleteObject` to the PR user.

## 3. Add repository secrets

Add these repository secrets in `Settings -> Secrets and variables -> Actions -> Secrets`:

- `VCPKG_CACHING_RW_AWS_ACCESS_KEY_ID`
- `VCPKG_CACHING_RW_AWS_SECRET_ACCESS_KEY`
- `VCPKG_CACHING_RO_AWS_ACCESS_KEY_ID`
- `VCPKG_CACHING_RO_AWS_SECRET_ACCESS_KEY`
- `VCPKG_CACHING_AWS_DEFAULT_REGION`
- `VCPKG_CACHING_AWS_ENDPOINT_URL` (optional, for S3-compatible endpoints)

For AWS S3, `VCPKG_CACHING_AWS_ENDPOINT_URL` is usually omitted.

## 4. Key/secret mode via CLI (one command at a time)

Run these commands one by one.

```bash
aws sso login --profile managed_warehouse
```

```bash
export AWS_PROFILE=managed_warehouse
```

```bash
aws sts get-caller-identity
```

```bash
REPO="PostHog/duckhog"; BUCKET="duckhog-vcpkg-cache"; PREFIX="duckhog"; REGION="us-east-1"; RW_USER="duckhog-vcpkg-cache-rw"; RO_USER="duckhog-vcpkg-cache-ro"
```

```bash
aws iam get-user --user-name "$RW_USER" >/dev/null 2>&1 || aws iam create-user --user-name "$RW_USER"
```

```bash
aws iam get-user --user-name "$RO_USER" >/dev/null 2>&1 || aws iam create-user --user-name "$RO_USER"
```

```bash
cat > /tmp/vcpkg-rw-policy.json <<EOF
{
  "Version":"2012-10-17",
  "Statement":[
    {
      "Effect":"Allow",
      "Action":["s3:ListBucket"],
      "Resource":"arn:aws:s3:::$BUCKET",
      "Condition":{"StringLike":{"s3:prefix":["$PREFIX/*"]}}
    },
    {
      "Effect":"Allow",
      "Action":["s3:GetObject","s3:PutObject","s3:DeleteObject"],
      "Resource":"arn:aws:s3:::$BUCKET/$PREFIX/*"
    }
  ]
}
EOF
```

```bash
cat > /tmp/vcpkg-ro-policy.json <<EOF
{
  "Version":"2012-10-17",
  "Statement":[
    {
      "Effect":"Allow",
      "Action":["s3:ListBucket"],
      "Resource":"arn:aws:s3:::$BUCKET",
      "Condition":{"StringLike":{"s3:prefix":["$PREFIX/*"]}}
    },
    {
      "Effect":"Allow",
      "Action":["s3:GetObject"],
      "Resource":"arn:aws:s3:::$BUCKET/$PREFIX/*"
    }
  ]
}
EOF
```

```bash
aws iam put-user-policy --user-name "$RW_USER" --policy-name vcpkg-cache-rw --policy-document file:///tmp/vcpkg-rw-policy.json
```

```bash
aws iam put-user-policy --user-name "$RO_USER" --policy-name vcpkg-cache-ro --policy-document file:///tmp/vcpkg-ro-policy.json
```

```bash
aws iam create-access-key --user-name "$RW_USER"
```

Copy `AccessKeyId` and `SecretAccessKey` from the previous output, then run:

```bash
gh secret set VCPKG_CACHING_RW_AWS_ACCESS_KEY_ID -R "$REPO" -b '<RW_ACCESS_KEY_ID>'
```

```bash
gh secret set VCPKG_CACHING_RW_AWS_SECRET_ACCESS_KEY -R "$REPO" -b '<RW_SECRET_ACCESS_KEY>'
```

```bash
aws iam create-access-key --user-name "$RO_USER"
```

Copy `AccessKeyId` and `SecretAccessKey` from the previous output, then run:

```bash
gh secret set VCPKG_CACHING_RO_AWS_ACCESS_KEY_ID -R "$REPO" -b '<RO_ACCESS_KEY_ID>'
```

```bash
gh secret set VCPKG_CACHING_RO_AWS_SECRET_ACCESS_KEY -R "$REPO" -b '<RO_SECRET_ACCESS_KEY>'
```

```bash
gh secret set VCPKG_CACHING_AWS_DEFAULT_REGION -R "$REPO" -b "$REGION"
```

For AWS S3, skip endpoint URL. For S3-compatible endpoints, set it:

```bash
gh secret set VCPKG_CACHING_AWS_ENDPOINT_URL -R "$REPO" -b '<ENDPOINT_URL>'
```

```bash
gh secret list -R "$REPO"
```

If key creation fails with key limit exceeded, list and remove old keys first:

```bash
aws iam list-access-keys --user-name "$RW_USER"
```

```bash
aws iam list-access-keys --user-name "$RO_USER"
```

## 5. Configure workflow cache mode by event

Use separate jobs so only `push` to `main` uses read-write mode:

```yaml
duckdb-stable-build-main:
  if: ${{ github.event_name == 'push' && github.ref == 'refs/heads/main' }}
  permissions:
    contents: read
  uses: duckdb/extension-ci-tools/.github/workflows/_extension_distribution.yml@v1.4.3
  with:
    duckdb_version: v1.4.3
    ci_tools_version: v1.4.3
    extension_name: duckhog
    vcpkg_binary_sources: clear;x-aws,s3://duckhog-vcpkg-cache/duckhog/,readwrite;http,https://vcpkg-cache.duckdb.org,read
  secrets:
    VCPKG_CACHING_AWS_ACCESS_KEY_ID: ${{ secrets.VCPKG_CACHING_RW_AWS_ACCESS_KEY_ID }}
    VCPKG_CACHING_AWS_SECRET_ACCESS_KEY: ${{ secrets.VCPKG_CACHING_RW_AWS_SECRET_ACCESS_KEY }}
    VCPKG_CACHING_AWS_ENDPOINT_URL: ${{ secrets.VCPKG_CACHING_AWS_ENDPOINT_URL }}
    VCPKG_CACHING_AWS_DEFAULT_REGION: ${{ secrets.VCPKG_CACHING_AWS_DEFAULT_REGION }}

duckdb-stable-build-pr:
  if: ${{ github.event_name == 'pull_request' }}
  permissions:
    contents: read
  uses: duckdb/extension-ci-tools/.github/workflows/_extension_distribution.yml@v1.4.3
  with:
    duckdb_version: v1.4.3
    ci_tools_version: v1.4.3
    extension_name: duckhog
    vcpkg_binary_sources: clear;x-aws,s3://duckhog-vcpkg-cache/duckhog/,read;http,https://vcpkg-cache.duckdb.org,read
  secrets:
    VCPKG_CACHING_AWS_ACCESS_KEY_ID: ${{ secrets.VCPKG_CACHING_RO_AWS_ACCESS_KEY_ID }}
    VCPKG_CACHING_AWS_SECRET_ACCESS_KEY: ${{ secrets.VCPKG_CACHING_RO_AWS_SECRET_ACCESS_KEY }}
    VCPKG_CACHING_AWS_ENDPOINT_URL: ${{ secrets.VCPKG_CACHING_AWS_ENDPOINT_URL }}
    VCPKG_CACHING_AWS_DEFAULT_REGION: ${{ secrets.VCPKG_CACHING_AWS_DEFAULT_REGION }}
```

### 5.1 Configure `tidy-check` to prefer private cache

Set `tidy-check` environment so:

- `main` push uses private cache in `readwrite` mode with RW credentials.
- same-repo PR uses private cache in `read` mode with RO credentials.
- fork PR falls back to public read-only cache with no secrets.

```yaml
tidy-check:
  env:
    AWS_ACCESS_KEY_ID: ${{ github.event_name == 'push' && github.ref == 'refs/heads/main' && secrets.VCPKG_CACHING_RW_AWS_ACCESS_KEY_ID || github.event_name == 'pull_request' && github.event.pull_request.head.repo.full_name == github.repository && secrets.VCPKG_CACHING_RO_AWS_ACCESS_KEY_ID || '' }}
    AWS_SECRET_ACCESS_KEY: ${{ github.event_name == 'push' && github.ref == 'refs/heads/main' && secrets.VCPKG_CACHING_RW_AWS_SECRET_ACCESS_KEY || github.event_name == 'pull_request' && github.event.pull_request.head.repo.full_name == github.repository && secrets.VCPKG_CACHING_RO_AWS_SECRET_ACCESS_KEY || '' }}
    AWS_DEFAULT_REGION: ${{ github.event_name == 'push' && github.ref == 'refs/heads/main' && secrets.VCPKG_CACHING_AWS_DEFAULT_REGION || github.event_name == 'pull_request' && github.event.pull_request.head.repo.full_name == github.repository && secrets.VCPKG_CACHING_AWS_DEFAULT_REGION || '' }}
    AWS_ENDPOINT_URL: ${{ github.event_name == 'push' && github.ref == 'refs/heads/main' && secrets.VCPKG_CACHING_AWS_ENDPOINT_URL || github.event_name == 'pull_request' && github.event.pull_request.head.repo.full_name == github.repository && secrets.VCPKG_CACHING_AWS_ENDPOINT_URL || '' }}
    AWS_REQUEST_CHECKSUM_CALCULATION: when_required
    VCPKG_BINARY_SOURCES: ${{ github.event_name == 'push' && github.ref == 'refs/heads/main' && 'clear;x-aws,s3://duckhog-vcpkg-cache/duckhog/,readwrite;http,https://vcpkg-cache.duckdb.org,read' || github.event_name == 'pull_request' && github.event.pull_request.head.repo.full_name == github.repository && 'clear;x-aws,s3://duckhog-vcpkg-cache/duckhog/,read;http,https://vcpkg-cache.duckdb.org,read' || 'clear;http,https://vcpkg-cache.duckdb.org,read' }}
```

## 6. Keep ABI keys stable

Cache hits require ABI match. Keep these stable where possible:

- `VCPKG_TARGET_TRIPLET` and `VCPKG_HOST_TRIPLET`
- compiler and runner image
- `vcpkg-configuration.json` baseline
- overlay ports/triplets
- build flags that affect ABI

## 7. Verification checklist

### PR run checklist

- Confirm PR job uses `x-aws,...,read` (not `readwrite`).
- Confirm PR job receives RO credentials only.
- Confirm no `s3:PutObject` or `s3:DeleteObject` permissions exist for RO user.

### Push-to-main checklist

- Confirm main job uses `x-aws,...,readwrite`.
- Confirm main job receives RW credentials.
- First run may build and upload.
- Subsequent run should show `Restored N package(s)` with `N > 0` (when ABI unchanged).

## 8. Operations and security hygiene

- Rotate both RW and RO keys regularly.
- Keep at most one active key pair per user when possible.
- Keep branch protection enabled on `main`.
- Review IAM user policies and bucket settings periodically.
- Use bucket lifecycle rules to control storage growth.

## 9. Important note about `x-gha`

Do not use `x-gha`. It was removed in vcpkg (June 2025). Use `x-aws`, `nuget`, or `files`.

## References

- https://learn.microsoft.com/en-us/vcpkg/reference/binarycaching
- https://learn.microsoft.com/en-us/vcpkg/users/binarycaching-troubleshooting
- https://devblogs.microsoft.com/cppblog/whats-new-in-vcpkg-june-2025/
