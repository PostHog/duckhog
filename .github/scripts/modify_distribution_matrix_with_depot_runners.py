#!/usr/bin/env python3
"""Wrap extension-ci-tools matrix generation and pin Linux runners to Depot labels."""

import argparse
import json
import pathlib
import subprocess
import sys


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Call extension-ci-tools matrix parser and override Linux runner labels."
    )
    parser.add_argument("--select_os", default="", help="OS matrix being generated")
    parser.add_argument("--output", default="", help="Output JSON path")
    args, unknown = parser.parse_known_args()
    args.forwarded_args = unknown
    return args


def run_base_parser(args: argparse.Namespace) -> None:
    cmd = [
        "python3",
        "extension-ci-tools/scripts/modify_distribution_matrix.py",
        "--select_os",
        args.select_os,
        "--output",
        args.output,
        *args.forwarded_args,
    ]
    subprocess.check_call(cmd)


def override_linux_runners(output_path: str) -> None:
    if not output_path:
        return

    matrix_file = pathlib.Path(output_path)
    matrix = json.loads(matrix_file.read_text())
    include_entries = matrix.get("include", [])

    for entry in include_entries:
        arch = entry.get("duckdb_arch")
        if arch in {"linux_amd64", "linux_amd64_musl"}:
            entry["runner"] = ["self-hosted", "depot-ubuntu-24.04-64"]
        elif arch == "linux_arm64":
            entry["runner"] = ["self-hosted", "depot-ubuntu-24.04-arm"]

    matrix_file.write_text(json.dumps(matrix, indent=2))


def main() -> int:
    args = parse_args()
    run_base_parser(args)

    if args.select_os == "linux":
        override_linux_runners(args.output)

    return 0


if __name__ == "__main__":
    sys.exit(main())
