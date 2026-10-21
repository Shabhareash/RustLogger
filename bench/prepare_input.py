#!/usr/bin/env python3
"""Build a 1M-line ISO-timestamp auth log from the bundled OpenSSH sample.

The original auth_1m.log is not in the repo. This keeps the benchmark
reproducible from OpenSSH_200k.log while giving both engines the same
ISO syslog input the YAML configs expect.
"""

import argparse
import os
import re

MONTHS = {
    "Jan": 1, "Feb": 2, "Mar": 3, "Apr": 4, "May": 5, "Jun": 6,
    "Jul": 7, "Aug": 8, "Sep": 9, "Oct": 10, "Nov": 11, "Dec": 12,
}
BSD = re.compile(
    r"^(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+"
    r"(\d{1,2})\s+(\d{2}):(\d{2}):(\d{2})\s+(.*)$"
)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("source")
    parser.add_argument("output")
    parser.add_argument("--lines", type=int, default=1_000_000)
    parser.add_argument("--year", type=int, default=2026)
    args = parser.parse_args()

    clean = []
    skipped = 0
    with open(args.source, "r", errors="replace") as fh:
        for line in fh:
            line = line.rstrip("\n").rstrip("\r")
            match = BSD.match(line)
            if match is None:
                skipped += 1
                continue
            mon, day, hh, mm, ss, rest = match.groups()
            stamp = (
                f"{args.year:04d}-{MONTHS[mon]:02d}-{int(day):02d}"
                f"T{hh}:{mm}:{ss}.000000+00:00"
            )
            clean.append(f"{stamp} {rest}")

    if not clean:
        raise SystemExit(f"no syslog lines found in {args.source}")

    os.makedirs(os.path.dirname(os.path.abspath(args.output)), exist_ok=True)
    with open(args.output, "w") as fh:
        for i in range(args.lines):
            fh.write(clean[i % len(clean)])
            fh.write("\n")

    size_mb = os.path.getsize(args.output) / 1e6
    print(
        f"wrote {args.lines} lines to {args.output} "
        f"({size_mb:.1f} MB, {len(clean)} unique source lines, {skipped} skipped)"
    )


if __name__ == "__main__":
    main()
