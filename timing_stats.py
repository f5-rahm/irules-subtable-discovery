#!/usr/bin/env python3
"""
Parse LOCALDB sampled timing log lines and report per-TMM stats.

Reads from stdin (or files passed as arguments). Looks for lines
emitted by LOCALDB::set when debug_sample > 0, of the form:

    ... tmm=<N> subtable=<name> writes=<W> key=<K> clicks=<C> FAST_LOCAL|SLOW (sampled 1/<R>)

Reports per-TMM sample count, min/avg/max/percentile click timings,
and the FAST_LOCAL vs SLOW tag tally — a quick way to see whether
every TMM is writing to a locally-owned subtable.

Usage:
    ssh root@<bigip> "grep 'sampled' /var/log/ltm" | python3 timing_stats.py
    python3 timing_stats.py /path/to/ltm.log
    python3 timing_stats.py --threshold 200 ltm.log    # custom SLOW threshold
"""

import argparse
import re
import statistics
import sys
from collections import defaultdict

TMM_RE = re.compile(r"tmm=(\d+)")
CLICKS_RE = re.compile(r"clicks=(\d+)")
TAG_RE = re.compile(r"\b(FAST_LOCAL|SLOW)\b")
SAMPLE_RE = re.compile(r"sampled 1/(\d+)")


def parse_lines(lines, require_sampled=True):
    """
    Yield dicts for each line that contains the expected LOCALDB sample fields.
    Lines without 'sampled' are skipped when require_sampled=True (default).
    """
    for line in lines:
        if require_sampled and "sampled" not in line:
            continue
        t = TMM_RE.search(line)
        c = CLICKS_RE.search(line)
        if not (t and c):
            continue
        tag = TAG_RE.search(line)
        sample = SAMPLE_RE.search(line)
        yield {
            "tmm": int(t.group(1)),
            "clicks": int(c.group(1)),
            "tag": tag.group(1) if tag else None,
            "sample_rate": int(sample.group(1)) if sample else None,
        }


def percentile(sorted_values, p):
    """Index-based percentile from a pre-sorted list. p is 0-100."""
    if not sorted_values:
        return 0
    n = len(sorted_values)
    idx = min(n - 1, int(n * p / 100))
    return sorted_values[idx]


def main():
    parser = argparse.ArgumentParser(
        description=(
            "Parse LOCALDB sampled timing log lines and report per-TMM stats."
        ),
        epilog=(
            "Reads from stdin if no files given. Typical usage:\n"
            "  ssh root@<bigip> \"grep 'sampled' /var/log/ltm\" | %(prog)s\n"
            "  %(prog)s /var/log/ltm\n"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("files", nargs="*",
                        help="Log file(s) to read; reads stdin if omitted")
    parser.add_argument("--threshold", type=int, default=100,
                        help="Click threshold below which a write is "
                             "considered owner-local (default: 100; "
                             "should match static::LOCALDB_fast_threshold "
                             "in the iRule)")
    parser.add_argument("--show-slow", action="store_true",
                        help="List the slowest 10 samples per TMM "
                             "(useful for outlier inspection)")
    parser.add_argument("--all-lines", action="store_true",
                        help="Don't require 'sampled' in the line; parse "
                             "any line with tmm= and clicks= (use to "
                             "include /probe diagnostic output)")
    args = parser.parse_args()

    # Source: stdin or files
    if args.files:
        def line_source():
            for path in args.files:
                with open(path) as f:
                    for line in f:
                        yield line
    else:
        def line_source():
            for line in sys.stdin:
                yield line

    # Aggregate per TMM
    samples = defaultdict(list)
    tags = defaultdict(lambda: {"FAST_LOCAL": 0, "SLOW": 0, "untagged": 0})
    sample_rates = set()

    for rec in parse_lines(line_source(), require_sampled=not args.all_lines):
        samples[rec["tmm"]].append(rec["clicks"])
        if rec["tag"] in ("FAST_LOCAL", "SLOW"):
            tags[rec["tmm"]][rec["tag"]] += 1
        else:
            tags[rec["tmm"]]["untagged"] += 1
        if rec["sample_rate"] is not None:
            sample_rates.add(rec["sample_rate"])

    if not samples:
        print("No matching log lines found.", file=sys.stderr)
        print("(Looking for lines containing 'sampled' with tmm= and clicks=. "
              "Use --all-lines to relax that.)", file=sys.stderr)
        sys.exit(1)

    # Header context
    if sample_rates:
        rate_desc = (f"1/{next(iter(sample_rates))}"
                     if len(sample_rates) == 1
                     else f"mixed: {sorted(sample_rates)}")
        print(f"Sample rate: {rate_desc}   "
              f"Locality threshold: {args.threshold} clicks\n")
    else:
        print(f"Locality threshold: {args.threshold} clicks\n")

    # Main per-TMM table
    print(f"{'TMM':>5} {'n':>6} {'FAST':>6} {'SLOW':>6} "
          f"{'min':>7} {'p50':>7} {'avg':>9} {'p95':>7} {'p99':>7} {'max':>9}")
    print("-" * 78)

    grand_total = 0
    grand_fast = 0
    grand_slow = 0
    for tmm in sorted(samples):
        s = sorted(samples[tmm])
        n = len(s)
        avg = statistics.mean(s)
        p50 = percentile(s, 50)
        p95 = percentile(s, 95)
        p99 = percentile(s, 99)
        t = tags[tmm]
        fast = t["FAST_LOCAL"]
        slow = t["SLOW"]
        grand_total += n
        grand_fast += fast
        grand_slow += slow
        print(f"{tmm:>5} {n:>6} {fast:>6} {slow:>6} "
              f"{min(s):>7} {p50:>7} {avg:>9.1f} "
              f"{p95:>7} {p99:>7} {max(s):>9}")

    # Summary
    print("-" * 78)
    print(f"Total: {grand_total} samples across {len(samples)} TMMs   "
          f"FAST_LOCAL={grand_fast}  SLOW={grand_slow}")

    # Verdict
    print()
    bad_tmms = []
    for tmm in sorted(samples):
        s = samples[tmm]
        avg = sum(s) / len(s)
        if avg >= args.threshold:
            bad_tmms.append((tmm, avg))

    if bad_tmms:
        print("LOCALITY ISSUE: the following TMMs have average write timing "
              "above threshold")
        print("(meaning they are NOT writing to a locally-owned subtable):")
        for tmm, avg in bad_tmms:
            print(f"  tmm={tmm} avg={avg:.1f} clicks (threshold {args.threshold})")
        print()
        print("Possible causes:")
        print("  1. init_table is using a deterministic name scheme instead "
              "of timing-probe discovery")
        print("  2. init_table's maxtry is too low for this TMM count "
              "(check for WARNING in /var/log/ltm)")
        print("  3. fast_threshold in the iRule is set too low for this hardware")
    else:
        print(f"OK: all TMMs have average write timing below {args.threshold} "
              f"clicks. Per-TMM locality is working.")

    # Slow-sample drill-down
    if args.show_slow:
        print("\nSlowest 10 samples per TMM:")
        for tmm in sorted(samples):
            s = sorted(samples[tmm], reverse=True)[:10]
            print(f"  tmm={tmm}: {s}")


if __name__ == "__main__":
    main()
