#!/usr/bin/env python3
"""
TMM subtable loader, prober, and resetter for the LOCALDB iRule.

Modes:
  load       Distribute writes across all TMMs (one write per HTTP req).
             Source can be:
               --count N           generate N synthetic key/value pairs
               --file PATH         read key,value lines from a file
               (neither)           coverage-driven: loop until every TMM
                                   has --min-writes-per-tmm writes
             Reports per-TMM write distribution.

  bulk-post  Emulate batched HTTP POST loading. Generates synthetic
             UUIDv4 batches and POSTs each to /bulk_load. One HTTP
             request writes many keys, exercising body parsing and
             tight-loop write throughput rather than per-connection
             overhead. Defaults sized for a 4-TMM box; override
             --num-posts and --uuids-per-post for other configs.

  probe      Hit /probe many times for a single shared subtable name
             to identify which TMM owns it. Reports per-TMM hits,
             OWNER / NON_OWNER tag tallies, and click timing stats.

  reset      Hit /reset many times so it fans out across all TMMs,
             clearing each TMM's local subtable. Reports per-TMM
             deletion counts.

All modes call /info first to discover the TMM count via TMM::cmp_count.

Companion to:
  - /Common/LOCALDB                proc library
  - /Common/subtable_test_updates  calling iRule (/info, /load, /bulk_load,
                                   /dump, /probe, /reset, /whoami endpoints)
"""

import argparse
import csv
import re
import socket
import sys
import threading
import time
import uuid
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed

# Response-body parsing patterns
TMM_RE = re.compile(rb"tmm=(\d+)")
TOTAL_RE = re.compile(rb"total_tmms=(\d+)")
CLICKS_RE = re.compile(rb"clicks=(\d+)")
TAG_RE = re.compile(rb"\b(OWNER|NON_OWNER)\b")
DELETED_RE = re.compile(rb"deleted=(\d+)")
WRITTEN_RE = re.compile(rb"written=(\d+)")
ELAPSED_RE = re.compile(rb"elapsed_clicks=(\d+)")
PER_WRITE_RE = re.compile(rb"clicks_per_write=(\d+)")


# ---------- HTTP helpers ----------

def http_get(host, port, path, timeout=5.0):
    """
    Open a fresh TCP connection, send minimal HTTP/1.1 with
    Connection: close, return raw response bytes.

    Fresh socket = fresh source port = fresh DAG hash. This is what
    spreads requests across TMMs. Do NOT add keep-alive here.
    """
    req = (
        f"GET {path} HTTP/1.1\r\n"
        f"Host: {host}\r\n"
        f"Connection: close\r\n"
        f"\r\n"
    ).encode()
    with socket.create_connection((host, port), timeout=timeout) as s:
        s.sendall(req)
        chunks = []
        while True:
            buf = s.recv(4096)
            if not buf:
                break
            chunks.append(buf)
    return b"".join(chunks)


def http_post(host, port, path, body_bytes, timeout=30.0):
    """
    POST a body to path with Content-Length set explicitly.
    Returns the full response (headers + body).

    The iRule's /bulk_load handler requires Content-Length and does
    not support chunked transfer encoding. Body should be raw bytes.
    Timeout is generous because large bodies can take a while to
    upload + write on the BIG-IP side.
    """
    headers = (
        f"POST {path} HTTP/1.1\r\n"
        f"Host: {host}\r\n"
        f"Connection: close\r\n"
        f"Content-Type: text/plain\r\n"
        f"Content-Length: {len(body_bytes)}\r\n"
        f"\r\n"
    ).encode()
    with socket.create_connection((host, port), timeout=timeout) as s:
        s.sendall(headers)
        s.sendall(body_bytes)
        chunks = []
        while True:
            buf = s.recv(8192)
            if not buf:
                break
            chunks.append(buf)
    return b"".join(chunks)


def discover_tmm_count(host, port):
    body = http_get(host, port, "/info")
    m = TOTAL_RE.search(body)
    if not m:
        raise RuntimeError(f"could not parse total_tmms from /info response: {body!r}")
    return int(m.group(1))


def call_load(host, port, key, val):
    body = http_get(host, port, f"/load?key={key}&val={val}")
    tmm_m = TMM_RE.search(body)
    return int(tmm_m.group(1)) if tmm_m else None


def call_bulk_post(host, port, body_bytes):
    """
    POST a batch of newline-separated keys to /bulk_load.
    Returns dict with tmm, written count, total elapsed clicks (server-
    side write loop only), and per-write average clicks.
    """
    resp = http_post(host, port, "/bulk_load", body_bytes)
    tmm_m = TMM_RE.search(resp)
    written_m = WRITTEN_RE.search(resp)
    elapsed_m = ELAPSED_RE.search(resp)
    per_write_m = PER_WRITE_RE.search(resp)
    return {
        "tmm": int(tmm_m.group(1)) if tmm_m else None,
        "written": int(written_m.group(1)) if written_m else None,
        "elapsed_clicks": int(elapsed_m.group(1)) if elapsed_m else None,
        "clicks_per_write": int(per_write_m.group(1)) if per_write_m else None,
    }


def call_probe(host, port, name):
    body = http_get(host, port, f"/probe?name={name}")
    tmm_m = TMM_RE.search(body)
    clicks_m = CLICKS_RE.search(body)
    tag_m = TAG_RE.search(body)
    return {
        "tmm": int(tmm_m.group(1)) if tmm_m else None,
        "clicks": int(clicks_m.group(1)) if clicks_m else None,
        "tag": tag_m.group(1).decode() if tag_m else None,
    }


def call_reset(host, port):
    body = http_get(host, port, "/reset")
    tmm_m = TMM_RE.search(body)
    del_m = DELETED_RE.search(body)
    return (
        int(tmm_m.group(1)) if tmm_m else None,
        int(del_m.group(1)) if del_m else None,
    )


# ---------- record sources for load mode ----------

def synthetic_records(count, key_prefix):
    """Generate (key, val) pairs for synthetic loads."""
    for i in range(1, count + 1):
        yield (f"{key_prefix}_{i}", str(i))


def file_records(path):
    """
    Read key,value pairs from a CSV file.

    Format: two columns (key,value), one record per line.
    Lines starting with # are ignored. A header row is auto-detected
    if the first non-comment line is literally 'key,value'.
    """
    with open(path, newline="") as f:
        reader = csv.reader(f)
        first = True
        for row in reader:
            if not row or row[0].startswith("#"):
                continue
            if first:
                first = False
                if (len(row) >= 2
                        and row[0].strip().lower() == "key"
                        and row[1].strip().lower() == "value"):
                    continue  # skip header
            if len(row) < 2:
                raise ValueError(
                    f"malformed line in {path}: {row!r} (expected key,value)")
            yield (row[0], row[1])


# ---------- load mode ----------

def cmd_load(args):
    print(f"Discovering TMM count from {args.host}:{args.port}/info ...")
    total_tmms = discover_tmm_count(args.host, args.port)
    print(f"BIG-IP reports {total_tmms} TMMs.")

    # Pick a record source
    if args.count is not None:
        source_desc = f"synthetic ({args.count:,} records, prefix='{args.key_prefix}')"
        records = synthetic_records(args.count, args.key_prefix)
        bound = args.count
    elif args.file is not None:
        source_desc = f"file ({args.file})"
        records = file_records(args.file)
        bound = None
    else:
        source_desc = (f"coverage-driven (until every TMM has "
                       f">= {args.min_writes_per_tmm} writes, "
                       f"max {args.max_connections})")
        records = synthetic_records(args.max_connections, args.key_prefix)
        bound = args.max_connections

    print(f"Source: {source_desc}")
    print(f"Workers: {args.workers}\n")

    hits = defaultdict(int)
    hits_lock = threading.Lock()
    sent = 0
    completed = 0
    errors = 0
    start = time.time()

    def covered():
        # Coverage-driven early-exit only applies when no explicit source given
        if args.count is not None or args.file is not None:
            return False
        with hits_lock:
            if len(hits) < total_tmms:
                return False
            return all(v >= args.min_writes_per_tmm for v in hits.values())

    record_iter = iter(records)

    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        in_flight = {}

        def submit_next():
            try:
                key, val = next(record_iter)
            except StopIteration:
                return False
            fut = ex.submit(call_load, args.host, args.port, key, val)
            in_flight[fut] = (key, val)
            return True

        # Prime the pool
        for _ in range(args.workers):
            if not submit_next():
                break
            sent += 1

        progress_every = max(1, (bound or 10000) // 50) if bound else 1000

        while in_flight:
            done = next(as_completed(in_flight))
            in_flight.pop(done)
            completed += 1
            try:
                tmm = done.result()
                if tmm is not None:
                    with hits_lock:
                        hits[tmm] += 1
                else:
                    errors += 1
            except Exception as e:
                errors += 1
                if errors <= 5:
                    print(f"connection error: {e}", file=sys.stderr)
                elif errors == 6:
                    print("(suppressing further connection errors)",
                          file=sys.stderr)

            if completed % progress_every == 0:
                elapsed = time.time() - start
                rate = completed / elapsed if elapsed > 0 else 0
                with hits_lock:
                    snap = dict(sorted(hits.items()))
                missing = sorted(set(range(total_tmms)) - set(snap.keys()))
                if bound:
                    pct = 100 * completed / bound
                    print(f"completed={completed:,}/{bound:,} ({pct:.1f}%) "
                          f"rate={rate:.0f}/s coverage={len(snap)}/{total_tmms} "
                          f"missing={missing} errors={errors}")
                else:
                    print(f"completed={completed:,} rate={rate:.0f}/s "
                          f"coverage={len(snap)}/{total_tmms} "
                          f"missing={missing} errors={errors}")

            # Coverage early-exit (only in coverage mode)
            if covered():
                print(f"\nFull coverage reached after {completed:,} writes.")
                break

            # Queue next record (or drain if source exhausted)
            if not submit_next():
                continue
            sent += 1

    elapsed = time.time() - start
    rate = completed / elapsed if elapsed > 0 else 0
    print(f"\nDone. completed={completed:,} errors={errors} "
          f"elapsed={elapsed:.1f}s rate={rate:.0f}/s")
    print("\nFinal distribution:")
    with hits_lock:
        total_writes = sum(hits.values())
        for tmm in sorted(hits):
            pct = 100 * hits[tmm] / total_writes if total_writes else 0
            print(f"  tmm{tmm:>3}: {hits[tmm]:>10,} writes ({pct:5.2f}%)")
        missing = set(range(total_tmms)) - set(hits.keys())
        if missing:
            print(f"  never reached: {sorted(missing)}")

    print("\nFor per-TMM timing stats, pull sampled lines from /var/log/ltm:")
    print("  ssh root@<bigip> \"grep 'sampled' /var/log/ltm\" | python3 timing_stats.py")


# ---------- bulk-post mode ----------

def make_uuid_batch(n):
    """
    Build a batch body of n unique UUIDv4 strings, one per line,
    terminated with \n. Returns bytes ready to POST.
    """
    # uuid.uuid4() draws from os.urandom — globally unique across batches
    # in this process. No per-batch seeding needed.
    lines = (str(uuid.uuid4()) for _ in range(n))
    return ("\n".join(lines) + "\n").encode("ascii")


def cmd_bulk_post(args):
    print(f"Discovering TMM count from {args.host}:{args.port}/info ...")
    total_tmms = discover_tmm_count(args.host, args.port)
    print(f"BIG-IP reports {total_tmms} TMMs.")

    total_uuids = args.num_posts * args.uuids_per_post
    print(f"Plan: {args.num_posts:,} POSTs × {args.uuids_per_post:,} "
          f"UUIDs each = {total_uuids:,} total writes")
    body_bytes_per_post = args.uuids_per_post * 37  # 36 chars + \n
    print(f"Per-POST body size: ~{body_bytes_per_post:,} bytes "
          f"(~{body_bytes_per_post / 1024:.1f} KiB)")
    print(f"Workers: {args.workers}\n")

    # Per-TMM aggregates
    per_tmm = defaultdict(lambda: {
        "posts": 0,
        "writes": 0,
        "total_elapsed_clicks": 0,
        "per_write_clicks": [],  # samples for distribution stats
    })
    lock = threading.Lock()

    completed = 0
    errors = 0
    start = time.time()

    def submit_one(executor):
        # Generate the UUID batch in the worker, not the main thread,
        # so the main thread isn't a CPU bottleneck on large runs.
        return executor.submit(_post_one, args.host, args.port, args.uuids_per_post)

    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        in_flight = set()
        # Prime the pool
        prime = min(args.workers, args.num_posts)
        for _ in range(prime):
            in_flight.add(submit_one(ex))
        sent = prime

        while in_flight:
            done = next(as_completed(in_flight))
            in_flight.remove(done)
            completed += 1

            try:
                r = done.result()
            except Exception as e:
                errors += 1
                if errors <= 5:
                    print(f"connection error: {e}", file=sys.stderr)
                elif errors == 6:
                    print("(suppressing further connection errors)",
                          file=sys.stderr)
                r = None

            if r and r["tmm"] is not None and r["written"] is not None:
                with lock:
                    b = per_tmm[r["tmm"]]
                    b["posts"] += 1
                    b["writes"] += r["written"]
                    if r["elapsed_clicks"] is not None:
                        b["total_elapsed_clicks"] += r["elapsed_clicks"]
                    if r["clicks_per_write"] is not None:
                        b["per_write_clicks"].append(r["clicks_per_write"])

            # Progress every 5% or every 10 posts, whichever larger
            progress_every = max(10, args.num_posts // 20)
            if completed % progress_every == 0 or completed == args.num_posts:
                elapsed = time.time() - start
                rate = completed / elapsed if elapsed > 0 else 0
                writes_done = sum(b["writes"] for b in per_tmm.values())
                write_rate = writes_done / elapsed if elapsed > 0 else 0
                print(f"completed={completed:,}/{args.num_posts:,} "
                      f"posts_per_s={rate:.1f} "
                      f"writes={writes_done:,} ({write_rate:,.0f}/s) "
                      f"errors={errors}")

            if sent < args.num_posts:
                in_flight.add(submit_one(ex))
                sent += 1

    elapsed = time.time() - start
    total_writes = sum(b["writes"] for b in per_tmm.values())
    print(f"\nDone. posts={completed:,} writes={total_writes:,} "
          f"errors={errors} elapsed={elapsed:.1f}s")
    if elapsed > 0:
        print(f"Throughput: {completed / elapsed:.1f} POSTs/s, "
              f"{total_writes / elapsed:,.0f} writes/s")

    print("\nPer-TMM distribution:")
    print(f"{'TMM':>5} {'posts':>7} {'writes':>10} {'pct':>7} "
          f"{'avg_per_write':>15} {'min':>5} {'max':>7}")
    print("-" * 60)
    for tmm in sorted(per_tmm):
        b = per_tmm[tmm]
        pct = 100 * b["writes"] / total_writes if total_writes else 0
        cps = b["per_write_clicks"]
        if cps:
            avg = sum(cps) / len(cps)
            print(f"{tmm:>5} {b['posts']:>7} {b['writes']:>10,} "
                  f"{pct:>6.2f}% {avg:>15.1f} {min(cps):>5} {max(cps):>7}")
        else:
            print(f"{tmm:>5} {b['posts']:>7} {b['writes']:>10,} "
                  f"{pct:>6.2f}% {'-':>15} {'-':>5} {'-':>7}")

    missing = sorted(set(range(total_tmms)) - set(per_tmm.keys()))
    if missing:
        print(f"\nWARNING: TMMs not reached: {missing}.")

    # Sanity-check the per-write timing — flag if any TMM looks slow
    print()
    threshold = 100  # match static::LOCALDB_fast_threshold
    bad = []
    for tmm in sorted(per_tmm):
        cps = per_tmm[tmm]["per_write_clicks"]
        if cps:
            avg = sum(cps) / len(cps)
            if avg >= threshold:
                bad.append((tmm, avg))
    if bad:
        print("LOCALITY WARNING: the following TMMs have avg per-write timing "
              f">= {threshold} clicks:")
        for tmm, avg in bad:
            print(f"  tmm={tmm} avg={avg:.1f} clicks/write")
        print("Run 'tbl-loader.py probe' against the discovered subtable names "
              "to investigate.")
    else:
        print(f"OK: all TMMs averaged < {threshold} clicks per write. "
              f"Locality holding under bulk load.")


def _post_one(host, port, n):
    """Worker function: build a UUID batch and POST it."""
    body = make_uuid_batch(n)
    return call_bulk_post(host, port, body)


# ---------- probe mode ----------

def cmd_probe(args):
    print(f"Discovering TMM count from {args.host}:{args.port}/info ...")
    total_tmms = discover_tmm_count(args.host, args.port)
    print(f"BIG-IP reports {total_tmms} TMMs.")
    print(f"Probing subtable name '{args.name}' with {args.requests} "
          f"requests ({args.workers} workers)...\n")

    per_tmm = defaultdict(lambda: {
        "hits": 0, "clicks": [], "owner": 0, "non_owner": 0,
    })
    lock = threading.Lock()

    def submit_one(executor):
        return executor.submit(call_probe, args.host, args.port, args.name)

    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        in_flight = set()
        sent = 0
        prime = min(args.workers, args.requests)
        for _ in range(prime):
            in_flight.add(submit_one(ex))
            sent += 1

        completed = 0
        while in_flight:
            done = next(as_completed(in_flight))
            in_flight.remove(done)
            completed += 1
            try:
                r = done.result()
            except Exception as e:
                print(f"connection error: {e}", file=sys.stderr)
                r = None

            if r and r["tmm"] is not None:
                with lock:
                    bucket = per_tmm[r["tmm"]]
                    bucket["hits"] += 1
                    if r["clicks"] is not None:
                        bucket["clicks"].append(r["clicks"])
                    if r["tag"] == "OWNER":
                        bucket["owner"] += 1
                    elif r["tag"] == "NON_OWNER":
                        bucket["non_owner"] += 1

            if completed % 50 == 0:
                print(f"completed={completed}/{args.requests}")

            if sent < args.requests:
                in_flight.add(submit_one(ex))
                sent += 1

    print(f"\nResults for subtable '{args.name}':")
    print(f"{'TMM':>5} {'hits':>6} {'OWNER':>6} {'NON_OWNER':>10} "
          f"{'min_clicks':>11} {'avg_clicks':>11} {'max_clicks':>11}")
    print("-" * 64)

    likely_owner = None
    best_avg = None
    for tmm in sorted(per_tmm.keys()):
        b = per_tmm[tmm]
        clicks = b["clicks"]
        if clicks:
            cmin = min(clicks)
            cmax = max(clicks)
            cavg = sum(clicks) / len(clicks)
        else:
            cmin = cmax = cavg = 0
        print(f"{tmm:>5} {b['hits']:>6} {b['owner']:>6} {b['non_owner']:>10} "
              f"{cmin:>11} {cavg:>11.1f} {cmax:>11}")
        if b["owner"] > 0 and (best_avg is None or cavg < best_avg):
            best_avg = cavg
            likely_owner = tmm

    missing = sorted(set(range(total_tmms)) - set(per_tmm.keys()))
    if missing:
        print(f"\nTMMs never reached during probe: {missing}")
        print("(Send more --requests to improve coverage.)")

    if likely_owner is not None:
        print(f"\nLikely owner of subtable '{args.name}': TMM {likely_owner} "
              f"(avg {best_avg:.1f} clicks, "
              f"tagged OWNER {per_tmm[likely_owner]['owner']} times)")
    else:
        print(f"\nNo TMM tagged OWNER for '{args.name}'. Either no owner "
              f"exists yet, or fast_threshold is set too low.")


# ---------- reset mode ----------

def cmd_reset(args):
    print(f"Discovering TMM count from {args.host}:{args.port}/info ...")
    total_tmms = discover_tmm_count(args.host, args.port)
    print(f"BIG-IP reports {total_tmms} TMMs.")
    print(f"Sending {args.requests} /reset requests with "
          f"{args.workers} workers...\n")

    per_tmm = defaultdict(lambda: {
        "hits": 0, "total_deleted": 0, "first_deleted": None,
    })
    lock = threading.Lock()

    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        futs = [ex.submit(call_reset, args.host, args.port)
                for _ in range(args.requests)]
        for fut in as_completed(futs):
            try:
                tmm, deleted = fut.result()
            except Exception as e:
                print(f"connection error: {e}", file=sys.stderr)
                continue
            if tmm is not None:
                with lock:
                    b = per_tmm[tmm]
                    b["hits"] += 1
                    if b["first_deleted"] is None and deleted is not None:
                        b["first_deleted"] = deleted
                    if deleted is not None:
                        b["total_deleted"] += deleted

    print("Reset summary:")
    print(f"{'TMM':>5} {'hits':>6} {'first_deleted':>14} {'total_deleted':>14}")
    print("-" * 42)
    for tmm in sorted(per_tmm):
        b = per_tmm[tmm]
        first = b["first_deleted"] if b["first_deleted"] is not None else "-"
        print(f"{tmm:>5} {b['hits']:>6} {first:>14} {b['total_deleted']:>14}")

    missing = sorted(set(range(total_tmms)) - set(per_tmm.keys()))
    if missing:
        print(f"\nWARNING: TMMs not reached: {missing}. "
              f"Re-run with higher --requests.")
    else:
        total_first = sum(b["first_deleted"] or 0 for b in per_tmm.values())
        print(f"\nAll {total_tmms} TMMs cleared. "
              f"Total entries removed (first-hit): {total_first:,}")


# ---------- argparse ----------

def build_parser():
    parser = argparse.ArgumentParser(
        prog="tbl-loader.py",
        description=(
            "TMM subtable loader, prober, and resetter for the LOCALDB iRule.\n\n"
            "Modes:\n"
            "  load       Distribute writes across all TMMs (one write per HTTP req).\n"
            "  bulk-post  POST batches of UUIDs to /bulk_load (emulates file batches).\n"
            "  probe      Find which TMM owns a given shared subtable name.\n"
            "  reset      Clear all per-TMM subtables.\n\n"
            "All modes call /info first to discover total TMM count."
        ),
        epilog=(
            "Examples:\n"
            "  # Coverage-driven: loop until every TMM has >= 5 writes\n"
            "  tbl-loader.py load --host 10.0.2.49 --port 80 --min-writes-per-tmm 5\n\n"
            "  # Synthetic bulk load: 1,000,000 records, one write per request\n"
            "  tbl-loader.py load --host 10.0.2.49 --port 80 --count 1000000 --workers 64\n\n"
            "  # Load from a CSV file (key,value per line, optional header)\n"
            "  tbl-loader.py load --host 10.0.2.49 --port 80 --file records.csv --workers 64\n\n"
            "  # Bulk POST: 64 batches of 15,625 UUIDs (4-TMM scale, ~1M writes total)\n"
            "  tbl-loader.py bulk-post --host 10.0.2.49 --port 80\n\n"
            "  # Bulk POST matching a 16-TMM colleague's pattern (256 × 15,625)\n"
            "  tbl-loader.py bulk-post --host 10.0.2.49 --port 80 \\\n"
            "      --num-posts 256 --uuids-per-post 15625\n\n"
            "  # Probe: find which TMM owns subtable 'uuid_01'\n"
            "  tbl-loader.py probe --host 10.0.2.49 --port 80 --name uuid_01 --requests 500\n\n"
            "  # Clear all per-TMM subtables between test runs\n"
            "  tbl-loader.py reset --host 10.0.2.49 --port 80\n"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    sub = parser.add_subparsers(
        dest="mode",
        required=True,
        metavar="MODE",
        title="modes",
        description="Run 'tbl-loader.py <mode> -h' for mode-specific options.",
    )

    # ---- load ----
    p_load = sub.add_parser(
        "load",
        help="Distribute writes across all TMMs (production path).",
        description=(
            "Open many short-lived HTTP connections to /load so the BIG-IP DAG\n"
            "hashes them across TMMs. Each TMM writes to its own subtable,\n"
            "discovered at first-write time by the LOCALDB proc.\n\n"
            "Choose ONE source:\n"
            "  --count N    generate N synthetic key/value pairs\n"
            "  --file PATH  read key,value pairs from a CSV file\n"
            "  (neither)    coverage-driven: loop until every TMM has\n"
            "               --min-writes-per-tmm writes (capped at\n"
            "               --max-connections)"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p_load.add_argument("--host", required=True, help="BIG-IP VIP address")
    p_load.add_argument("--port", type=int, default=80,
                        help="VIP port (default: 80)")
    p_load.add_argument("--workers", type=int, default=32,
                        help="Concurrent connection workers "
                             "(default: 32; try 64-128 for bulk loads)")
    p_load.add_argument("--key-prefix", default="loadkey",
                        help="Prefix for generated keys (default: loadkey)")

    src = p_load.add_mutually_exclusive_group()
    src.add_argument("--count", type=int,
                     help="Generate N synthetic key/value pairs")
    src.add_argument("--file", type=str,
                     help="Read key,value pairs from a CSV file "
                          "(one per line; '#' comments OK; optional "
                          "'key,value' header)")

    p_load.add_argument("--min-writes-per-tmm", type=int, default=5,
                        help="Coverage mode: stop once every TMM has at "
                             "least this many writes (default: 5)")
    p_load.add_argument("--max-connections", type=int, default=5000,
                        help="Coverage mode: hard cap on total connections "
                             "(default: 5000)")
    p_load.set_defaults(func=cmd_load)

    # ---- bulk-post ----
    p_bulk = sub.add_parser(
        "bulk-post",
        help="POST batches of synthetic UUIDs to /bulk_load (emulates file-batch loading).",
        description=(
            "Generate synthetic UUIDv4 batches and POST each to /bulk_load.\n"
            "Each POST writes many keys in a tight server-side loop, which\n"
            "exercises body parsing and write throughput rather than per-\n"
            "connection setup overhead. Returns per-TMM distribution and\n"
            "per-write click timing aggregated from response bodies.\n\n"
            "Defaults are sized for a 4-TMM box (1/4 of a 16-vCPU pattern\n"
            "of 256 × 15,625 UUIDs). Override --num-posts to scale.\n\n"
            "To match a colleague's 16-vCPU run exactly:\n"
            "  --num-posts 256 --uuids-per-post 15625"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p_bulk.add_argument("--host", required=True, help="BIG-IP VIP address")
    p_bulk.add_argument("--port", type=int, default=80,
                        help="VIP port (default: 80)")
    p_bulk.add_argument("--num-posts", type=int, default=64,
                        help="Number of POST requests to send "
                             "(default: 64, sized for 4-TMM)")
    p_bulk.add_argument("--uuids-per-post", type=int, default=15625,
                        help="UUIDs per POST body (default: 15625, "
                             "matching colleague's pattern). "
                             "Body size ≈ 37 × this value bytes.")
    p_bulk.add_argument("--workers", type=int, default=16,
                        help="Concurrent POST workers (default: 16). "
                             "Each POST is large; fewer workers than --load "
                             "is usually right.")
    p_bulk.set_defaults(func=cmd_bulk_post)

    # ---- probe ----
    p_probe = sub.add_parser(
        "probe",
        help="Find which TMM owns a given shared subtable name.",
        description=(
            "Hit /probe?name=<name> repeatedly so every TMM writes to the same\n"
            "subtable on purpose. The owner TMM responds fast (tagged OWNER);\n"
            "non-owners respond slow (NON_OWNER). Aggregates per-TMM hit\n"
            "counts, click timings, and OWNER/NON_OWNER tag tallies, then\n"
            "prints the most likely owner."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p_probe.add_argument("--host", required=True, help="BIG-IP VIP address")
    p_probe.add_argument("--port", type=int, default=80,
                         help="VIP port (default: 80)")
    p_probe.add_argument("--name", default="uuid_01",
                         help="Subtable name to probe (default: uuid_01)")
    p_probe.add_argument("--requests", type=int, default=500,
                         help="Total /probe requests to send (default: 500)")
    p_probe.add_argument("--workers", type=int, default=32,
                         help="Concurrent connection workers (default: 32)")
    p_probe.set_defaults(func=cmd_probe)

    # ---- reset ----
    p_reset = sub.add_parser(
        "reset",
        help="Clear all per-TMM LOCALDB subtables.",
        description=(
            "Hit /reset on the VIP many times so the request fans out\n"
            "across all TMMs, clearing each TMM's local subtable. Use\n"
            "between test runs to start from clean state.\n\n"
            "The first-hit deletion count for each TMM tells you how big\n"
            "that TMM's subtable was at reset time, which doubles as a\n"
            "sanity check on the previous run's distribution."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p_reset.add_argument("--host", required=True, help="BIG-IP VIP address")
    p_reset.add_argument("--port", type=int, default=80,
                         help="VIP port (default: 80)")
    p_reset.add_argument("--requests", type=int, default=200,
                         help="Total /reset requests to send "
                              "(default: 200; should be >> TMM count)")
    p_reset.add_argument("--workers", type=int, default=32,
                         help="Concurrent connection workers (default: 32)")
    p_reset.set_defaults(func=cmd_reset)

    return parser


def main():
    parser = build_parser()
    # No args at all → print full help (including subcommand list) and exit
    # cleanly, instead of argparse's unhelpful "MODE required" error.
    if len(sys.argv) == 1:
        parser.print_help()
        sys.exit(0)
    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
