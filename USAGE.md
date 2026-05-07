# subtable_test.tcl — User Guide

HTTP-driven test harness for validating per-TMM subtable locality on F5 BIG-IP. Companion to the `LOCALDB` proc library.

## What This Is For

TMM subtables in TMOS are partitioned across TMMs, with each subtable name hash-assigned to a single owning TMM. Writes to a non-owner TMM incur substantial inter-TMM coordination overhead (often 1000x+ slower than owner-local writes). This iRule exposes endpoints to:

- Drive load against the LOCALDB proc library (which discovers a locally-owned subtable name per TMM)
- Verify locality via timing measurements
- Probe arbitrary subtable names to identify their owners
- Reset state between test runs

## Prerequisites

1. The `LOCALDB` proc library deployed at `/Common/LOCALDB`
2. This iRule attached to a virtual server with an HTTP profile
3. The companion Python loader script (`table_loader.py`) for driving load

## Endpoints

### `GET /info`

Returns the TMM index that handled the request and the total TMM count on this BIG-IP. Used by the loader to discover cluster size.

```
$ curl http://10.0.2.49/info
tmm=2 total_tmms=4
```

### `GET /load?key=<KEY>&val=<VAL>`

Writes `<KEY>=<VAL>` to whichever TMM accepts the connection, into that TMM's locally-owned subtable. Returns the TMM index and confirms the key was written.

```
$ curl "http://10.0.2.49/load?key=user_alice&val=12345"
tmm=1 subtable=tmm 1 subtable localdb_tmm_1_847291 total_tmms 4 writes 1 entries 1 key=user_alice
```

The response is intentionally minimal (no full count enumeration) so throughput stays flat as the subtable grows.

### `POST /bulk_load`

Accepts a POST body of newline-separated keys (one per line). Each key is written to the responding TMM's locally-owned subtable with a fixed value of `"1"`. One HTTP connection writes many keys, eliminating per-write TCP setup overhead.

```
$ printf 'aaaa-1\nbbbb-2\ncccc-3\n' | curl --data-binary @- \
    -H "Content-Type: text/plain" http://10.0.2.49/bulk_load
tmm=2 subtable=localdb_tmm_2_5946 written=3 elapsed_clicks=42 clicks_per_write=14
```

The response includes server-side write-loop timing (`elapsed_clicks` for the whole batch, `clicks_per_write` averaged). The loader's `bulk-post` mode aggregates these for per-TMM stats. The handler requires an explicit `Content-Length` header (no chunked encoding) and caps batch size at 16 MiB for memory safety. UUIDv4 input gives ~580 KiB per 15,625-key batch.

This endpoint achieves roughly 30× the throughput of the per-write `/load` path because the write loop happens server-side in a single iRule invocation rather than 15,625 separate request/response cycles.

### `GET /dump`

Returns all keys currently in the responding TMM's subtable, plus the entry count. Note that you can only see *one TMM's* keys per request — the one that hashed the inbound connection. To see all TMMs, hit `/dump` repeatedly (the loader's reset/probe modes do this fan-out automatically).

```
$ curl http://10.0.2.49/dump
tmm=3 count=24832 keys=loadkey_1 loadkey_5 loadkey_9 loadkey_13 ...
```

### `GET /probe?name=<NAME>`

Diagnostic mode. Every TMM writes a TMM-tagged key to the same subtable name. The owner of `<NAME>` writes in tens of clicks (`OWNER`); non-owners take 100+ clicks (`NON_OWNER`). Used to confirm or discover which TMM owns a given subtable name.

```
$ curl "http://10.0.2.49/probe?name=uuid_01"
tmm=2 subtable=uuid_01 clicks=2104 entries=4 NON_OWNER

$ curl "http://10.0.2.49/probe?name=uuid_01"
tmm=3 subtable=uuid_01 clicks=21 entries=4 OWNER
```

If `name` is omitted, defaults to `uuid_01`.

### `GET /reset`

Clears all entries from the responding TMM's local subtable but retains the discovered subtable name (so the next write doesn't re-run the timing-probe ownership discovery). Returns the deletion count.

```
$ curl http://10.0.2.49/reset
tmm=1 deleted=24832

$ curl http://10.0.2.49/reset
tmm=1 deleted=0    # already cleared on this TMM
```

To clear all TMMs, hit it many times (200+ for a 4-TMM system) so connections fan out — see the loader's `reset` subcommand.

### `GET /whoami`

Returns this TMM's full LOCALDB state: TMM index, discovered subtable name, total TMM count, lifetime write count, and current entry count.

```
$ curl http://10.0.2.49/whoami
tmm 2 subtable localdb_tmm_2_473918 total_tmms 4 writes 24832 entries 24832
```

Useful right after deploying the rule to confirm every TMM picked a unique subtable name.

## Configuration Knobs

All configuration lives in `RULE_INIT` of the `LOCALDB` rule:

```tcl
set static::LOCALDB_debug_sample 1000   # Log sampling rate
set static::LOCALDB_fast_threshold 100  # Click threshold for OWNER tag
```

### `static::LOCALDB_debug_sample`

Controls how many `LOCALDB::set` operations get logged with timing data:

- `0` — no debug logs at all (production)
- `1` — log every write (verbose; use only for low-volume debugging)
- `N > 1` — log roughly 1 in N writes per TMM

For a 1M-record test on a 16-TMM system, `1000` produces ~1000 log lines (manageable). For 100k records on 4 TMMs, `100` is comfortable. Match log volume to your `/var/log/ltm` rotation policy.

The calling rule's `CLIENT_ACCEPTED` and `/load` log lines also gate on this same value via `LOCALDB::should_log`, so one knob controls everything.

### `static::LOCALDB_fast_threshold`

Click threshold below which a write is tagged `FAST_LOCAL` / `OWNER`. Defaults to 100. Empirically:

- Owner-local writes: 3-50 clicks
- Non-owner writes on small subtables: 100-500 clicks
- Non-owner writes on large subtables: 10,000-300,000+ clicks

If you see legitimate owner writes occasionally tagged `SLOW`, raise the threshold. If non-owner writes are leaking through as `FAST_LOCAL`, lower it.

## Test Workflow

A typical locality-validation cycle uses per-write loading:

```bash
# 1. Verify every TMM picked a unique subtable name
for i in $(seq 1 30); do curl -s http://10.0.2.49/whoami; done | sort -u

# 2. Clear any residual state
python tbl-loader.py reset --host 10.0.2.49 --port 80

# 3. Run a load
python tbl-loader.py load --host 10.0.2.49 --port 80 \
    --count 100000 --workers 64

# 4. Pull timing samples and verify locality
ssh root@10.0.2.49 "grep 'sampled' /var/log/ltm" | python timing_stats.py

# 5. (Optional) Spot-check ownership of a runtime-discovered name
SUBNAME=$(ssh root@10.0.2.49 "grep 'initialized after' /var/log/ltm | head -1 | grep -o 'localdb_tmm_[^ ]*'")
python tbl-loader.py probe --host 10.0.2.49 --port 80 --name "$SUBNAME" --requests 200

# 6. Clean up before next iteration
python tbl-loader.py reset --host 10.0.2.49 --port 80
```

## Bulk POST Loading

For high-throughput tests where TCP setup cost would dominate, use `bulk-post` mode. It generates synthetic UUIDv4 batches and POSTs each to `/bulk_load`, where the iRule walks the body in a tight write loop. One TCP connection writes many keys.

```bash
# Default 4-TMM test: 64 POSTs × 15,625 UUIDs = 1,000,000 total writes
python tbl-loader.py bulk-post --host 10.0.2.49 --port 80

# Match a colleague's 16-vCPU pattern (256 × 15,625 ≈ 4M writes)
python tbl-loader.py bulk-post --host 10.0.2.49 --port 80 \
    --num-posts 256 --uuids-per-post 15625

# Smaller, faster smoke test
python tbl-loader.py bulk-post --host 10.0.2.49 --port 80 \
    --num-posts 16 --uuids-per-post 1000
```

The output reports per-TMM POST counts, write counts, and `clicks_per_write` averages aggregated from the response bodies — no log scraping required. A locality verdict prints at the end ("OK: all TMMs averaged < 100 clicks per write" or a warning naming slow TMMs).

For accurate measurements, **silence the calling rule's logging** before running bulk-post at high rates. The default `LOCALDB.tcl` ships with `static::LOCALDB_debug_sample 1000`, which generates ~1000 sample log lines per million writes — fine for the per-write `load` mode, but it interacts with bulk POSTs in a way that drifts throughput across consecutive runs. Set `static::LOCALDB_debug_sample 0` in `RULE_INIT` and reload the rule before bulk testing. The response bodies still carry per-batch timing, so you don't lose visibility.

## Defaults and Recommended Tuning

The loader and the iRule ship with defaults aimed at a 4-TMM virtual edition. Below is a reference for what the defaults are and how to deviate for common test patterns.

### Loader defaults

| Mode | Parameter | Default | When to change |
|------|-----------|---------|----------------|
| `load` | `--workers` | 32 | Bump to 64-128 for bulk synthetic loads. Lower if you hit TIME_WAIT exhaustion. |
| `load` | `--count` | (none) | Required for synthetic loads. Use 100k for locality tests, 1M+ for throughput. |
| `load` | `--min-writes-per-tmm` | 5 | Coverage mode only. Fine as-is for "did every TMM get touched" verification. |
| `load` | `--max-connections` | 5,000 | Coverage mode safety cap. Raise if TMM count is high (16+). |
| `bulk-post` | `--num-posts` | 64 | Scale with TMM count. Rule of thumb: 16× TMM count for stable distribution. |
| `bulk-post` | `--uuids-per-post` | 15,625 | Match the workload you're emulating. Larger = more memory pressure, fewer connections. |
| `bulk-post` | `--workers` | 16 | Each POST is large; lower than `load`'s default on purpose. |
| `probe` | `--requests` | 500 | More for high-TMM-count systems (>8 TMMs need 1000+ for full coverage). |
| `reset` | `--requests` | 200 | Should be ≥ 50× TMM count to ensure every TMM is hit at least once. |

### iRule defaults (in `LOCALDB.tcl` RULE_INIT)

| Static | Default | Meaning | Recommended deviations |
|--------|---------|---------|------------------------|
| `LOCALDB_debug_sample` | `1000` | Log roughly 1-in-N writes | Set to `0` for bulk-post throughput tests; `100` for low-volume locality verification; `1` only for low-write-rate debugging. |
| `LOCALDB_fast_threshold` | `100` | Click threshold for OWNER tag | Raise to `200` on slower hardware where local writes occasionally exceed 100 clicks; lower to `50` if non-owner writes are leaking through as FAST_LOCAL on small subtables. |

### Recommended patterns by use case

**Validating locality on a fresh deploy** — small load, generous sampling, full timing analysis:
```bash
python tbl-loader.py reset --host $VIP --port 80
python tbl-loader.py load --host $VIP --port 80 --count 100000 --workers 64
ssh root@$VIP "grep 'sampled' /var/log/ltm" | python timing_stats.py
```
Keep `LOCALDB_debug_sample` at `1000` (default).

**Throughput benchmark** — bulk POSTs, silenced logs, multiple runs:
```bash
# In LOCALDB.tcl RULE_INIT: set LOCALDB_debug_sample 0, reload rule
for i in 1 2 3 4 5; do
    python tbl-loader.py reset --host $VIP --port 80
    python tbl-loader.py bulk-post --host $VIP --port 80 --num-posts 64
    sleep 30
done
```
Compare throughput across runs; consistent numbers indicate the locality and logging are both clean.

**Apples-to-apples comparison with a colleague's run** — match their POST count and batch size:
```bash
python tbl-loader.py bulk-post --host $VIP --port 80 \
    --num-posts <THEIRS> --uuids-per-post <THEIRS>
```
For a 16-vCPU box doing 256 × 15,625, scale your 4-TMM box to 64 × 15,625 (1/4 the POSTs, same batch size).

**Memory pressure / scale test** — push subtable sizes high enough to find limits:
```bash
python tbl-loader.py bulk-post --host $VIP --port 80 \
    --num-posts 256 --uuids-per-post 60000
```
That's ~15M writes (~3.75M per TMM on a 4-TMM box). Watch `tmsh show sys memory` during the run.

**Forensic ownership investigation** — figure out which TMM owns a specific name:
```bash
python tbl-loader.py probe --host $VIP --port 80 --name suspicious_name --requests 1000
```
1000 requests give clean signal even on 16-TMM systems.

## Interpreting Results

### Healthy Locality

All TMMs cluster in the low-clicks range:

```
  TMM  samples        min        avg        max
    0       96          3       18.2         84
    1       98          4       19.7         91
    2      102          3       17.9         77
    3      101          3       18.5         88
```

Every TMM is writing to its owner-local subtable. Throughput stays flat across the run.

### Broken Locality

One or two TMMs cluster low; the rest are orders of magnitude higher:

```
  TMM  samples        min        avg        max
    0       74        121    64855.6     229089
    1       34        136    71536.3     236204
    2       38        121    98516.9     293259
    3       62          3       13.3         25
```

Most writes are funneling through TMM 3 because the subtable names assigned to TMMs 0/1/2 don't actually hash to those TMMs. Run `/probe` against each subtable name to confirm, and check that `init_table` is using the timing-probe approach (not deterministic naming).

### Throughput Decay Over Time

Smooth throughput decay (e.g., 3000/s → 600/s over 40k requests) without timing degradation in the samples usually means the calling rule is doing O(n) work on every request — typically `table keys` + `llength` to compute a count. The current iRule avoids this by maintaining the count incrementally in `static::LOCALDB_entries`.

## Troubleshooting

### `can't read "static::LOCALDB_<X>": no such variable`

A static was added to the rule but the existing TMM state didn't get re-initialized. Reload the rule (`tmsh modify ltm rule LOCALDB ...`) — `RULE_INIT` will re-run on every TMM and re-initialize defaults. The current rule includes self-healing checks that re-run `init_table` on missing variables, so this should not recur.

### `WARNING could not find local subtable after N probes`

The discovery loop in `init_table` exhausted its tries without finding a name with timing under `fast_threshold`. Either:

- Raise `maxtry` in `init_table` (currently 200; for 32+ TMMs you may want 500)
- Raise `fast_threshold` in `RULE_INIT` if owner writes are legitimately slower on this hardware (run `/probe` against a few names to see what owner-vs-non-owner timing looks like in your environment)

### Uneven write distribution across TMMs

If `/load` requests aren't fanning out evenly (e.g., one TMM gets 80% of writes), this is a DAG/disaggregation issue, not a LOCALDB issue. Check:

- That the loader is using fresh sockets per request (`Connection: close`, no keep-alive)
- That client ephemeral port range is large enough (`cat /proc/sys/net/ipv4/ip_local_port_range`)
- BIG-IP DAG configuration (`tmsh list net ... cmp-hash`)

LOCALDB locality and DAG distribution are independent concerns. A working LOCALDB should give every TMM fast writes regardless of how unevenly DAG distributes connections.

## Production Use (DON'T WITHOUT MORE INVESTIGATION!)

This is a test harness built in a home lab to discover subtable write behavior. For production, you'd typically:

1. Remove the `/probe`, `/reset`, `/dump`, `/whoami` endpoints (or wrap them in source-IP allow-lists)
2. Set `static::LOCALDB_debug_sample` to `0` to disable per-write logging
3. Replace the HTTP-based `/load` interface with whatever your real workload uses (CLIENT_DATA framing, HTTP_REQUEST body parsing, etc.)
4. Switch from `set_unique` to `set` if your real workload may write the same key twice (or vice versa)
5. Decide whether `indef` lifetime is right for your data — for ephemeral session data, finite idle is usually better, with the caveat that `static::LOCALDB_entries` becomes an upper-bound estimate rather than an exact count
