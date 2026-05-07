# irules-subtable-discovery

iRule pattern and test harness for **per-TMM local subtable storage** on F5 BIG-IP, plus the tooling to validate that locality at scale.

## The "Interesting Problem" Rabbit Hole

TMM subtables in TMOS are partitioned across TMMs by hashing the subtable name. A write from a non-owner TMM costs roughly **1,000–10,000× more** than a write from the owner — the difference between ~5 clock clicks and ~70,000. If you want fast per-TMM session storage from an iRule, every TMM has to write to a subtable it owns. The catch is that you cannot construct a name that's guaranteed to be locally owned: the name-to-owner mapping is an opaque hash internal to TMOS. Names like `localdb_tmm_3` aren't owned by TMM 3 just because they end in `3`; they're owned by whichever TMM the hash assigns them to.

We hit this directly. A "cleaner" rewrite using deterministic per-TMM names (`localdb_tmm_0`, `localdb_tmm_1`, ...) appeared to work — writes succeeded, distribution looked even, throughput was decent — but per-TMM timing analysis revealed three of four TMMs were averaging **64,000–98,000 clicks per write** while one TMM averaged 13. The deterministic names happened to all hash to one or two TMMs, so the rest were paying the inter-TMM coordination tax on every write.

## The Solution

Building on a timing-probe approach from a colleague's take on this problem (thanks Nat!!), the fix lives in the `init_table` proc: each TMM generates random candidate names, times a trial write to each, and keeps the first name whose write completes under a configurable click threshold (default 100). After this change, all TMMs averaged 5-7 clicks per write — a ~10,000× improvement on the affected TMMs with no other modifications. The repo includes the corrected library, a calling iRule with diagnostic and bulk-load endpoints, and a Python harness that validates locality through timing measurements rather than just smoke-testing that writes succeed.

## What's In Here

```
.
├── LOCALDB.tcl                    # The proc library (timing-probe discovery, set/lookup/keys)
├── subtable_test.tcl              # Calling iRule exposing /info, /load, /bulk_load,
│                                  #   /dump, /probe, /reset, /whoami
├── tbl-loader.py                  # Multi-mode load generator and prober
├── timing_stats.py                # Log-line parser, per-TMM percentile analysis
└── USAGE.md                       # Detailed endpoint reference + tuning guide
```

## Top Lessons

A few things I'd carry forward from this project to similar systems:

**Discovery beats construction in opaque-hash systems.** Anywhere ownership is determined by a hash you can't see (TMM subtables, Redis Cluster slots, Cassandra token ranges, Kafka partition assignments), you can't construct a locally-owned key — you have to find one by trying. If a working pattern looks over-engineered with random retries and timing measurements, those parts are usually load-bearing. Removing them produces a system that looks fine until you measure it.

**Smoke tests miss this class of bug.** The broken design passed every smoke test: connections accepted, writes returned 200, distribution counts looked balanced, throughput was a believable number. The bug only became visible with **per-TMM percentile breakdowns of write timing** — and even then, one TMM was masquerading as fine because it happened to be the accidental hash target for every name. Average-of-averages would have hidden it. The check that actually catches this is "does every TMM have similar p99 timing," not "did the writes succeed."

**Logging is not free at high write rates.** I know this one well, but seeing it lived out always shocks me at how not free it is. Once locality and an O(n) `count` bug were both fixed, bulk-POST throughput drifted downward across consecutive runs (163k → 129k → 122k writes/sec). The cause was unconditional `log local0.` statements producing 264 syslog writes per test cycle. After silencing them, runs stabilized at ~133k writes/sec ±4%. The lesson: at high write rates, the *rule path* needs to be quiet, not just "well-gated." Even gated log statements run their gate evaluation on every request. Pull timing data from response bodies instead of logs whenever you can.

For the long version with charts and full investigation narrative, I'll have an article on DevCentral coming out soon.

## Quick Start

### Deploy

1. Install the LOCALDB rule
2. Install the subtable_test rule
3. Attach the subtable_test rule to a virtual server with an HTTP profile

### Validate

The five-step locality verification:

```bash
# 1. Confirm every TMM picked a unique subtable name
for i in $(seq 1 30); do curl -s http://<vip>/whoami; done | sort -u

# 2. Reset to clean state
python tbl-loader.py reset --host <vip> --port 80

# 3. Drive 100k writes spread across TMMs
python tbl-loader.py load --host <vip> --port 80 --count 100000 --workers 64

# 4. Pull per-TMM timing percentiles and verdict
ssh root@<vip> "grep 'sampled' /var/log/ltm" | python timing_stats.py

# 5. Spot-check ownership of a discovered name
python tbl-loader.py probe --host <vip> --port 80 \
    --name <name from step 1> --requests 200
```

A healthy result looks like:

```
  TMM      n   FAST   SLOW     min     p50       avg     p95     p99       max
------------------------------------------------------------------------------
    0     25     25      0       3       5       5.5      10      11        11
    1     24     24      0       3       5       6.1      11      18        18
    2     24     24      0       2       6       6.1      10      11        11
    3     25     25      0       2       6       6.5      12      13        13

OK: all TMMs have average write timing below 100 clicks. Per-TMM locality is working.
```

If any TMM's average exceeds the threshold, the report names it and points at likely causes.

### High-Throughput Bulk Loading

For throughput tests where TCP setup would dominate, use `bulk-post` mode. It POSTs batches of synthetic UUIDs to `/bulk_load`, which writes them in a tight server-side loop:

```bash
# Home lab virtual 4-TMM scale: 64 POSTs × 15,625 random IDs = 1M writes
python tbl-loader.py bulk-post --host <vip> --port 80

# Cloud instance 16-TMM scale (256 POSTs × 15,625 random IDs ≈ 4M writes)
python tbl-loader.py bulk-post --host <vip> --port 80 \
    --num-posts 256 --uuids-per-post 15625
```

Expect ~30× the throughput of `load` mode (130k+ writes/s vs 4-5k writes/s on a 4-TMM VE).

**Before bulk testing:** set `static::LOCALDB_debug_sample 0` in `LOCALDB.tcl`'s `RULE_INIT` and reload the rule. The default sampling rate of 1000 produces enough syslog volume at bulk-post throughput to drift results across consecutive runs. Response bodies still carry per-batch timing, so visibility is preserved.

### Other Modes

```bash
# Find which TMM owns an arbitrary subtable name
python tbl-loader.py probe --host <vip> --port 80 --name uuid_01 --requests 500

# Clear all per-TMM subtables between runs
python tbl-loader.py reset --host <vip> --port 80

# Top-level help — lists all modes
python tbl-loader.py
```

## Configuration

Two statics in `LOCALDB.tcl`'s `RULE_INIT` control behavior:

| Static | Default | Recommended deviations |
|--------|---------|------------------------|
| `LOCALDB_debug_sample` | `1000` | `0` for bulk-post benchmarks; `100` for low-volume diagnostics; `1` only for active debugging at very low rates |
| `LOCALDB_fast_threshold` | `100` | `200` if owner writes occasionally exceed 100 clicks on slower hardware; `50` for tighter detection on small subtables |

See [USAGE.md](USAGE.md) for the full table of loader and iRule parameters with per-use-case recommendations.

## Compatibility

Built and validated against TMOS v21 on a 4-TMM virtual edition. The pattern itself goes back further (the original LOCALDB style) predates this and works on older versions); the proc library here uses syntax compatible with v17+ as far as I'm aware. The `HTTP::collect` flow in `/bulk_load` requires explicit `Content-Length`; chunked transfer encoding is not supported.

## License

[License](LICENSE) is MIT.

## Contributing

Issues and PRs welcome. If you've reproduced the locality validation on a different platform (different TMM count, hardware vs VE, etc.) and have timing numbers to share, those are especially useful — the more data points across configurations, the better the heuristics in this repo can be tuned.
