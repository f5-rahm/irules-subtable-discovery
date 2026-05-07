ltm rule subtable_test {
    #
    # Calling iRule for the LOCALDB proc library.
    #
    # Exposes HTTP endpoints to drive testing and validation of the
    # per-TMM subtable pattern. Companion to /Common/LOCALDB.
    #
    # Endpoints:
    #   GET /info             — returns this TMM's index and total TMM count
    #   GET /load?key=K&val=V — write K=V to this TMM's local subtable
    #   GET /dump             — list all keys in this TMM's local subtable
    #   GET /probe?name=NAME  — diagnostic: time a write to a shared subtable
    #                           name to identify which TMM owns it
    #   GET /reset            — clear all entries from this TMM's subtable
    #   GET /whoami           — report this TMM's LOCALDB state
    #

    when CLIENT_ACCEPTED {
        # Sample CLIENT_ACCEPTED logs against the same rate as LOCALDB writes,
        # using a per-TMM accept counter as a proxy.
        if { ![info exists static::CALLER_accepts] } { set static::CALLER_accepts 0 }
        incr static::CALLER_accepts
        if { [info exists static::LOCALDB_debug_sample] &&
             $static::LOCALDB_debug_sample > 0 &&
             ($static::LOCALDB_debug_sample == 1 ||
              ($static::CALLER_accepts % $static::LOCALDB_debug_sample) == 0) } {
            log local0. "tmm=[TMM::cmp_unit] CLIENT_ACCEPTED accepts=$static::CALLER_accepts from [IP::client_addr]:[TCP::client_port]"
        }
    }

    when HTTP_REQUEST {
        set uri [HTTP::uri]

        # ---- /info ---------------------------------------------------------
        # Returns this TMM's index and the total TMM count from
        # TMM::cmp_count. Used by the loader to discover cluster size.
        if { $uri starts_with "/info" } {
            set body "tmm=[TMM::cmp_unit] total_tmms=[TMM::cmp_count]\n"
            HTTP::respond 200 content $body "Content-Type" "text/plain"
            return
        }

        # ---- /load ---------------------------------------------------------
        # Write key=val to this TMM's local-fast subtable. The response
        # body is intentionally minimal (no count enumeration) so that
        # throughput stays flat as the subtable grows.
        if { $uri starts_with "/load" } {
            set qkey [URI::query $uri key]
            set qval [URI::query $uri val]

            if { $qkey ne "" } {
                # Use set_unique when the loader is generating monotonic keys.
                # If keys may be repeated, change to LOCALDB::set instead.
                call LOCALDB::set_unique $qkey $qval 3600 indef

                # Sampled calling-rule log; aligns with the sampled
                # LOCALDB::set log on the same write.
                if { [call LOCALDB::should_log] } {
                    log local0. "tmm=[TMM::cmp_unit] /load key=$qkey"
                }

                HTTP::respond 200 content "tmm=[TMM::cmp_unit] subtable=[call LOCALDB::whoami] key=$qkey\n" "Content-Type" "text/plain"
                return
            }
            HTTP::respond 400 content "missing key/val\n"
            return
        }

        # ---- /bulk_load ----------------------------------------------------
        # Accepts a POST body of newline-separated keys (typically UUIDs).
        # Each key gets written to this TMM's local-fast subtable with a
        # fixed value of "1". One HTTP connection writes many keys, which
        # exercises HTTP body parsing and tight-loop write throughput
        # rather than connection setup overhead.
        #
        # The Content-Length header is required; chunked transfer is not
        # supported by this handler. Use a default value if not provided.
        if { $uri starts_with "/bulk_load" } {
            if { [HTTP::method] ne "POST" } {
                HTTP::respond 405 content "bulk_load requires POST\n"
                return
            }
            set cl [HTTP::header value Content-Length]
            if { $cl eq "" || $cl == 0 } {
                HTTP::respond 411 content "missing or zero Content-Length\n"
                return
            }
            # Cap collection size to protect memory. 16 MiB is enough for
            # ~400k UUIDs at 38 bytes each (36 chars + \r\n).
            set max_collect 16777216
            if { $cl > $max_collect } {
                HTTP::respond 413 content "payload too large (max $max_collect bytes)\n"
                return
            }
            # Stash the request id so HTTP_REQUEST_DATA knows it's handling
            # a bulk_load, then collect the body.
            set bulk_load_pending 1
            HTTP::collect $cl
            return
        }

        # ---- /dump ---------------------------------------------------------
        # Return the keys currently in this TMM's subtable. Hit it many
        # times (one connection per TMM you want to inspect) to map
        # contents across all TMMs.
        if { $uri starts_with "/dump" } {
            set allkeys [call LOCALDB::keys]
            set count [call LOCALDB::count]
            HTTP::respond 200 content "tmm=[TMM::cmp_unit] count=$count keys=$allkeys\n" "Content-Type" "text/plain"
            return
        }

        # ---- /probe --------------------------------------------------------
        # Diagnostic mode: every TMM writes to the SAME subtable name on
        # purpose. The owner TMM responds fast (OWNER); non-owners respond
        # slow (NON_OWNER). Used to verify name->owner hash assignments.
        if { $uri starts_with "/probe" } {
            set probe_name [URI::query $uri name]
            if { $probe_name eq "" } { set probe_name "uuid_01" }

            set before [clock clicks]
            table set -subtable $probe_name "probe_[TMM::cmp_unit]" [TMM::cmp_unit] 60 indef
            set after [clock clicks]
            set diff [expr {$after - $before}]
            set tag [expr {$diff < $static::LOCALDB_fast_threshold ? "OWNER" : "NON_OWNER"}]

            set total [llength [table keys -subtable $probe_name]]
            # /probe is a diagnostic endpoint, only run with a few hundred
            # requests at a time. Keeping this log line is useful for
            # post-hoc analysis without meaningful overhead.
            log local0. "PROBE tmm=[TMM::cmp_unit] subtable=$probe_name clicks=$diff entries=$total $tag"
            HTTP::respond 200 content "tmm=[TMM::cmp_unit] subtable=$probe_name clicks=$diff entries=$total $tag\n" "Content-Type" "text/plain"
            return
        }

        # ---- /reset --------------------------------------------------------
        # Clear all entries from this TMM's local subtable. Hit it many
        # times so it fans out across all TMMs.
        #
        # No log statement — /reset gets fanned out 200+ times to cover all
        # TMMs, which is enough syslog volume to slow throughput on the
        # next test run. The response body carries the deleted count, and
        # the loader aggregates and reports it.
        if { $uri starts_with "/reset" } {
            set deleted [call LOCALDB::reset_entries]
            HTTP::respond 200 content "tmm=[TMM::cmp_unit] deleted=$deleted\n" "Content-Type" "text/plain"
            return
        }

        # ---- /whoami -------------------------------------------------------
        # Report this TMM's full LOCALDB state. Useful for verifying
        # init_table picked a unique name per TMM after a fresh deploy.
        if { $uri starts_with "/whoami" } {
            HTTP::respond 200 content "[call LOCALDB::whoami]\n" "Content-Type" "text/plain"
            return
        }

        HTTP::respond 404 content "unknown endpoint\n"
    }

    when HTTP_REQUEST_DATA {
        # Triggered after HTTP::collect completes for /bulk_load.
        # Parse the body as newline-separated keys and write each one
        # to this TMM's local subtable.
        if { ![info exists bulk_load_pending] || !$bulk_load_pending } {
            return
        }
        set bulk_load_pending 0

        set body [HTTP::payload]
        set body_len [string length $body]

        # Time the whole batch so the response can carry it back to the
        # client. This captures the steady-state cost of a tight write
        # loop on this TMM, which is closer to a "real" workload than
        # per-connection setup-dominated timing.
        set before [clock clicks]
        set written 0

        # Split on \n; tolerate trailing \r from \r\n line endings.
        # Uses set_unique (skips lookup-before-set) — assumes input is
        # globally unique, which is true for UUIDv4. If you ever feed
        # potentially-overlapping batches, switch to LOCALDB::set so
        # the entries counter stays accurate.
        foreach line [split $body "\n"] {
            set key [string trim $line]
            if { $key eq "" } { continue }
            call LOCALDB::set_unique $key "1" 3600 indef
            incr written
        }
        set after [clock clicks]
        set elapsed [expr {$after - $before}]
        set per_write [expr {$written > 0 ? $elapsed / $written : 0}]

        # No log statement here on purpose. The loader aggregates per-write
        # timing from the response body (clicks_per_write field), so syslog
        # is just I/O overhead during high-rate bulk runs. If you need
        # per-batch debug visibility, gate a log line behind
        # [call LOCALDB::should_log] and set debug_sample to a low number,
        # but expect throughput to drop accordingly.

        HTTP::respond 200 content "tmm=[TMM::cmp_unit] subtable=localdb_tmm_[TMM::cmp_unit] written=$written elapsed_clicks=$elapsed clicks_per_write=$per_write\n" "Content-Type" "text/plain"
    }
}
