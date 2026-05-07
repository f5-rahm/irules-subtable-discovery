ltm rule LOCALDB {
    #
    # LOCALDB — per-TMM subtable proc library
    #
    # Each TMM discovers a subtable name it OWNS by timing trial writes.
    # Names that hash to the local TMM complete in single/double-digit
    # clock clicks; names owned by other TMMs take 100+ clicks because
    # they require inter-TMM coordination. The proc library hides this
    # discovery and exposes simple set/lookup/keys/count operations that
    # are guaranteed to be local-fast on every TMM.
    #
    # Static variables (per-TMM):
    #   static::LOCALDB_debug_sample    Log sampling rate (0=off, 1=every,
    #                                   N>1 = roughly 1 in N writes)
    #   static::LOCALDB_fast_threshold  Click threshold for "local" writes
    #   static::LOCALDB_name            The discovered local-fast subtable name
    #   static::LOCALDB_tmm             This TMM's index ([TMM::cmp_unit])
    #   static::LOCALDB_writes          Per-TMM total write counter
    #   static::LOCALDB_entries         Per-TMM live entry count (O(1) tracked)
    #
    when RULE_INIT {
        # Log sampling: 0=off, 1=every write, N>1=roughly 1-in-N
        set static::LOCALDB_debug_sample 1000

        # Click threshold: writes faster than this are considered owner-local.
        # Empirically, owner writes complete in 3-50 clicks; non-owner writes
        # take 100+ on small subtables and thousands on large ones. 100 gives
        # clean separation with margin.
        set static::LOCALDB_fast_threshold 100

        # Initialize per-TMM state to safe defaults so they exist on every
        # TMM from rule-load time, before init_table runs.
        set static::LOCALDB_name    ""
        set static::LOCALDB_tmm     -1
        set static::LOCALDB_writes  0
        set static::LOCALDB_entries 0

        log local0. "LOCALDB procs loaded debug_sample=$static::LOCALDB_debug_sample fast_threshold=$static::LOCALDB_fast_threshold tmm=[TMM::cmp_unit]"
    }

    # init_table — discover a subtable name this TMM owns
    #
    # The TMOS name->owner mapping is an opaque hash, so deterministic
    # naming (e.g. "localdb_tmm_<N>") does NOT guarantee local ownership.
    # We probe random names and time the write; the first name that
    # writes in fewer than fast_threshold clicks is owned locally.
    proc init_table { } {
        set static::LOCALDB_tmm     [TMM::cmp_unit]
        set static::LOCALDB_writes  0
        set static::LOCALDB_entries 0

        set try 0
        set maxtry 200
        set found 0
        while { $try < $maxtry } {
            set candidate "localdb_tmm_${static::LOCALDB_tmm}_[expr {int(rand() * 1000000)}]"
            set before [clock clicks]
            table set -subtable $candidate "probe" "1" 5
            set after [clock clicks]
            set diff [expr {$after - $before}]
            if { $diff < $static::LOCALDB_fast_threshold } {
                set static::LOCALDB_name $candidate
                # Clean up the probe entry so it doesn't pollute keys()
                table delete -subtable $candidate "probe"
                set found 1
                log local0. "tmm=$static::LOCALDB_tmm subtable=$static::LOCALDB_name initialized after $try probes (diff=$diff clicks of [TMM::cmp_count] total TMMs)"
                break
            }
            incr try
        }
        if { !$found } {
            # Fallback: use the last candidate even though it's slow.
            # Better than failing; log loudly so we know.
            set static::LOCALDB_name $candidate
            log local0. "tmm=$static::LOCALDB_tmm WARNING could not find local subtable after $maxtry probes; using $static::LOCALDB_name (last diff=$diff)"
        }
    }

    # set — write key/value to this TMM's local subtable
    #
    # Maintains static::LOCALDB_entries incrementally so count() stays O(1).
    # Logs timing on sampled writes (every Nth, where N=debug_sample).
    proc set { key val { idle 180 } { life indef } } {
        # Self-heal: if any per-TMM state is missing or unset (rule reload,
        # manual unset, etc.), re-initialize.
        if { ![info exists static::LOCALDB_name] ||
             ![info exists static::LOCALDB_tmm] ||
             ![info exists static::LOCALDB_writes] ||
             ![info exists static::LOCALDB_entries] ||
             $static::LOCALDB_name eq "" } {
            call LOCALDB::init_table
        }

        # Determine if this is a new key vs an update, before writing.
        # One extra lookup per write but keeps entries counter exact.
        set existing [table lookup -subtable $static::LOCALDB_name $key]

        incr static::LOCALDB_writes
        set should_log 0
        if { $static::LOCALDB_debug_sample > 0 } {
            if { $static::LOCALDB_debug_sample == 1 } {
                set should_log 1
            } elseif { ($static::LOCALDB_writes % $static::LOCALDB_debug_sample) == 0 } {
                set should_log 1
            }
        }

        if { $should_log } {
            set before [clock clicks]
            table set -subtable $static::LOCALDB_name $key $val $idle $life
            set after [clock clicks]
            set diff [expr {$after - $before}]
            set tag [expr {$diff < $static::LOCALDB_fast_threshold ? "FAST_LOCAL" : "SLOW"}]
            log local0. "tmm=$static::LOCALDB_tmm subtable=$static::LOCALDB_name writes=$static::LOCALDB_writes key=$key clicks=$diff $tag (sampled 1/$static::LOCALDB_debug_sample)"
        } else {
            table set -subtable $static::LOCALDB_name $key $val $idle $life
        }

        # Maintain entries counter
        if { $existing eq "" } {
            incr static::LOCALDB_entries
        }
    }

    # set_unique — variant of set() for callers who guarantee unique keys
    #
    # Skips the lookup-before-set, trading exactness for speed. Use when
    # you know every call uses a key that has never been written before.
    proc set_unique { key val { idle 180 } { life indef } } {
        if { ![info exists static::LOCALDB_name] || $static::LOCALDB_name eq "" } {
            call LOCALDB::init_table
        }

        incr static::LOCALDB_writes
        set should_log 0
        if { $static::LOCALDB_debug_sample > 0 } {
            if { $static::LOCALDB_debug_sample == 1 } {
                set should_log 1
            } elseif { ($static::LOCALDB_writes % $static::LOCALDB_debug_sample) == 0 } {
                set should_log 1
            }
        }

        if { $should_log } {
            set before [clock clicks]
            table set -subtable $static::LOCALDB_name $key $val $idle $life
            set after [clock clicks]
            set diff [expr {$after - $before}]
            set tag [expr {$diff < $static::LOCALDB_fast_threshold ? "FAST_LOCAL" : "SLOW"}]
            log local0. "tmm=$static::LOCALDB_tmm subtable=$static::LOCALDB_name writes=$static::LOCALDB_writes key=$key clicks=$diff $tag (unique, sampled 1/$static::LOCALDB_debug_sample)"
        } else {
            table set -subtable $static::LOCALDB_name $key $val $idle $life
        }

        incr static::LOCALDB_entries
    }

    proc lookup { key } {
        if { ![info exists static::LOCALDB_name] || $static::LOCALDB_name eq "" } {
            call LOCALDB::init_table
            return ""
        }
        return [table lookup -subtable $static::LOCALDB_name $key]
    }

    proc keys { } {
        if { ![info exists static::LOCALDB_name] || $static::LOCALDB_name eq "" } {
            call LOCALDB::init_table
            return ""
        }
        return [table keys -subtable $static::LOCALDB_name]
    }

    # count — O(1) entry count from incrementally-maintained counter
    #
    # NOTE: counter does not decrement on idle expiry, so for finite-idle
    # workloads it represents an upper bound. For indef-idle (test loads)
    # or short-lived measurements, it's exact.
    proc count { } {
        if { ![info exists static::LOCALDB_entries] } { return 0 }
        return $static::LOCALDB_entries
    }

    # count_actual — O(n) entry count by enumerating keys
    #
    # Use for reconciliation / verification. Avoid in hot paths.
    proc count_actual { } {
        if { ![info exists static::LOCALDB_name] || $static::LOCALDB_name eq "" } {
            return 0
        }
        return [llength [table keys -subtable $static::LOCALDB_name]]
    }

    # delete — remove a single key, decrementing the entries counter
    proc delete { key } {
        if { ![info exists static::LOCALDB_name] || $static::LOCALDB_name eq "" } {
            return
        }
        set existing [table lookup -subtable $static::LOCALDB_name $key]
        if { $existing ne "" } {
            table delete -subtable $static::LOCALDB_name $key
            if { [info exists static::LOCALDB_entries] && $static::LOCALDB_entries > 0 } {
                incr static::LOCALDB_entries -1
            }
        }
    }

    # delete_table — wipe all entries from this TMM's subtable
    proc delete_table { } {
        set deleted 0
        if { [info exists static::LOCALDB_name] && $static::LOCALDB_name ne "" } {
            foreach k [table keys -subtable $static::LOCALDB_name] {
                table delete -subtable $static::LOCALDB_name $k
                incr deleted
            }
        }
        # Reset to RULE_INIT defaults rather than unset, so existence
        # checks in other procs don't trip.
        set static::LOCALDB_name    ""
        set static::LOCALDB_tmm     -1
        set static::LOCALDB_writes  0
        set static::LOCALDB_entries 0
        return $deleted
    }

    # reset_entries — clear all entries but keep the discovered subtable name
    #
    # Use this between test runs to get clean state without re-running
    # the timing-probe ownership discovery on every reset.
    proc reset_entries { } {
        set deleted 0
        if { [info exists static::LOCALDB_name] && $static::LOCALDB_name ne "" } {
            foreach k [table keys -subtable $static::LOCALDB_name] {
                table delete -subtable $static::LOCALDB_name $k
                incr deleted
            }
        }
        set static::LOCALDB_writes  0
        set static::LOCALDB_entries 0
        return $deleted
    }

    proc whoami { } {
        if { ![info exists static::LOCALDB_name] || $static::LOCALDB_name eq "" } {
            call LOCALDB::init_table
        }
        return [list tmm $static::LOCALDB_tmm subtable $static::LOCALDB_name total_tmms [TMM::cmp_count] writes $static::LOCALDB_writes entries $static::LOCALDB_entries]
    }

    # should_log — returns 1 if this TMM's current write count is a sample point
    #
    # Use from calling rules to gate their own log lines against the same
    # sample rate as the LOCALDB proc itself, so log output stays coherent.
    proc should_log { } {
        if { ![info exists static::LOCALDB_debug_sample] } { return 0 }
        if { $static::LOCALDB_debug_sample <= 0 } { return 0 }
        if { $static::LOCALDB_debug_sample == 1 } { return 1 }
        if { ![info exists static::LOCALDB_writes] } { return 0 }
        return [expr {($static::LOCALDB_writes % $static::LOCALDB_debug_sample) == 0}]
    }
}
