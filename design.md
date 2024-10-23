
# udp2sub internals

## State machine

This section documents the state transitions for the four slots that are used for collecting packets for the four
most recent subobservations.  (At most two are collecting packets at any one time, but one or more may be queued for
writing out as subfiles). The `code` boxes below give states as (state,meta_done) pairs.

### UDP_parse()

Upon a new packet arriving that belongs to a different subobs to the previous packet's subobs,
`UDP_parse` first updates the current subobservation receiving window (as delineated by `start_window`
and `end_window`),
 then for any slot in `state` 1 that's no longer in the window:
- if it was for the subobservation immediately before the new window, it transitions to `state` 2 (request subfile write)
- If it's older, it transitions to `state` 6 (mark for abandonment, pending metafits read completion)

The first time a packet arrives for a new slot, the slot is marked as being for that subobs, iff `state`==0


```
0.0 -> 1.1         # mark as receiving packets, and request metafits read
1._ -> 2._         # request subfile write. This is the transition for recent packets
1._ -> 6._         # abandon.
```

### add_meta_fits()

idles until it finds at least one slot in `state` 1 whose `meta_done` is 0
if it finds one, it changes `meta_done` for the oldest such slot to 2, attempts
to read the metafile, then sets `meta_done` to 4 or 5 depending on whether the
metadata acquisition was successful.

```
6.[01] -> _.6   # let makesub know it's safe to free the slot.
[^6].1 -> _.2   # enter metafits reading state
_.2 -> _.[45]   # exit metafits reading state
```

### makesub()
idles until it finds at least one slot in `state` 2 whose `meta_done` = 4
Then it attempts to write out a subfile for the oldest such slot, setting `state` to 3
to indicate it's working on it, then on completion
sets the `state` to 4 or 5 (depending on whether it succeeded or failed)

the slot is cleared once writing is complete, or if the slot needs abandoning
(metafits failed, or slot too old)

```
2.4 -> 3.4        # record that subfile is a WIP
3.4 -> [45].4     # subfile writing succeeded/failed
                  
[45].4 -> 0.0     # free the slot  (subfile write complete)
4.5 -> 0.0        # free the slot (metafits read failed)
6.[456] -> 0.0    # free the slot (abandonment requested)
```

## Standard compatibility

The state variables now use C atomics, which require at least C11.
The only difference between C11 and C17 in that regard is the deprecation of ATOMIC_VAR_INIT,
so we specify C17 to ensure we don't use that.
(C17 was generally speaking a bugfix release anyway)

