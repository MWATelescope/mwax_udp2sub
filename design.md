
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
- If it's older, and not currently reading metafits it transitions to `state` 6 (abandon)

The first time a packet arrives for a new slot, the slot is cleared, iff `state`>=4
(which in turn sets both `state` and `meta_done` to zero)
then if `state` is zero, it's set to 1 just after initialising the `subobs` field

```
1._ -> 2._         # request subfile write. This is the transition for recent packets
1.[^2] -> 6._      # abandon.  (note that if we miss this window it could get stuck)

[456._] -> 0.0     # free the slot (but we should probably check if add_meta_fits has finished with it)
0._ -> 1._         # mark as receiving packets
```

### add_meta_fits()

idles until it finds at least one slot in `state` 1 whose `meta_done` is 0
if it finds one, it changes `meta_done` for the oldest such slot to 2, attempts
to read the metafile, then sets `meta_done` to 4 or 5 depending on whether the
metadata acquisition was successful.

```
1.0 -> _.2         # enter metafits reading state (but after this we should check state has not become 4 or 5 before continuing)
_.2 -> _.[45]      # exit metafits reading state
```

### makesub()
idles until it finds at least one slot in `state` 2 whose `meta_done` = 4
Then it attempts to write out a subfile for the oldest such slot, setting `state` to 3
to indicate it's working on it, then on completion
sets the `state` to 4 or 5 (depending on whether it succeeded or failed)

```
2.4 -> 3.4      # record that subfile is a WIP
3.4 -> [45].4   # subfile writing succeeded/failed
```

