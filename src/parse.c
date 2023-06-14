#define _GNU_SOURCE

#include <stdint.h>
#include <string.h>
#include <signal.h>

#include "mwax_udp2sub.h"

const char *TAG = "udp-parse";

/** UDP parsing thread. */
void *task_parse_udp(void *args) {
    app_state_t *state = (app_state_t *)args;
    instance_config_t conf = state->cfg;

    log_info("Initialising...");
    set_cpu_affinity(TAG, conf.cpu_mask_UDP_parse);

    pkt_data_t *my_udp;                                           // Make a local pointer to the UDP packet we're working on.
    uint32_t last_good_packet_sub_time = 0;                             // What sub obs was the last packet (for caching)
    uint32_t subobs_mask = 0xFFFFFFF8;                                  // Mask to apply with '&' to get sub obs from GPS second
    uint32_t start_window = 0;                                          // What's the oldest packet we're accepting right now?
    uint32_t end_window = 0;                                            // What's the highest GPS time we can accept before recalculating our usable window?  Hint: We *need* to recalculate that window
    int slot_ndx;                                                       // index into which of the 4 subobs metadata blocks we want
    int rf_ndx;                                                         // index into the position in the meta array we are using for this rf input (for this sub).  NB May be different for the same rf on a different sub.
    subobs_udp_meta_t *this_sub = NULL;                                 // Pointer to the relevant one of the four subobs metadata arrays
    int loop;

//---------------- Main loop to process incoming udp packets -------------------

    while (!state->terminate) {
      if ( state->UDP_removed_from_buff < state->UDP_added_to_buff ) {                // If there is at least one packet waiting to be processed
        my_udp = &state->UDPbuf[state->UDP_removed_from_buff % state->UDP_num_slots];      // then this is a pointer to it.

        // Process the next arrived UDP packet
        uint64_t subsecspersec = 
            my_udp->packet_type == PACKET_TYPE_CRITICAL    ? CS_SUBSECSPERSEC 
          : my_udp->packet_type == PACKET_TYPE_OVERSAMPLED ? OS_SUBSECSPERSEC 
          :                                                  CS_SUBSECSPERSEC ; // Default to critically sampled.
        if (my_udp->packet_type == PACKET_TYPE_CRITICAL || my_udp->packet_type == PACKET_TYPE_OVERSAMPLED) {            // If it's a real packet off the network with some fields in network order, they need to be converted to host order and have a few tweaks made before we can use the packet
          my_udp->subsec_time   = ntohs(my_udp->subsec_time);         // Convert subsec_time to a usable uint16_t which at this stage is still within a single second
          my_udp->GPS_time      = ntohl(my_udp->GPS_time);            // Convert GPS_time (bottom 32 bits only) to a usable uint32_t
          my_udp->rf_input      = ntohs(my_udp->rf_input);            // Convert rf_input to a usable uint16_t
          my_udp->edt2udp_token = ntohs(my_udp->edt2udp_token);       // Convert edt2udp_token to a usable uint16_t
                                                                      // the bottom three bits of the GPS_time is 'which second within the subobservation' this second is
          my_udp->subsec_time += ((my_udp->GPS_time & 0b111) * subsecspersec) + 1;    // Change the subsec field to be the packet count within a whole subobs, not just this second.  was 0 to 624.  now 1 to 5000 inclusive. (0x21 packets can be 0 to 5001!)
          my_udp->packet_type += 0x01;                                   // Now increment the packet type to say it's similar to a 0x20 but in host byte order
        } else {                                                        // If is wasn't a 0x20 packet, the only other packet we handle is a 0x21
          // Not for us?
          if ( my_udp->packet_type != 0x21 || my_udp->packet_type != 0x31) {  // This packet is not a valid packet for us.
            state->UDP_removed_from_buff++;                                    // Flag it as used and release the buffer slot.  We don't want to see it again.
            continue;                                                   // start the loop again
          }

        }

//---------- If this packet is for the same sub obs as the previous packet, we can assume a bunch of things haven't changed or rolled over, otherwise we have things to check

        if ( ( my_udp->GPS_time & subobs_mask ) != last_good_packet_sub_time ) {                // If this is a different sub obs than the last packet we allowed through to be processed.

//---------- Arrived too late to be usable?

          if (my_udp->GPS_time < start_window) {                        // This packet has a time stamp before the earliest open sub-observation
//WIP            mon->ignored_for_being_too_old++;                              // Keep track for debugging of how many packets were too late to process
            state->UDP_removed_from_buff++;                                    // Throw it away.  ie flag it as used and release the buffer slot.  We don't want to see it again.
            continue;                                                   // start the loop again
          }

//---------- Further ahead in time than we're ready for?

          if (my_udp->GPS_time > end_window) {                          // This packet has a time stamp after the end of the latest open sub-observation.  We need to do some preparatory work before we can process this packet.

            // First we need to know if this is simply the next chronological subobs, or if we have skipped ahead.  NB that a closedown packet will look like we skipped ahead to 2106.  If we've skipped ahead then all current subobs need to be closed.

            if ( end_window == (( my_udp->GPS_time | 0b111 ) - 8) ) {   // our proposed new end window is the udp packet's time with the bottom 3 bits all set.  If that's (only) 8 seconds after the current end window...
              start_window = end_window - 7;                            // then our new start window is (and may already have been) the beginning of the subobs which was our last subobs a moment ago. (7 seconds because the second counts are inclusive).
              end_window = my_udp->GPS_time | 0b111;                    // new end window is the last second of the subobs for the second we just saw, so round up (ie set) the last three bits
                                                                        // Ensure every subobs but the last one are closed
            } else {                                                    // otherwise the packet stream is so far into the future that we need to close *all* open subobs.
              end_window = my_udp->GPS_time | 0b111;                    // new end window is the last second of the subobs for the second we just saw, so round up (ie set) the last three bits
              start_window = end_window - 7;                            // then our new start window is the beginning second of the subobs for the packet we just saw.  (7 seconds because the second counts are inclusive).
            }

            for ( loop = 0 ; loop < 4 ; loop++ ) {                                                      // We'll check all subobs meta slots.  If they're too old, we'll rule them off.  NB this may not be checked in time order of subobs
              if ( ( state->sub[ loop ].subobs < start_window ) && ( state->sub[ loop ].state == 1 ) ) {              // If this sub obs slot is currently in use by us (ie state==1) and has now reached its timeout ( < start_window )
                if ( state->sub[ loop ].subobs == ( start_window - 8 ) ) {     // then if it's a very recent subobs (which is what we'd expect during normal operations)
                  state->sub[ loop ].state = 2;                                // set the state flag to tell another thread it's their job to write out this subobs and pass the data on down the line
                } else {                                                // or if it isn't recent then it's probably because the receivers (or medconv array) went away so we want to abandon it
                  if(state->sub[loop].meta_done != 2) {                        // but we can only do that if we're not currently trying to load its metafits. If not,
                    state->sub[ loop ].state = 6;                              // set the state flag to indicate that this subobs should be abandoned as too old to be useful
                    state->monitor.discarded_subobs++;
                  }
                }
              }
            }

            if ( my_udp->GPS_time == CLOSEDOWN ) {                      // If we've been asked to close down via a udp packet with the time set to the year 2106
              state->terminate = true;                                         // tell everyone to shut down.
              state->UDP_removed_from_buff++;                                  // Flag it as used and release the buffer slot.  We don't want to see it again.
              continue;                                                 // This will take us out to the while !terminate loop that will shut us down.  (Could have used a break too. It would have the same effect)
            }

            // We now have a new subobs that we need to set up for.  Hopefully the slot we want to use is either empty or finished with and free for reuse.  If not we've overrun the sub writing threads

            slot_ndx = ( my_udp->GPS_time >> 3 ) & 0b11;                // The 3 LSB are 'what second in the subobs' we are.  We want the 2 bits immediately to the left of that.  They will give us our index into the subobs metadata array

            if ( state->sub[ slot_ndx ].state >= 4 ) {                         // If the state is a 4 or 5 or higher, then it's free for reuse, but someone needs to wipe the contents.  I guess that 'someone' means me!
              memset( &state->sub[ slot_ndx ], 0, sizeof( subobs_udp_meta_t ));        // Among other things, like wiping the array of pointers back to NULL, this memset should set the value of 'state' back to 0.
            }

            if ( state->sub[ slot_ndx ].state != 0 ) {                         // If state isn't 0 now, then it's bad.  We have a transaction to store and the subobs it belongs to can't be set up because its slot is still in use by an earlier subobs

              log_error("Error: Subobs slot %d still in use. Currently in state %d.", slot_ndx, state->sub[slot_ndx].state );         // If this actually happens during normal operation, we will need to fix the cause or (maybe) bandaid it with a delay here, but for now
              log_error("Wanted to put gps=%d, subobs=%d, first=%lld, state=1", my_udp->GPS_time, ( my_udp->GPS_time & subobs_mask ), state->UDP_removed_from_buff );
              log_error("Found so=%d,st=%d,wait=%d,took=%d,first=%lld,last=%lld,startw=%lld,endw=%lld,count=%d,seen=%d",
                state->sub[slot_ndx].subobs, state->sub[slot_ndx].state, state->sub[slot_ndx].msec_wait, state->sub[slot_ndx].msec_took, state->sub[slot_ndx].first_udp, state->sub[slot_ndx].last_udp, state->sub[slot_ndx].udp_at_start_write, state->sub[slot_ndx].udp_at_end_write, state->sub[slot_ndx].udp_count, state->sub[slot_ndx].rf_seen );

              raise(SIGABRT);                                           // For debugging, let's just crash here so we can get a core dump
              state->terminate = true;                                         // Lets make a fatal error so we know we'll spot it
              continue;                                                 // abort what we're attempting and all go home
            }

            state->sub[ slot_ndx ].subobs = ( my_udp->GPS_time & subobs_mask );        // The subobs number is the GPS time of the beginning second so mask off the bottom 3 bits
            state->sub[ slot_ndx ].first_udp = state->UDP_removed_from_buff;          // This was the first udp packet seen for this sub. (0 based)
            state->sub[ slot_ndx ].state = 1;                                  // Let's remember we're using this slot now and tell other threads.  NB: The subobs field is assumed to have been populated *before* this becomes 1

          }

//---------- This packet isn't similar enough to previous ones (ie from the same sub-obs) to assume things, so let's get new pointers

          this_sub = &state->sub[(( my_udp->GPS_time >> 3 ) & 0b11 )];         // The 3 LSB are 'what second in the subobs' we are.  We want the 2 bits left of that.  They will give us an index into the subobs metadata array which we use to get the pointer to the struct
          last_good_packet_sub_time = ( my_udp->GPS_time & subobs_mask );       // Remember this so next packet we proabably don't need to do these checks and lookups again

        }

//---------- We have a udp packet to add and a place for it to go.  Time to add its details to the sub obs metadata

        rf_ndx = this_sub->rf2ndx[ my_udp->rf_input ];                  // Look up the position in the meta array we are using for this rf input (for this sub).  NB May be different for the same rf on a different sub

        if ( rf_ndx == 0 ) {                                            // If the lookup for this rf input for this sub is still a zero, it means this is the first time we've seen one this sub from this rf input
          this_sub->rf_seen++;                                          // Increase the number of different rf inputs seen so far.  START AT 1, NOT 0!
          this_sub->rf2ndx[ my_udp->rf_input ] = this_sub->rf_seen;     // and assign that number for this rf input's metadata index
          rf_ndx = this_sub->rf_seen;                                   // and get the correct index for this packet too because we'll need that
        }       // WIP Should check for array overrun.  ie that rf_seen has grown past MAX_INPUTS (or is it plus or minus one?)
      
        this_sub->udp_volts[ rf_ndx ][ my_udp->subsec_time ] = (char *) my_udp->volts;          // This is an important line so lets unpack what it does and why.
        // The 'this_sub' struct stores a 2D array of pointers (udp_volt) to all the udp payloads that apply to that sub obs.  The dimensions are rf_input (sorted by the order in which they were seen on
        // the incoming packet stream) and the packet count (0 to 5001) inside the subobs. By this stage, seconds and subsecs have been merged into a single number so subsec time is already in the range 0 to 5001

        this_sub->last_udp = state->UDP_removed_from_buff;                     // This was the last udp packet seen so far for this sub. (0 based) Eventually it won't be updated and the final value will remain there
        this_sub->udp_count++;                                          // Add one to the number of udp packets seen this sub obs.  NB Includes duplicates and faked delay packets so can't reliably be used to check if we're finished a sub
        state->monitor.udp_count++;

//---------- We are done with this packet, EXCEPT if this was the very first for an rf_input for a subobs, or the very last, then we want to duplicate them in the adjacent subobs.
//---------- This is because one packet may contain data that goes in two subobs (or even separate obs!) due to delay tracking

        uint64_t subsecspersec = 
            my_udp->packet_type == PACKET_TYPE_CRITICAL    ? CS_SUBSECSPERSEC 
          : my_udp->packet_type == PACKET_TYPE_OVERSAMPLED ? OS_SUBSECSPERSEC 
          :                                                  CS_SUBSECSPERSEC ; // Default to critically sampled.
        if ( my_udp->subsec_time == 1 ) {                               // If this was the first packet (for an rf input) for a subobs
          my_udp->subsec_time = subsecspersec + 1;                      // then say it's at the end of a subobs
          my_udp->GPS_time--;                                           // and back off the second count to put it in the last subobs
        } else {
          if ( my_udp->subsec_time == subsecspersec ) {                 // If this was the last packet (for an rf input) for a subobs
            my_udp->subsec_time = 0;                                    // then say it's at the start of a subobs
            my_udp->GPS_time++;                                         // and increment the second count to put it in the next subobs
          } else {
            state->UDP_removed_from_buff++;                                    // We don't need to duplicate this packet, so increment the number of packets we've ever processed (which automatically releases them from the buffer).
          }
        }

//---------- End of processing of this UDP packet ----------

      } else {                                                          //
//WIP        mon->sub_meta_sleeps++;                                            // record we went to sleep
        usleep(10000);                                                  // Chill for a bit
      }

    }

//---------- We've been told to shut down ----------

    log_info("UDP parser is shutting down");
    pthread_exit(NULL);
}
