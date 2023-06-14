//===================================================================================================================================================
// Capture UDP packets into a temporary, user space buffer and then sort/copy/process them into some output array
//
// Author(s)  BWC Brian Crosse brian.crosse@curtin.edu.au
//            LAW Luke Williams luke.a.williams@curtin.edu.au
// Commenced 2017-05-25
//
#define BUILD 87
#define THISVER "2.09"
//
// 2.09-087     2022-11-10 LAW  Factor out `reallocarray`.
//
// 2.08-086     2022-11-09 LAW  New delay table format.
//
// 2.07-085     2022-08-02 LAW  Various bugfixes and tidy-up.
//
// 2.06a-084    2022-07-26 LAW  Enable heartbeat thread, add monitor interface to config file, CLI options to force cable/geometric delays.
//
// 2.05a-083    2022-06-21 LAW  Fix APPLY_CABLE_DELAYS flag, write map of dummy UDP packets to subfile block 0.
//
// 2.04f-082    2022-06-15 LAW  Additional error checking around config file loader
//
// 2.04f-081    2022-06-15 LAW  Config file loading, heartbeat thread (current disabled), delay tracking fix
//
// 2.04f-080    2022-05-05 BWC  Delay tracking of sources now implemented. Merged channel mapping from GJS patch of build 79
//
// 2.04e-079    2022-05-02 GJS  Minor change of MWAX-to-channel mapping (mwax25 -> Ch02).  Also non-released fork of 'Implement more cable delays from metafits'
//
// 2.04d-078    2022-04-05 BWC  Implement cable delays "CABLEDEL" from metafits and write out fractional delay table to subfile
//
// 2.04c-077    2022-04-05 BWC  Force NINPUT_XGPU to round up the rf inputs number to multiples of 32 inputs (16 dual pol antennas)
//
// 2.04b-076    2022-03-23 BWC  Add observing 'MODE' to log and 'udp buffers used'. Remove the udp individual counts
//				Read ALTAZ table from metafits (3rd HDU) for delay calculations
//
// 2.04a-075    2022-03-16 BWC  Improve reading of metafits files
//                              Add mwax101 back in for development support
//                              Count the number of free files of the wrong size and log it.  (More important now we run !=128T)
//                              Log the number of rf_inputs in each subobs and show 'wanted', 'saw', and 'common' (ie both seen and wanted)
//
// 2.03h-074    2022-03-08 BWC  Change to 112T config (NB: 2.03g-073 was a minor config change GS made)
//
// 2.03f-072    2022-01-19 BWC  Change to Long Baseline configuration
//
// 2.03e-071    2021-12-02 GJS  Update to swap mwax05 back into getting channel 5.
//
// 2.03d-070    2021-11-09 GJS  Update to add breakthrough listen compute node to the channel mapping struct
//
// 2.03c-069    2021-10-26 BWC  Change mwax05 to CC25 (ie stop it from seeing data)
//                              Yet to do: "Also grab RAWSCALE from metafits and stick it in as is into the PSRDADA header.  It’s a decimal value (float?)"
//
// 2.03b-068    2021-10-25 BWC  Handle disappearance (and later restart) of edp packets better
//                              Change RECVMMSG_MODE back to MSG_WAITFORONE so VMA can work more efficiently
//                              Add logging of the number of free file available when about to write out a sub file
//                              Add version and build to PSRDADA header
//
// 2.03a-067    2021-10-20 BWC  Modified default channels to be 1-26 (with a copy of CC10 on mwax25 because mwax10 is in Perth)
//
// 2.02z-066    2021-10-07 GJS  Modified default channels to be 1-12
//
// 2.02y-065    2021-10-06 GJS  Modified default channels for mwax25 and 26
//
// 2.02y-064    2021-10-05 BWC  Trap SIGTERM in addition to SIGINT for shutdown requests
//
// 2.02x-063    2021-10-05 BWC  Change default channels for mwax25 and mwax26
//
// 2.02w-062    2021-10-05 BWC  Fix minor bug with affinity being set differently on mwax15
//
// 2.02v-061    2021-09-22 BWC  Add all genuine mwax servers (mwax01 to mwax26) into config
//
// 2.02u-060    2021-08-19 BWC  Add genuine mwax01 into config and move older servers to 192.168.90.19x addresses for the voltage network
//
// 2.02t-059    2021-03-19 BWC  Check /dev/shm/mwax directory exists to prevent crash
//                              Check /vulcan/metafits directory exists to prevent crash
//                              Add -c coarse channel override command on command line
//                              Included some delay tracking stuff (See below)
//                              Added source offset to packet address during copy
//                              Revert hardcoded sections to Short Baseline.
//                              Update names of mwax servers to mwax106 etc.
//                              Increase the size of the ip string to prevent a compiler warning
//
// 2.02s-058    2021-02-15 BWC  Update layout config for mwax01-mwax07
//
// 2.02r-057    2021-01-11 BWC  Update layout config to Long Baseline *with* RFIpole
//                              Alter the config options to be better suited to CC10 being 256T data volume
//
// 2.02q-056    2021-01-04 BWC  If the FILENAME in the metafits contains 'mwax_vcs' or 'mwax_corr' put in the relevant modes
//                              Update the server config
//
// 2.02p-055    2020-11-25 BWC  Force the MODE to NO_CAPTURE unless the FILENAME in the metafits contains 'HTR'
//
// 2.02n-054    2020-11-19 BWC  Don't set CPU affinity except for on mwax07 and mwax0a
//
// 2.02m-053    2020-11-12 BWC  To aid in debugging, convert NO_CAPTURES to HW_LFILES
//
// 2.02k-052    2020-11-05 BWC  Make "MSG_DONTWAIT" vs "MSG_WAITFORONE" a #define option
//                              Increase the size of the UDP buffers to 8x1024x1024
//                              Print more debug info at closedown
//
// 2.02j-051    2020-11-03 BWC  Add config lines for mwax07 and mwax0a to make it easy to switch coarse channels
//                              Add fflush(stdout) after all relevant printf lines for debug file output
//
// 2.02i-050    2020-10-29 BWC  Make changes to the recvmmsg code to hopefully improve performance.
//
// 2.02h-049    2020-10-23 BWC  Add command line debug option to set debug_mode on. (ie write to .free files instead of .sub files)
//
// 2.02g-048    2020-10-21 BWC  Add selectable cpu affinity per thread
//
// 2.02f-047    2020-10-13 BWC  Add the HP test machine to the configuration
//
// 2.02e-046    2020-09-17 BWC  Rename to mwax_udp2sub for consistency.  Comment out some unused variables to stop compile warnings
//                              Change the cfitsio call for reading strings from ffgsky to ffgkls in hope it fixes the compile on BL server. (Spoiler: It does)
//
// 2.02d-045    2020-09-09 BWC  Switch back to long baseline configuration
//                              Make UDP_num_slots a config variable so different servers can have different values
//                              Do the coarse channel reversal above (chan>=129)
//                              Change MODE to NO_CAPTURE after the observation ends
//
// 2.02c-044    2020-09-08 BWC  Read Metafits directly from vulcan. No more metabin files
//
// 2.02b-043    2020-09-03 BWC  Write ASCII header onto subfiles.  Need to fake some details for now
//
// 2.02a-042    2020-09-03 BWC  Force switch to short baseline tile ids to match medconv
//                              Add heaps of debugs
//
// 2.01a-041    2020-09-02 BWC  Read metabin from directory specified in config
//
// 2.00a-040    2020-08-25 BWC  Change logic to write to .sub file so that udp packet payloads are not copied until after the data arrives for that sub
//                              Clear out a lot of historical code that isn't relevant for actual sub file generation (shifted to other utilities)
//
// 1.00a-039    2020-02-07 BWC  Tell OS not to buffer disk writes.  This should improve memory usage.
//
// 1.00a-038    2020-02-04 BWC  Add feature to support multiple coarse channels arriving from the one input (multicast *or* file) on recsim only
//                              Change recsim to make .sub files with only 1 rf_input.
//
// 1.00a-037    2020-02-03 BWC  Ensure .sub is padded to full size even if tiles are missing.  Improve reading udp packets from file
//
// 1.00a-036    2019-12-10 BWC  Add recsim as a Perth server
//
// 1.00a-035    2019-10-31 BWC  Change to short baseline configuration
//
// 1.00a-034    2019-08-07 BWC  Make output .sub file name better suited for shell script use
//
// 1.00a-033    2019-07-24 BWC  Improve speed in correlate mode by stopping after the relevant data ends
//
// 1.00a-032    2019-07-10 BWC  Add mwax07 to supported server list
//
// 1.00a-031    2019-05-31 BWC  More debug data sent to stdout.  Add support for running on VCS boxes
//
// 1.00a-030    2019-05-27 BWC  Improve debug output during capture
//
// 1.00a-029    2019-05-10 BWC  Continue with sub file creation
//
// 1.00a-028    2019-04-10 BWC  Optionally generate sub files.
//
// 1.00a-027    2019-04-09 BWC  Optionally read the udp packets in from a file.  Clear out some old code that has been commented out for ages.
//
// 1.00a-026    2019-04-09 BWC  Send IP_DROP_MEMBERSHIP when closing.
//
// 1.00a-025    2019-04-04 BWC  Default the port based on hostname. Increase number of buffer slots
//
// 1.00a-024    2019-04-02 BWC  terminate if output file already exists.  Add a slot to the count stats so seconds can differ by 2
//
// 1.00a-023    2019-01-21 BWC  Add details for all MWAX machines
//
// 1.00a-022    2019-01-18 BWC  Add details for IBM Power8 to multicast with
//
// 1.00a-021    2019-01-18 BWC  Add details for Breakthrough Listen machine
//
// 1.00a-020    2018-12-19 BWC  Change the name of the disk file if a start time is listed
//
// 1.00a-019    2018-12-19 BWC  Log inputs and frequencies to screen for ibm
//
// 1.00a-018    2018-12-12 BWC  Add more options for multicast addresses
//
// 1.00a-017    2018-12-05 BWC  Add command line features for start and end time and restrict to one input
//
// 1.00a-016    2018-12-05 BWC  Bug fix to receiver packet size.  Should be payload of 4096 bytes, not 4096 samples (of two bytes each)
//
// 1.00a-015    2018-12-04 BWC  write to disk again
//
// 1.00a-014    2018-11-30 BWC  Make it read from multicast udp
//
// 1.00a-013    2017-07-21 BWC  Remove Even/Odd testing. Provide histograms
//
// 1.00a-012    2017-07-21 BWC  Write out raw data to binary dump file
//
// 1.00a-011    2017-07-20 BWC  Comment out histogram and change file write cadence to be time based
//
// 1.00a-010    2017-07-20 BWC  Partially process the packet so that we can keep up with high data rates
//
// 1.00a-009    2017-07-06 BWC  Update filename every dump to store files continuously
//
// 1.00a-008    2017-07-05 BWC  Dump power levels to file for both inputs and change scale to average per sample, not average per UDP packet
//
// 1.00a-007    2017-07-04 BWC  Dump power levels to file
//
// 1.00a-006    2017-07-03 BWC  Added PPD style power monitoring
//
// 1.00a-005    2017-06-09 BWC  Still adding core functions. Builds only used to test code portions.
//
// 1.00a-001    2017-05-25 BWC  The Beginning!
//
//===================================================================================================================================================
//
// To compile:  gcc mwax_udp2sub_xx.c -omwax_u2s -lpthread -Ofast -march=native -lrt -lcfitsio -Wall
//      or      gcc mwax_udp2sub_xx.c -o../mwax_u2s -lpthread -Ofast -march=native -lrt -lcfitsio -Wall
//              There should be NO warnings or errors on compile!
//
// To run:      numactl --cpunodebind=1 --membind=1 dd bs=4096 count=1288001 if=/dev/zero of=/dev/shm/mwax/a.free
//              numactl --cpunodebind=1 --membind=1 ./mwax_u2s
//
// To run:      From Helios type:
//                      for i in {01..06};do echo mwax$i;ssh mwax$i 'cd /data/solarplatinum;numactl --cpunodebind=0 --membind=0 ./udp2sub -c 23 > output.log &';done
//                      for i in {01..06};do echo mwax$i;ssh mwax$i 'cat /data/solarplatinum/20190405.log';done
//
//===================================================================================================================================================
//
// To do:               Too much to say!

#define _GNU_SOURCE

#include <sys/types.h>
#include <netinet/in.h>
#include <arpa/inet.h>

#include <netinet/ip.h>
#include <sys/socket.h>

#include <time.h>
#include <stdio.h>
#include <pthread.h>
#include <stdlib.h> 
#include <signal.h>
#include <string.h>
#include <math.h>
#include <unistd.h>
#include <errno.h>
#include <fcntl.h>
#include <dirent.h>
#include <float.h>

#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/shm.h>

#include <fitsio.h>

#include "mwax_udp2sub.h"
#include "vec3.h"

#define ERR_MSG_LEN 256

const char *TAG = "main";

//---------------- and some new friends -------------------


#define FITS_CHECK(OP) if (status) { fprintf(stderr, "Error in metafits access (%s): ", OP); fits_report_error(stderr, status); status = 0; }
#ifdef DEBUG
#define DEBUG_LOG(...) fprintf(stderr, __VA_ARGS__)
#else
#define DEBUG_LOG(...)
#endif

app_state_t state;



void *add_meta_fits_thread() {
  add_meta_fits();
  pthread_exit(NULL);
}



//---------------------------------------------------------------------------------------------------------------------------------------------------

void sigint_handler(int signo) {
  printf("\n\nAsked to shut down via SIGINT\n");
  fflush(stdout);
  state.terminate = true;                             // This is a volatile, file scope variable.  All threads should be watching for this.
}

void sigterm_handler(int signo) {
  printf("\n\nAsked to shut down via SIGTERM\n");
  fflush(stdout);
  state.terminate = true;                             // This is a volatile, file scope variable.  All threads should be watching for this.
}

void sigusr1_handler(int signo) {
  printf("\n\nSomeone sent a sigusr1 signal to me\n");
  fflush(stdout);
}


// Bad command line.  Report the supported usage.
void usage(char *err) {
  printf( "%s", err );
  printf("\n\n To run:        /home/mwa/udp2sub -f mwax.conf -i 1\n\n");
  printf("                    -f Configuration file to use for settings\n");
  printf("                    -i Instance number on server, if multiple copies per server in use\n");
  printf("                    -c Coarse channel override\n");
  printf("                    -d Debug mode.  Write to .free files\n");
  fflush(stdout);
}

int main(int argc, char **argv) {
  log_info("mwax_udp2sub v%s (build %d)", THISVER, BUILD);
  log_info("Starting up...");

  // Initialise the application state
  state.hostname = calloc(HOSTNAME_LENGTH, 1);
  state.msgvecs = NULL;
  state.iovecs = NULL;
  state.UDPbuf = NULL;
  state.UDP_added_to_buff = 0;
  state.UDP_removed_from_buff = 0;
  state.UDP_num_slots = 0;
  state.terminate = false;
  state.GPS_offset = 315964782;
  state.sub = NULL;
  state.blank_sub_line = NULL;
  state.sub_header = NULL;
  state.debug_mode = false;
  state.force_cable_delays = false;
  state.force_geo_delays = false;
  state.force_channel_id = false;
  state.force_instance_id = false;
  state.prog_build = BUILD;
  state.instance_id = 0;         // Assume we're the first (or only) instance on this server
  state.chan_id_override = 0;    // Assume we are going to use the default coarse channel for this instance
  state.config_path = "/vulcan/mwax_config/mwax_u2s.cfg";
  state.delaygen_cfg.obsid = 0;
  state.delaygen_cfg.subobs_idx = 0;
  state.delaygen_cfg.enabled = false;

  // Parse command line
  while (argc > 1 && argv[1][0] == '-') {
    switch (argv[1][1]) {
      case 'd':
        state.debug_mode = true;
        log_info("Enabling debug mode." );
        break ;
      case 'i':
        ++argv; --argc;
        state.force_instance_id = true;
        state.instance_id_override = atoi(argv[1]);
        log_warning("Using instance ID override: %s", state.instance_id_override);
        break;
      case 'c':
        ++argv; --argc;
        state.force_channel_id = true;
        state.chan_id_override = atoi(argv[1]);
        log_warning("Using channel ID override: %s", state.chan_id_override);
        break;
      case 'f':
        ++argv; --argc;
        state.config_path = argv[1];
        break ;
      case 'C':
        state.force_cable_delays = true;
        log_warning("Cable delay override is active. All observations will have cable delays applied." );
        break;
      case 'G':
        state.force_geo_delays = true;
        log_warning("Geometric delay override is active. All observations will have geometric delays applied." );
        break;
      case 'g':
        state.delaygen_cfg.enabled = true;
        break;
      case 'o':
        ++argv; --argc;
        state.delaygen_cfg.obsid = atoi(argv[1]);
        break;
      case 's':
        ++argv; --argc;
        state.delaygen_cfg.subobs_idx = atoi(argv[1]);
        break;
      default:
        usage("unknown option") ;
        exit(EXIT_FAILURE);
    }
    --argc;
    ++argv;
  }
  if (argc > 1) {                             // There is/are at least one command line option we don't understand
    usage("");                                // Print the available options
    exit(EXIT_FAILURE);
  }

  log_info("Using configuration database file: %s", state.config_path );

  // Attempt to get local hostname, defaulting to "unknown"
  if(gethostname( state.hostname, HOSTNAME_LENGTH) == -1)
    strcpy(state.hostname, "unknown");

  load_config(&state);
  if ( state.cfg.udp2sub_id == 0 ) {                                       // If the lookup returned an id of 0, we don't have enough information to continue
    fprintf(stderr, "Hostname not found in configuration\n");
    fflush(stdout);
    exit(EXIT_FAILURE);                                               // Abort!
  }
  signal(SIGINT, sigint_handler);                     // Tell the OS that we want to trap SIGINT calls
  signal(SIGTERM, sigterm_handler);                   // Tell the OS that we want to trap SIGTERM calls
  signal(SIGUSR1, sigusr1_handler);                   // Tell the OS that we want to trap SIGUSR1 calls

//---------------- Allocate the RAM we need for the incoming udp buffer and initialise it ------------------------

    state.UDP_num_slots = state.cfg.UDP_num_slots; // We moved this to a config variable, but most of the code still assumes the old name so make both names valid

    // In delay geneator mode, we don't really need to buffer any packets, but we'll let the structures be populated
    if(state.delaygen_cfg.enabled) {
      state.UDP_num_slots = 10;
    }

    state.msgvecs = calloc( 2 * state.UDP_num_slots, sizeof(struct mmsghdr));      // NB Make twice as big an array as the number of actual UDP packets we are going to buffer
    if ( state.msgvecs == NULL ) {                                            // If that failed
      printf("msgvecs calloc failed\n");                                // log a message
      fflush(stdout);
      exit(EXIT_FAILURE);                                               // and give up!
    }

    state.iovecs = calloc( state.UDP_num_slots, sizeof(struct iovec));             // NB Make the *same* number of entries as the number of actual UDP packets we are going to buffer
    if (state. iovecs == NULL ) {                                             // If that failed
      printf("iovecs calloc failed\n");                                 // log a message
      fflush(stdout);
      exit(EXIT_FAILURE);                                               // and give up!
    }

    state.UDPbuf = calloc( state.UDP_num_slots, sizeof(pkt_data_t));       // NB Make the *same* number of entries as the number of actual UDP packets we are going to buffer
    if ( state.UDPbuf == NULL ) {                                             // If that failed
      printf("UDPbuf calloc failed\n");                                 // log a message
      fflush(stdout);
      exit(EXIT_FAILURE);                                               // and give up!
    }

//---------- Now initialize the arrays

    for (int loop = 0; loop < state.UDP_num_slots; loop++) {

        state.iovecs[loop].iov_base = &state.UDPbuf[loop];
        state.iovecs[loop].iov_len = sizeof(pkt_data_t);
        state.msgvecs[loop].msg_hdr.msg_iov = &state.iovecs[loop];                // Populate the first copy
        state.msgvecs[loop].msg_hdr.msg_iovlen = 1;
        state.msgvecs[loop + state.UDP_num_slots].msg_hdr.msg_iov = &state.iovecs[loop];        // Now populate a second copy of msgvecs that point back to the first set
        state.msgvecs[loop + state.UDP_num_slots].msg_hdr.msg_iovlen = 1;                 // That way we don't need to worry (as much) about rolling over buffers during reads
    }

//---------------- Allocate the RAM we need for the subobs pointers and metadata and initialise it ------------------------

    state.sub = calloc( 4, sizeof( subobs_udp_meta_t ) );                     // Make 4 slots to store the metadata against the maximum 4 subobs that can be open at one time
    if ( state.sub == NULL ) {                                                // If that failed
      printf("sub calloc failed\n");                                    // log a message
      fflush(stdout);
      exit(EXIT_FAILURE);                                               // and give up!
    }

    state.blank_sub_line = calloc( SUB_LINE_SIZE, sizeof(char));              // Make ourselves a buffer of zeros that's the size of an empty line.  We'll use this for padding out sub file
    if ( state.blank_sub_line == NULL ) {                                     // If that failed
      printf("blank_sub_line calloc failed\n");                         // log a message
      fflush(stdout);
      exit(EXIT_FAILURE);                                               // and give up!
    }

    state.sub_header = calloc( SUBFILE_HEADER_SIZE, sizeof(char));      // Make ourselves a buffer that's initially of zeros that's the size of a sub file header.
    if ( state.sub_header == NULL ) {                                         // If that failed
      printf("sub_header calloc failed\n");                             // log a message
      fflush(stdout);
      exit(EXIT_FAILURE);                                               // and give up!
    }

    // Enter delay generator if enabled, then quit
    if(state.delaygen_cfg.enabled) {
      return delaygen(state.delaygen_cfg.obsid, state.delaygen_cfg.subobs_idx);
    }


//---------------- We're ready to fire up the four worker threads now ------------------------

    fprintf(stderr, "Firing up pthreads\n");
    fflush(stdout);

    pthread_t UDP_recv_pt;
    pthread_create(&UDP_recv_pt,NULL,task_receive_buffer,NULL);                    // Fire up the process to receive the udp packets into the large buffers we just created

    pthread_t UDP_parse_pt;
    pthread_create(&UDP_parse_pt,NULL,task_parse_udp,NULL);                  // Fire up the process to parse the udp packets and generate arrays of sorted pointers

    pthread_t makesub_pt;
    pthread_create(&makesub_pt,NULL,task_make_sub,NULL);                      // Fire up the process to generate sub files from raw packets and pointers

    pthread_t monitor_pt;
    pthread_create(&monitor_pt,NULL,task_heartbeat,NULL);

//---------------- The threads are off and running.  Now we just wait for a message to terminate, like a signal or a fatal error ------------------------

    printf("Master thread switching to metafits reading.\n");
    fflush(stdout);

//--------------- Set CPU affinity ---------------

    printf("Set process parent cpu affinity returned %d\n", set_cpu_affinity ( conf.cpu_mask_parent ) );
    fflush(stdout);

    add_meta_fits();

//    while(!terminate) sleep(1);                 // The master thread currently does nothing! Zip! Nada!  What a waste eh?

//---------------- Clean up the mess we made and go home ------------------------

    printf("Master thread waiting for child threads to end.\n");
    fflush(stdout);

    pthread_join(UDP_recv_pt,NULL);
    printf("UDP_recv joined.\n");
    fflush(stdout);

    pthread_join(UDP_parse_pt,NULL);
    printf("UDP_parse joined.\n");
    fflush(stdout);

    pthread_join(makesub_pt,NULL);
    printf("makesub joined.\n");
    fflush(stdout);

//---------- The other threads are all closed down now. Perfect opportunity to have a look at the memory status (while nobody is changing it in the background ----------

    subobs_udp_meta_t *subm;                                            // pointer to the sub metadata array I'm looking at

    for ( int loop = 0 ; loop < 4 ; loop++ ) {                          // Look through all four subobs meta arrays
      subm = &sub[ loop ];                                              // Temporary pointer to our sub's metadata array

      printf( "slot=%d,so=%d,st=%d,wait=%d,took=%d,first=%lld,last=%lld,startw=%lld,endw=%lld,count=%d,seen=%d\n",
            loop, subm->subobs, subm->state, subm->msec_wait, subm->msec_took, subm->first_udp, subm->last_udp, subm->udp_at_start_write, subm->udp_at_end_write, subm->udp_count, subm->rf_seen );
    }

    printf( "\n" );                                                     // Blank line

    pkt_data_t *my_udp;                                           // Make a local pointer to the UDP packet we're going to be working on.
    uint32_t subobs_mask = 0xFFFFFFF8;                                  // Mask to apply with '&' to get sub obs from GPS second

    int64_t UDP_closelog = state.UDP_removed_from_buff - 20;                    // Go back 20 udp packets

    if ( state.debug_mode ) {                                                 // If we're in debug mode
      UDP_closelog = state.UDP_removed_from_buff - 100;                      // go back 100!
    }

    if ( UDP_closelog < 0 ) UDP_closelog = 0;                           // In case we haven't really started yet

    while ( UDP_closelog <= state.UDP_removed_from_buff ) {
      my_udp = &state.UDPbuf[ UDP_closelog % state.UDP_num_slots ];
      printf( "num=%lld,slot=%d,freq=%d,rf=%d,time=%d:%d,e2u=%d:%d\n",
        UDP_closelog,
        (( my_udp->GPS_time >> 3 ) & 0b11),
        my_udp->freq_channel,
        my_udp->rf_input,
        (my_udp->GPS_time & subobs_mask ),
        my_udp->subsec_time,
        my_udp->edt2udp_id,
        my_udp->edt2udp_token
      );

      UDP_closelog++;
    }

// WIP Print out the next 20 udp buffers starting from UDP_removed_from_buffer (assuming they exist).
// Before printing them do the ntoh conversion for all relevant fields
// This will cause UDP_removed_from_buffer itself to be printed twice, once with and once without conversion

//---------- Free up everything from the heap ----------

    free( state.msgvecs );                                                    // The threads are dead, so nobody needs this memory now. Let it be free!
    free( state.iovecs );                                                     // Give back to OS (heap)
    free( state.UDPbuf );                                                     // This is the big one.  Probably many GB!

    free( state.sub );                                                        // Free the metadata array storage area

    printf("Exiting process\n");
    fflush(stdout);

    exit(EXIT_SUCCESS);
}

// End of code.  Comments, snip and examples only follow
