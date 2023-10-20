/** mwax_udp2sub - UDP packet capture and processing for the MWAX correlator
  *
  * Captures UDP packets into a temporary, user space buffer and then sort, 
  * copy and process them into an shared memory output buffer for further
  * processing.
  *
  * Authors: 
  *   LAW Luke Williams luke.a.williams@curtin.edu.au (current maintainer)
  *   BWC Brian Crosse brian.crosse@curtin.edu.au (inactive)
  * 
  * Commenced 2017-05-25
  */

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




app_state_t state;



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

    state.sub = calloc( 4, sizeof( subobs_udp_meta_t ) );               // Make 4 slots to store the metadata against the maximum 4 subobs that can be open at one time
    if ( state.sub == NULL ) {                                          // If that failed
      printf("sub calloc failed\n");                                    // log a message
      fflush(stdout);                                                   // Flush the output buffer
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
