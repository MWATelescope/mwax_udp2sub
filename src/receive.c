#define _GNU_SOURCE

#include <stdint.h>
#include <stdio.h>
#include <stdbool.h>
#include <sys/socket.h>
#include <netinet/ip.h>

#include "mwax_udp2sub.h"

const char *TAG = "udp-buffer";

/** UDP receive buffering thread.
 * Pulls UDP packets out of the kernel and place them in a large user-space circular buffer.
 */
void *task_receive_buffer(void *args) {
    app_state_t *state = (app_state_t *)args;
    instance_config_t conf = state->cfg;

    log_info("Initialising...");
    set_cpu_affinity(TAG, conf.cpu_mask_UDP_recv);

    int64_t UDP_slots_empty;                                            // How much room is left unused in the application's UDP receive buffer
    int64_t UDP_first_empty;                                            // Index to the first empty slot 0 to (UDP_num_slots-1)
    int64_t UDP_slots_empty_min = conf.UDP_num_slots + 1 ;              // what's the smallest number of empty slots we've seen this batch?  (Set an initial value that will always be beaten)
    int64_t Num_loops_when_full = 0 ;                                   // How many times (since program start) have we checked if there was room in the buffer and there wasn't any left  :-(
    int retval;                                                         // General return value variable.  Context dependant.

    log_info("Setting up multicast listener for %s:%d on interface %s", conf.multicast_ip, conf.multicast_port, conf.local_if);

    struct sockaddr_in addr;                                            // Standard socket setup stuff needed even for multicast UDP
    memset(&addr, 0, sizeof(addr));
    int fd;
    struct ip_mreq mreq;

    if ((fd=socket(AF_INET,SOCK_DGRAM,0)) == -1) {                      // create what looks like an ordinary UDP socket
      perror("socket");
      state->terminate = true;
      pthread_exit(NULL);
    }

    addr.sin_family=AF_INET;
    addr.sin_addr.s_addr=htonl(INADDR_ANY);
    addr.sin_port=htons(conf.multicast_port);

    if (setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &(int){1}, sizeof(int)) == -1) {
      perror("setsockopt SO_REUSEADDR");
      close(fd);
      state->terminate = true;
      pthread_exit(NULL);
    }

    if (bind(fd,(struct sockaddr *) &addr,sizeof(addr)) == -1) {        // bind to the receive address
      perror("bind");
      close(fd);
      state->terminate = true;
      pthread_exit(NULL);
    }

    mreq.imr_multiaddr.s_addr=inet_addr( conf.multicast_ip );                   // use setsockopt() to request that the kernel join a multicast group
    mreq.imr_interface.s_addr=inet_addr( conf.local_if );

    if (setsockopt(fd,IPPROTO_IP,IP_ADD_MEMBERSHIP,&mreq,sizeof(mreq)) == -1) {
      perror("setsockopt IP_ADD_MEMBERSHIP");
      close(fd);
      state->terminate = true;
      pthread_exit(NULL);
    }

    log_info("Ready to receive packets." );

    UDP_slots_empty = conf.UDP_num_slots;                               // We haven't written to any yet, so all slots are available at the moment
    UDP_first_empty = 0;                                                // The first of those is number zero
    struct mmsghdr *UDP_first_empty_ptr = &state->msgvecs[UDP_first_empty];    // Set our first empty pointer to the address of the 0th element in the array

    while (!state->terminate) {
      if ( UDP_slots_empty > 0 ) {              // There's room for at least 1 UDP packet to arrive!  We should go ask the OS for it.

        if ( ( retval = recvmmsg( fd, UDP_first_empty_ptr, UDP_slots_empty, MSG_WAITFORONE, NULL ) ) == -1 ) 
          if ( ( retval = recvmmsg( fd, UDP_first_empty_ptr, UDP_slots_empty, MSG_WAITFORONE, NULL ) ) == -1 ) 
            if ( ( retval = recvmmsg( fd, UDP_first_empty_ptr, UDP_slots_empty, MSG_WAITFORONE, NULL ) ) == -1 ) 
              if ( ( retval = recvmmsg( fd, UDP_first_empty_ptr, UDP_slots_empty, MSG_WAITFORONE, NULL ) ) == -1 ) 
                if ( ( retval = recvmmsg( fd, UDP_first_empty_ptr , UDP_slots_empty, MSG_WAITFORONE, NULL ) ) == -1 ) continue;

        state->UDP_added_to_buff += retval;                                          // Add that to the number we've ever seen and placed in the buffer
      } else {
        Num_loops_when_full++;
      }

      UDP_slots_empty = state->UDP_num_slots + state->UDP_removed_from_buff - state->UDP_added_to_buff;   // How many UDP slots are available for us to (ask to) read using the one recvmmsg() request?
      if ( UDP_slots_empty < UDP_slots_empty_min ) UDP_slots_empty_min = UDP_slots_empty;       // Do we have a new winner of the "What's the minimum number of empty slots available" prize?  Debug use only.  Not needed
      UDP_first_empty = ( state->UDP_added_to_buff % state->UDP_num_slots );                // The index from 0 to (UDP_num_slots-1) of the first available UDP packet slot in the buffer
      UDP_first_empty_ptr = &state->msgvecs[UDP_first_empty];
    }

    log_info("Termination signal received. Cleaning up...");
    log_debug("Receive loop was unable to process packets due to a full buffer %lld times.", Num_loops_when_full);
    log_debug("At its fullest, the buffer had %lld free slots.", UDP_slots_empty_min);

    mreq.imr_multiaddr.s_addr=inet_addr( conf.multicast_ip );
    mreq.imr_interface.s_addr=inet_addr( conf.local_if );

    if (setsockopt(fd,IPPROTO_IP,IP_DROP_MEMBERSHIP,&mreq,sizeof(mreq)) == -1) {  // Say we don't want multicast packets any more.
      perror("setsockopt IP_DROP_MEMBERSHIP");
      close(fd);
      pthread_exit(NULL);
    }
    close(fd);                                                                  // Close the file descriptor for the port now we're about the leave

    log_info("Receive buffer is shutting down");
    pthread_exit(NULL);
}