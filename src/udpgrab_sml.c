//===================================================================================================================================================
// Capture UDP packets into a temporary, user space buffer and then sort/copy/process them into some output array
//
// Author(s)  BWC Brian Crosse brian.crosse@curtin.edu.au
// Commenced 2017-05-25
//
// 2.00a-001	2019-08-14 BWC	Fork Code from udp2sub.c to generate udpgrab_sml.  Remove as many external dependencies as possible so it runs stand alone.
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
// To Compile:  gcc udpgrab_sml.c -oudpgrab_sml -lpthread -Ofast -march=native
//              There should be NO warnings or errors on compile!
//
// To run:      From Helios type:
//                      for i in {01..06};do echo mwax$i;ssh mwax$i 'cd /data/solarplatinum;numactl --cpunodebind=0 --membind=0 ./udpgrab_sml -v -s 1238470810 -e 1238470976 > 20190405.log &';done
//                      for i in {01..06};do echo mwax$i;ssh mwax$i 'cat /data/solarplatinum/20190405.log';done
//
//===================================================================================================================================================
//
// To do:               Too much to say!

#define BUILD 1

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
#include <unistd.h>
#include <errno.h>
#include <fcntl.h>

//---------------- Define our old friends -------------------

#define FALSE 0
#define TRUE !(FALSE)

#define BOOL int
#define INT8 signed char
#define UINT8 unsigned char
#define INT16 short
#define UINT16 unsigned short
#define INT32 int
#define UINT32 unsigned int
#define INT64 long long int
#define UINT64 unsigned long long int

#define ERR_MSG_LEN 256

//---------------- Structure definitions --------------------

#pragma pack(push,1)                            // We're parsing this header directly off the comms line, so we want/need to force the compiler not to add it's own idea of structure padding
typedef struct ni_packet {                              // Format of a National Instruments UDP packet

    uint8_t packet_type;                      // Packet type (0x00 == Binary Header, 0x20 == 2K samples of RRI Voltage Data in complex 8 bit real + 8 bit imaginary format)
    uint8_t freq_channel;                     // The current coarse channel frequency number [0 to 255 inclusive].  This number is available from the header, but repeated here to simplify archiving and monitoring multiple frequencies.
    uint16_t rf_input;                        // maps to tile/antenna and polarisation (LSB is 0 for X, 1 for Y)
    uint32_t GPS_time;                        // [second] GPS time of packet (bottom 32 bits only)
    uint16_t subsec_time;                     // count from 0 to PACKETS_PER_SEC - 1 (from header)  Typically PACKETS_PER_SEC is 625 in MWA
    uint8_t spare1;                           // spare padding byte.  Reserved for future use.
    uint8_t edt2udp_id;                       // Which edt2udp instance generated this packet and therefore a lookup to who to ask for a retransmission (0 implies no retransmission possible)
    uint16_t edt2udp_token;                   // value to pass back to edt2udp instance to help identify source packet.  A token's value is constant for a given source rf_input and freq_channel for entire sub-observation
    uint8_t spare2[2];                        // Spare bytes for future use

    UINT16 payload[2048];                               // Need to work out which bytes are what WIP!!!  NI explanation of 8r, 8i, 8r, 8i etc is not consistent with the values seen

//    int8_t volt[2048][2];                     // 2048 complex voltage samples.  Each one is 8 bits real, followed by 8 bits imaginary

} ni_packet_t;
#pragma pack(pop)                               // Set the structure packing back to 'normal' whatever that is

typedef struct pckt_count {                   // Array to store arrived packet statistics on packets per second

    uint32_t GPS_time;                        // [second] GPS time of packet (bottom 32 bits only)
    uint32_t count;                           // How many have we seen?
//    uint64_t first_seen;                      // when was it first seen
    uint32_t ICS;                               // incoherent sum of the first sample in each packet for a source

} pckt_count_t;

//---------------------------------------------------------------------------------------------------------------------------------------------------
// These variables are shared by all threads.  "file scope"
//---------------------------------------------------------------------------------------------------------------------------------------------------

#define CLOSEDOWN (0xFFFFFFFF)

volatile BOOL terminate = FALSE;                        // Global request for everyone to close down

//cpu_set_t physical_id_0, physical_id_1;                       // Make a CPU affinity set for socket 0 and one for socket 1 (NUMA node0 and node1)

volatile INT64 UDP_added_to_buff = 0;                   // Total number of packets received and placed in the buffer.  If it overflows we are in trouble and need a restart.
volatile INT64 UDP_removed_from_buff = 0;               // Total number of packets pulled from buffer and space freed up.  If it overflows we are in trouble and need a restart.

volatile BOOL verbose = FALSE;                          // Default to minimal debug messages
volatile BOOL correlate = FALSE;                        // Default to not generating sub files for the correlator

int UDPport = 59000;                                    // UDP port to listen to.

uint32_t start_capture_time = 0;                        // Unless overridden on the command line, start capture immediately
uint32_t end_capture_time = 2147000000;                 // Unless overridden on the command line, end capture a long time in the future

uint32_t first_gps_start_capture_time = 0;

int numanode = 0;                                       // which Numa node to force memory allocations on.
//INT64 UDP_num_slots = 256;                           // default to 4096 UDP slots in the buffer
//INT64 UDP_num_slots = 4096;                           // default to 4096 UDP slots in the buffer
INT64 UDP_num_slots = 10000000;                          // MOAR!!
int UDPtype = 1;                                        // UDP packet type. Default to NI format

struct mmsghdr *msgvecs;
struct iovec *iovecs;
ni_packet_t *UDPbuf;

char *my_path = "";                                     // Pointer to my path
char *readfile = "";

int server = 0;                                         // which server are we running on

char *local_interface_medconv01 = "192.168.90.122";                             // The local NIC port we want the multicast traffic to arrive on
char *local_interface_mwax00 = "192.168.90.210";                                // The local NIC port we want the multicast traffic to arrive on
char *local_interface_mwax01 = "192.168.90.201";                                // The local NIC port we want the multicast traffic to arrive on
char *local_interface_mwax02 = "192.168.90.202";                                // The local NIC port we want the multicast traffic to arrive on
char *local_interface_mwax03 = "192.168.90.203";                                // The local NIC port we want the multicast traffic to arrive on
char *local_interface_mwax04 = "192.168.90.204";                                // The local NIC port we want the multicast traffic to arrive on
char *local_interface_mwax05 = "192.168.90.205";                                // The local NIC port we want the multicast traffic to arrive on
char *local_interface_mwax06 = "192.168.90.206";                                // The local NIC port we want the multicast traffic to arrive on
char *local_interface_mwax07 = "192.168.90.207";                                // The local NIC port we want the multicast traffic to arrive on
char *local_interface_blc00 = "192.168.90.240";                                // The local NIC port we want the multicast traffic to arrive on
char *local_interface_ibm = "192.168.90.212";
char *local_interface_vcs01 = "202.9.9.131";
char *local_interface_vcs02 = "202.9.9.132";
char *local_interface_vcs03 = "202.9.9.133";
char *local_interface_vcs04 = "202.9.9.134";
char *local_interface_vcs05 = "202.9.9.135";
char *local_interface_vcs06 = "202.9.9.136";
char *local_interface_vcs07 = "202.9.9.137";
char *local_interface_vcs08 = "202.9.9.138";
char *local_interface_vcs09 = "202.9.9.139";
char *local_interface_vcs10 = "202.9.9.140";
char *local_interface_vcs11 = "202.9.9.141";
char *local_interface_vcs12 = "202.9.9.142";
char *local_interface_vcs13 = "202.9.9.143";
char *local_interface_vcs14 = "202.9.9.144";
char *local_interface_vcs15 = "202.9.9.145";
char *local_interface_vcs16 = "202.9.9.146";

char *local_interface;

#define PAYLOAD_SIZE (4096LL)
#define FILE_HEADER_SIZE (4096LL)
#define BLOCK_SIZE (26214400LL)
#define SEC_OFFSET (655360000LL)
#define PACKET_OFFSET (4096LL)
#define LINE_OFFSET (102400LL)

// Pointers to three buffers that together will form a 3-element ring buffer for assembling successive sub files
INT8 * sub_a;
INT8 * sub_b;
INT8 * sub_c;
INT8 * previous_sub_buffer;
INT8 * current_sub_buffer;
INT8 * next_sub_buffer;


//===================================================================================================================================================
// THREAD BLOCK.  The following functions are complete threads
//===================================================================================================================================================

//---------------------------------------------------------------------------------------------------------------------------------------------------
// UDP_recv - Pull UDP packets out of the kernel and place them in a large user-space circular buffer
//---------------------------------------------------------------------------------------------------------------------------------------------------

void *UDP_recv()
{
    printf("UDP_recv started\n");

//---------------- Initialize and declare variables ------------------------

    INT64 UDP_slots_empty;                                              // How much room is left unused in the application's UDP receive buffer
    INT64 UDP_first_empty;                                              // Index to the first empty slot 0 to (UDP_num_slots-1)

    INT64 UDP_slots_empty_min = UDP_num_slots + 1 ;                     // what's the smallest number of empty slots we've seen this batch?  (Set an initial value that will always be beaten)

    INT64 Num_loops_when_full = 0 ;                                     // How many times (since program start) have we checked if there was room in the buffer and there wasn't any left  :-(

    int retval;                                                         // General return value variable.  Context dependant.

//--------------------------------

    char *multicast_ip = "239.255.90.0";                                // Multicast address we will join

    if ( UDPport == 59001 ) multicast_ip = "239.255.90.1";              // Multicast address we will join
    if ( UDPport == 59002 ) multicast_ip = "239.255.90.2";              // Multicast address we will join
    if ( UDPport == 59003 ) multicast_ip = "239.255.90.3";              // Multicast address we will join
    if ( UDPport == 59004 ) multicast_ip = "239.255.90.4";              // Multicast address we will join
    if ( UDPport == 59005 ) multicast_ip = "239.255.90.5";              // Multicast address we will join
    if ( UDPport == 59006 ) multicast_ip = "239.255.90.6";              // Multicast address we will join
    if ( UDPport == 59007 ) multicast_ip = "239.255.90.7";              // Multicast address we will join
    if ( UDPport == 59008 ) multicast_ip = "239.255.90.8";              // Multicast address we will join
    if ( UDPport == 59009 ) multicast_ip = "239.255.90.9";              // Multicast address we will join
    if ( UDPport == 59010 ) multicast_ip = "239.255.90.10";             // Multicast address we will join
    if ( UDPport == 59011 ) multicast_ip = "239.255.90.11";             // Multicast address we will join
    if ( UDPport == 59012 ) multicast_ip = "239.255.90.12";             // Multicast address we will join
    if ( UDPport == 59013 ) multicast_ip = "239.255.90.13";             // Multicast address we will join
    if ( UDPport == 59014 ) multicast_ip = "239.255.90.14";             // Multicast address we will join
    if ( UDPport == 59015 ) multicast_ip = "239.255.90.15";             // Multicast address we will join
    if ( UDPport == 59016 ) multicast_ip = "239.255.90.16";             // Multicast address we will join
    if ( UDPport == 59017 ) multicast_ip = "239.255.90.17";             // Multicast address we will join
    if ( UDPport == 59018 ) multicast_ip = "239.255.90.18";             // Multicast address we will join
    if ( UDPport == 59019 ) multicast_ip = "239.255.90.19";             // Multicast address we will join
    if ( UDPport == 59020 ) multicast_ip = "239.255.90.20";             // Multicast address we will join
    if ( UDPport == 59021 ) multicast_ip = "239.255.90.21";             // Multicast address we will join
    if ( UDPport == 59022 ) multicast_ip = "239.255.90.22";             // Multicast address we will join
    if ( UDPport == 59023 ) multicast_ip = "239.255.90.23";             // Multicast address we will join
    if ( UDPport == 59024 ) multicast_ip = "239.255.90.24";             // Multicast address we will join

    printf ( "Receiving from multicast %s:%d on interface %s\n", multicast_ip, UDPport, local_interface );

    struct sockaddr_in addr;                                            // Standard socket setup stuff needed even for multicast UDP
    memset(&addr,0,sizeof(addr));

    int fd;
    struct ip_mreq mreq;

    // create what looks like an ordinary UDP socket
    if ((fd=socket(AF_INET,SOCK_DGRAM,0)) < 0) {
      perror("socket");
      pthread_exit(NULL);
    }

    addr.sin_family=AF_INET;
    addr.sin_addr.s_addr=htonl(INADDR_ANY);
    addr.sin_port=htons(UDPport);

    if (setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &(int){1}, sizeof(int)) == -1)
    {
      perror("setsockopt SO_REUSEADDR");
      close(fd);
      exit(-1);
    }

    // bind to the receive address
    if (bind(fd,(struct sockaddr *) &addr,sizeof(addr)) < 0) {
      perror("bind");
      pthread_exit(NULL);
    }

    mreq.imr_multiaddr.s_addr=inet_addr(multicast_ip);
    mreq.imr_interface.s_addr=inet_addr(local_interface);
    if (setsockopt(fd,IPPROTO_IP,IP_ADD_MEMBERSHIP,&mreq,sizeof(mreq)) < 0) {
      perror("setsockopt IP_ADD_MEMBERSHIP");
      pthread_exit(NULL);
    }

//--------------------------------------------------------------------------------------------------------------------------------------------------------

    printf( "Ready to start\n" );

    UDP_slots_empty = UDP_num_slots;                                    // We haven't written to any yet, so all slots are available at the moment
    UDP_first_empty = 0;                                                // The first of those is number zero
    struct mmsghdr *UDP_first_empty_ptr = &msgvecs[UDP_first_empty];

    while (!terminate) {

      if ( UDP_slots_empty > 0 ) {                                              // There's room for at least 1 UDP packet to arrive!  We should go ask the OS for it.

        if ( ( retval = recvmmsg( fd, UDP_first_empty_ptr, UDP_slots_empty, MSG_DONTWAIT, NULL ) ) == -1 )                              //
          if ( ( retval = recvmmsg( fd, UDP_first_empty_ptr, UDP_slots_empty, MSG_DONTWAIT, NULL ) ) == -1 )                            //
            if ( ( retval = recvmmsg( fd, UDP_first_empty_ptr, UDP_slots_empty, MSG_DONTWAIT, NULL ) ) == -1 )                          //
              if ( ( retval = recvmmsg( fd, UDP_first_empty_ptr, UDP_slots_empty, MSG_DONTWAIT, NULL ) ) == -1 )                        //
                if ( ( retval = recvmmsg( fd, UDP_first_empty_ptr , UDP_slots_empty, MSG_DONTWAIT, NULL ) ) == -1 ) continue;

        UDP_added_to_buff += retval;                                          // Add that to the number we've ever seen and placed in the buffer

      } else {
        Num_loops_when_full++;
      }

      UDP_slots_empty = UDP_num_slots + UDP_removed_from_buff - UDP_added_to_buff;   // How many UDP slots are available for us to (ask to) read using the one recvmmsg() request?

      if ( UDP_slots_empty < UDP_slots_empty_min ) UDP_slots_empty_min = UDP_slots_empty;       // Do we have a new winner of the "What's the minimum number of empty slots available" prize?  Debug use only.  Not needed

      UDP_first_empty = ( UDP_added_to_buff % UDP_num_slots );                // The index from 0 to (UDP_num_slots-1) of the first available UDP packet slot in the buffer
      UDP_first_empty_ptr = &msgvecs[UDP_first_empty];

    }

    sleep(1);
    printf( "looped on full %lld times.  min = %lld\n", Num_loops_when_full, UDP_slots_empty_min );

    mreq.imr_multiaddr.s_addr=inet_addr(multicast_ip);
    mreq.imr_interface.s_addr=inet_addr(local_interface);
    if (setsockopt(fd,IPPROTO_IP,IP_DROP_MEMBERSHIP,&mreq,sizeof(mreq)) < 0) {  // Say we don't want multicast packets any more.
      perror("setsockopt IP_DROP_MEMBERSHIP");
      pthread_exit(NULL);
    }

    close(fd);                                                                  // Close the file descriptor for the port now we're about the leave

    printf( "Exiting UDP_recv\n");
    pthread_exit(NULL);
}

//---------------------------------------------------------------------------------------------------------------------------------------------------
// UDP_parse2sub - Check UDP packets as they arrive and generate sub files for correlator
//---------------------------------------------------------------------------------------------------------------------------------------------------

void *UDP_parse2sub()
{
    printf("UDP_parse2sub started\n");

//---------------- Initialize and declare variables ------------------------

    ni_packet_t *my_udp;                                                // Make a local pointer to the UDP packet we're working on.

    INT64 UDP_added_copy;                                               // This variable is going to be my local copy of UDP_added_to_buff

    INT64 UDP_slots_full;                                               // How many packets are still to be processed
    INT64 UDP_first_full;                                               // Index to the first waiting slot 0 to (UDP_num_slots-1)

    INT64 num_to_process;                                               // How many slots are we going to process in one mouthful?  Maybe UDP_slots_full or maybe fewer.

    INT64 Num_loops_when_empty = 0 ;                                    // How many times (since program start) have we checked if there was anything to process waiting in the buffer and there wasn't?

    INT64 loop, loop_end;

    INT64 sub_offset, largest_offset = 0;
    INT64 count_written = 0;

//---------------- Prepare the rf_input to correlator order mapping array as if it was read in from a metafits file ------------------------

    UINT16 sub_order;
    UINT16 input_mapping[ 65536 ] = {256};
    UINT16 rf_inputs[256] = {

//      rf_input values in 128T correlator order.  Use to sparce populate a 65536 element back-lookup table.  ie Enter this value, return the correlator order 0-255

      102,103,104,105,106,107,108,109,110,111,112,113,114,115,116,117,
      142,143,144,145,146,147,148,149,150,151,152,153,154,155,156,157,
      202,203,204,205,206,207,208,209,210,211,212,213,214,215,216,217,
      222,223,224,225,226,227,228,229,230,231,232,233,234,235,236,237,
      242,243,244,245,246,247,248,249,250,251,252,253,254,255,256,257,
      262,263,264,265,266,267,268,269,270,271,272,273,274,275,276,277,
      282,283,284,285,286,287,288,289,290,291,292,293,294,295,296,297,
      302,303,304,305,306,307,308,309,310,311,312,313,314,315,316,317,
      322,323,324,325,326,327,328,329,330,331,332,333,334,335,336,337,
      4002,4003,4004,4005,4006,4007,4008,4009,4010,4011,4012,4013,4014,4015,4016,4017,
      4018,4019,4020,4021,4022,4023,4024,4025,4026,4027,4028,4029,4030,4031,4032,4033,
      4034,4035,4036,4037,4038,4039,4040,4041,4042,4043,4044,4045,4046,4047,4048,4049,
      4050,4051,4052,4053,4054,4055,4056,4057,4058,4059,4060,4061,4062,4063,4064,4065,
      4066,4067,4068,4069,4070,4071,4072,4073,4074,4075,4076,4077,4078,4079,4080,4081,
      4082,4083,4084,4085,4086,4087,4088,4089,4090,4091,4092,4093,4094,4095,4096,4097,
      4098,4099,4100,4101,4102,4103,4104,4105,4106,4107,4108,4109,4110,4111,4112,4113
    };

    for ( loop = 0; loop < 256; loop++ ) {
      input_mapping[ rf_inputs[loop] ] = loop;                  // Now go back and populate the included ones to say where they go
    }

//---------------- Create one sub file -------------------

    char sub_file[300]={0};						// Room for the name of the file we're creating

    if ( strlen(readfile) == 25 ) {					// We are reading from a file and it has a nice long name like rawdump_1248519616_14.raw that contains the coarse channel number
      sprintf( sub_file, "%.11s%d%.3s.sub", &readfile[8], start_capture_time, &readfile[18] );		// generate a name like 1248519616_1248519616_14.sub
    } else {
      sprintf( sub_file, "%d.sub", start_capture_time );
    }

// Example source file rawdump_1248519616_09.raw

    //int filedesc = open( sub_file, O_WRONLY | O_CREAT | O_EXCL , S_IRUSR | S_IWUSR | S_IWGRP | S_IRGRP | S_IROTH );

    //if (filedesc == -1) {
      //perror("UDP_parse2sub: File create failed\n");
      //terminate = TRUE;
    //}

//---------------- Main loop to provide udp packets -------------------

    while (!terminate) {

      UDP_added_copy = UDP_added_to_buff;                               // Get a local copy of the other threads (file scope & volatile) variable so it doesn't change while I do maths on it.  It can only grow so this isn't a problem (no roll-over supported)

      UDP_slots_full = UDP_added_copy - UDP_removed_from_buff;          // How many UDP slots have data waiting?

      if ( UDP_slots_full > 0 ) {                                       // There's at least one packet to process!

        UDP_first_full = ( UDP_removed_from_buff % UDP_num_slots );     // The index from 0 to (UDP_num_slots-1) of the oldest waiting UDP packet slot in the buffer

        num_to_process = UDP_slots_full;                                // For now we'll try to eat them all at the same time.  This is the place to clip that number to a maximum mouthful.

        loop_end = UDP_first_full + num_to_process - 1;                 // This may take loop_end past (UDP_num_slots-1) but never > (UDP_num_slots * 2 - 1).
        if ( loop_end >= UDP_num_slots ) loop_end = UDP_num_slots - 1;  // Stop at the end of the buffer and force another pass through the loop from the begin if there are more packets to process

        for ( loop=UDP_first_full; loop<=loop_end; loop++ ) {           // We will process from UDP_first_full to loop_end INCLUSIVE, so needs to be <= in end condition!

//---------- At this point in the loop, we are about to process the next arrived UDP packet, and it is located at UDPbuf[loop] ----------
// put your code here

          my_udp = &UDPbuf[loop];                                       // Create local pointer to UDP packet

          my_udp->rf_input = ntohs( my_udp->rf_input );                 // exists on the wire as big-endian, we need to make it little-endian
          my_udp->subsec_time = ntohs( my_udp->subsec_time );           // exists on the wire as big-endian, we need to make it little-endian
          my_udp->GPS_time = ntohl( my_udp->GPS_time );                 // exists on the wire as big-endian, we need to make it little-endian

          if (start_capture_time == 0)
          {
            start_capture_time = my_udp->GPS_time + 8;
            end_capture_time = start_capture_time + 7;    // 7 because >=
          }

          if ( ( my_udp->GPS_time == CLOSEDOWN ) ||			// If we've been asked to close down via a udp packet with the time set to the year 2106
                ( my_udp->GPS_time > ( end_capture_time + 8 ) ) ) {	// or we're so far past the time we're interested in that it's pointless to keep checking

            terminate = TRUE;                                           // tell everyone to shut down
            UDP_removed_from_buff++;                                    // Increment the number of packets we've ever processed to ensure we don't see this closedown packet again (forever?)
            break;                                                      // and exit the 'for' loop early in case there are any more packets.  We'll terminate soon enough.
          }

          if ( ( my_udp->GPS_time >= start_capture_time) && ( my_udp->GPS_time <= end_capture_time) ) {         // It's for this sub file
            sub_order = input_mapping[ my_udp->rf_input ];
            if ( sub_order >= 256 ) printf ( "Error found an rf_input I can't explain. rfi=%d, so=%d\n", my_udp->rf_input, sub_order );

            sub_offset = FILE_HEADER_SIZE + BLOCK_SIZE
                + ( my_udp->GPS_time - start_capture_time ) * SEC_OFFSET
                + ( my_udp->subsec_time / 25 ) * BLOCK_SIZE
                + ( my_udp->subsec_time % 25 ) * PACKET_OFFSET
                + sub_order * LINE_OFFSET;

            if ( sub_offset > largest_offset ) {
              largest_offset = sub_offset;
              printf( "F=%d,T=%d:%d,ord[%d]=%d,off=%lld\n", my_udp->freq_channel, ( my_udp->GPS_time - start_capture_time ), my_udp->subsec_time, my_udp->rf_input, sub_order, sub_offset );
            }

#if 0       // remove direct writing to file
            lseek( filedesc, sub_offset, SEEK_SET );
            count_written++;

            if ( write( filedesc, my_udp->payload, PAYLOAD_SIZE ) != PAYLOAD_SIZE ) {
              printf( "Incomplete write\n" );                           // Theoretically an incomplete write is allowed and we should just continue, but it's probably a full disk so let's escape
            }
#endif
            // replace with writing to current sub buffer
            count_written++;
            memcpy((void *)(current_sub_buffer + sub_offset), (const void *)my_udp->payload, (size_t)PAYLOAD_SIZE);

          }

          UDP_removed_from_buff++;                                      // Increment the number of packets we've ever processed (which automatically releases them from the buffer).

//---------- End of processing of this UDP packet ----------

        }

      } else {
        Num_loops_when_empty++;                                         // We looked but found nothing to do.  Keep track of how many times that happened
      }

    }

    //close( filedesc );

    printf( "looped on empty %lld times\n", Num_loops_when_empty );
    printf( "processed %lld packets\n", UDP_removed_from_buff );
    printf( "found %lld packets to include out of a possible %lld (missing %lld)\n", count_written, (end_capture_time-start_capture_time+1LL)*160000LL, (end_capture_time-start_capture_time+1LL)*160000LL-count_written );
    printf( "Exiting UDP_parse2sub\n");
    pthread_exit(NULL);
}

//---------------------------------------------------------------------------------------------------------------------------------------------------

void sigint_handler(int signo)
{
  printf("\n\nAsked to shut down\n");
  terminate = TRUE;                             // This is a volatile, file scope variable.  All threads should be watching for this.
}

//---------------------------------------------------------------------------------------------------------------------------------------------------

void sigusr1_handler(int signo)
{
  printf("\n\nSomeone sent a sigusr1 signal to me\n");
}

//---------------------------------------------------------------------------------------------------------------------------------------------------

void usage(char *err)                           // Bad command line.  Report the supported usage.
{
    printf( "%s", err );
    printf("\n\n To run:        /home/mwa/UDPr -p 30000 -u 1 -n 0 -b 8192\n\n");

    printf("                    -p port to listen for UDP packets on\n");
    printf("                    -u specify UDP packet type ( 1 = NI, 2 = MWAHTR, 3 = 3pip )\n");
    printf("                    -n NUMA node to run on\n");
    printf("                    -b Number of UDP packets to buffer locally\n");
    printf("                    -v Verbose output mode\n");
    printf("Sizeof NI packet = %ld\n", sizeof(ni_packet_t) );

}

// ------------------------ Start of world -------------------------

int main(int argc, char **argv)
{
    int prog_build = BUILD;                             // Build number.  Remember to update each build revision!
    int loop;                                           // General purpose.  Usage changes in context

    char hostname[300];                                 // Long enough to fit a 255 host name.  Probably it will only be short, but -hey- it's just a few bytes

    signal(SIGINT, sigint_handler);                     // Tell the OS that we want to trap SIGINT calls
    signal(SIGUSR1, sigusr1_handler);                   // Tell the OS that we want to trap SIGUSR1 calls

    pthread_t UDP_recv_pt;                              // See thread list above for details on the following threads
    pthread_t UDP_parse_pt;

    if ( gethostname( hostname, sizeof hostname ) == -1 ) strcpy( hostname, "unknown" );
    printf("\nStartup UDPrecv on %s.  ver 1.00a Build %d\n", hostname, prog_build);

    local_interface = local_interface_medconv01;

    if ( strcmp ( hostname, "mwax00" ) == 0 ) { local_interface = local_interface_mwax00; UDPport = 59009; }
    if ( strcmp ( hostname, "mwax01" ) == 0 ) { local_interface = local_interface_mwax01; UDPport = 59009; }
    if ( strcmp ( hostname, "mwax02" ) == 0 ) { local_interface = local_interface_mwax02; UDPport = 59010; }
    if ( strcmp ( hostname, "mwax03" ) == 0 ) { local_interface = local_interface_mwax03; UDPport = 59011; }
    if ( strcmp ( hostname, "mwax04" ) == 0 ) { local_interface = local_interface_mwax04; UDPport = 59012; }
    if ( strcmp ( hostname, "mwax05" ) == 0 ) { local_interface = local_interface_mwax05; UDPport = 59013; }
    if ( strcmp ( hostname, "mwax06" ) == 0 ) { local_interface = local_interface_mwax06; UDPport = 59014; }
    if ( strcmp ( hostname, "mwax07" ) == 0 ) { local_interface = local_interface_mwax07; UDPport = 59015; }

    if ( strcmp ( hostname, "vcs01" ) == 0 ) { local_interface = local_interface_vcs01; UDPport = 59009; }
    if ( strcmp ( hostname, "vcs02" ) == 0 ) { local_interface = local_interface_vcs02; UDPport = 59009; }
    if ( strcmp ( hostname, "vcs03" ) == 0 ) { local_interface = local_interface_vcs03; UDPport = 59009; }
    if ( strcmp ( hostname, "vcs04" ) == 0 ) { local_interface = local_interface_vcs04; UDPport = 59009; }
    if ( strcmp ( hostname, "vcs05" ) == 0 ) { local_interface = local_interface_vcs05; UDPport = 59009; }
    if ( strcmp ( hostname, "vcs06" ) == 0 ) { local_interface = local_interface_vcs06; UDPport = 59009; }
    if ( strcmp ( hostname, "vcs07" ) == 0 ) { local_interface = local_interface_vcs07; UDPport = 59009; }
    if ( strcmp ( hostname, "vcs08" ) == 0 ) { local_interface = local_interface_vcs08; UDPport = 59009; }
    if ( strcmp ( hostname, "vcs09" ) == 0 ) { local_interface = local_interface_vcs09; UDPport = 59009; }
    if ( strcmp ( hostname, "vcs10" ) == 0 ) { local_interface = local_interface_vcs10; UDPport = 59009; }
    if ( strcmp ( hostname, "vcs11" ) == 0 ) { local_interface = local_interface_vcs11; UDPport = 59009; }
    if ( strcmp ( hostname, "vcs12" ) == 0 ) { local_interface = local_interface_vcs12; UDPport = 59009; }
    if ( strcmp ( hostname, "vcs13" ) == 0 ) { local_interface = local_interface_vcs13; UDPport = 59009; }
    if ( strcmp ( hostname, "vcs14" ) == 0 ) { local_interface = local_interface_vcs14; UDPport = 59009; }
    if ( strcmp ( hostname, "vcs15" ) == 0 ) { local_interface = local_interface_vcs15; UDPport = 59009; }
    if ( strcmp ( hostname, "vcs16" ) == 0 ) { local_interface = local_interface_vcs16; UDPport = 59009; }

    if ( strcmp ( hostname, "blc00" ) == 0 ) { local_interface = local_interface_blc00; UDPport = 59009; }

    if ( strcmp ( hostname, "ibm-p8-01" ) == 0 ) {      // default setup for ibm power 8
      server = 2;                                               // which server are we running on
      local_interface = local_interface_ibm;
//      UDPport = 55000;
    }

//---------------- Parse command line parameters ------------------------

    while (argc > 1 && argv[1][0] == '-') {
      switch (argv[1][1]) {

        case 'v':
          verbose = TRUE;
          printf ( "verbose on\n" );
          break ;

        case 'c':
          correlate = TRUE;
          printf ( "Correlate on\n" );
          break ;

        case 'p':
          ++argv ;
          --argc ;
          UDPport = atoi(argv[1]);
          break;

        case 'n':
          ++argv ;
          --argc ;
          numanode = atoi(argv[1]);
          break;

        case 's':
          ++argv ;
          --argc ;
          start_capture_time = strtol(argv[1],0,0);
          printf ( "Start time = %d\n", start_capture_time ) ;
          break;

        case 'e':
          ++argv ;
          --argc ;
          end_capture_time = strtol(argv[1],0,0);
          printf ( "End time = %d\n", end_capture_time ) ;
          break;

        case 'b':
          ++argv ;
          --argc ;
          UDP_num_slots = strtoll(argv[1],0,0);
          break;

        case 'u':
          ++argv ;
          --argc ;
          UDPtype = atoi(argv[1]);
          break;

        case 'f':
          ++argv ;
          --argc ;
          readfile = argv[1];
          printf ( "File to read = %s\n", readfile );
          break ;

        default:
          usage("unknown option") ;
          exit(EXIT_FAILURE);

      }
      --argc ;
      ++argv ;
    }

    if (argc > 1) {                             // There is/are at least one command line option we don't understand
      usage("");                                // Print the available options
      exit(EXIT_FAILURE);
    }

// ------------------------ Set up data for CPU affinity so that we can control which socket & core we run on -------------------------
/*
    pthread_t my_thread;                        // I need to look up my own TID / LWP
    my_thread = pthread_self();                 // So what's my TID / LWP?

    CPU_ZERO(&physical_id_0);                   // Zero out the initial set
    CPU_ZERO(&physical_id_1);                   // This one too
    CPU_SET( 0, &physical_id_0);                // CPU  0 is on physical_id 0 (from /proc/cpuinfo)
    CPU_SET( 1, &physical_id_0);                // CPU  1 is on physical_id 0 (from /proc/cpuinfo)
    CPU_SET( 2, &physical_id_0);                // CPU  2 is on physical_id 0 (from /proc/cpuinfo)
    CPU_SET( 3, &physical_id_0);                // CPU  3 is on physical_id 0 (from /proc/cpuinfo)
    CPU_SET( 4, &physical_id_0);                // CPU  4 is on physical_id 0 (from /proc/cpuinfo)
    CPU_SET( 5, &physical_id_0);                // CPU  5 is on physical_id 0 (from /proc/cpuinfo)
    CPU_SET( 6, &physical_id_0);                // CPU  6 is on physical_id 0 (from /proc/cpuinfo)
    CPU_SET( 7, &physical_id_0);                // CPU  7 is on physical_id 0 (from /proc/cpuinfo)
    CPU_SET( 8, &physical_id_1);                // CPU  8 is on physical_id 1 (from /proc/cpuinfo)
    CPU_SET( 9, &physical_id_1);                // CPU  9 is on physical_id 1 (from /proc/cpuinfo)
    CPU_SET(10, &physical_id_1);                // CPU 10 is on physical_id 1 (from /proc/cpuinfo)
    CPU_SET(11, &physical_id_1);                // CPU 11 is on physical_id 1 (from /proc/cpuinfo)
    CPU_SET(12, &physical_id_1);                // CPU 12 is on physical_id 1 (from /proc/cpuinfo)
    CPU_SET(13, &physical_id_1);                // CPU 13 is on physical_id 1 (from /proc/cpuinfo)
    CPU_SET(14, &physical_id_1);                // CPU 14 is on physical_id 1 (from /proc/cpuinfo)
    CPU_SET(15, &physical_id_1);                // CPU 15 is on physical_id 1 (from /proc/cpuinfo)
    CPU_SET(16, &physical_id_0);                // CPU 16 is on physical_id 0 (from /proc/cpuinfo)
    CPU_SET(17, &physical_id_0);                // CPU 17 is on physical_id 0 (from /proc/cpuinfo)
    CPU_SET(18, &physical_id_0);                // CPU 18 is on physical_id 0 (from /proc/cpuinfo)
    CPU_SET(19, &physical_id_0);                // CPU 19 is on physical_id 0 (from /proc/cpuinfo)
    CPU_SET(20, &physical_id_0);                // CPU 20 is on physical_id 0 (from /proc/cpuinfo)
    CPU_SET(21, &physical_id_0);                // CPU 21 is on physical_id 0 (from /proc/cpuinfo)
    CPU_SET(22, &physical_id_0);                // CPU 22 is on physical_id 0 (from /proc/cpuinfo)
    CPU_SET(23, &physical_id_0);                // CPU 23 is on physical_id 0 (from /proc/cpuinfo)
    CPU_SET(24, &physical_id_1);                // CPU 24 is on physical_id 1 (from /proc/cpuinfo)
    CPU_SET(25, &physical_id_1);                // CPU 25 is on physical_id 1 (from /proc/cpuinfo)
    CPU_SET(26, &physical_id_1);                // CPU 26 is on physical_id 1 (from /proc/cpuinfo)
    CPU_SET(27, &physical_id_1);                // CPU 27 is on physical_id 1 (from /proc/cpuinfo)
    CPU_SET(28, &physical_id_1);                // CPU 28 is on physical_id 1 (from /proc/cpuinfo)
    CPU_SET(29, &physical_id_1);                // CPU 29 is on physical_id 1 (from /proc/cpuinfo)
    CPU_SET(30, &physical_id_1);                // CPU 30 is on physical_id 1 (from /proc/cpuinfo)
    CPU_SET(31, &physical_id_1);                // CPU 31 is on physical_id 1 (from /proc/cpuinfo)
*/

    if ( remove( "rawdump.bin" ) == 0 ) {
      printf ( "Deleted file\n" );
    } else {
      printf ( "error %d deleting file\n", errno );
    }

    // allocate three buffers for assembling sub files
    sub_a = (INT8 *)malloc(FILE_HEADER_SIZE + (201*BLOCK_SIZE));  // PSRDADA header plus metadata block plus 200 40ms blocks
    sub_b = (INT8 *)malloc(FILE_HEADER_SIZE + (201*BLOCK_SIZE));
    sub_c = (INT8 *)malloc(FILE_HEADER_SIZE + (201*BLOCK_SIZE));
    previous_sub_buffer = sub_c;
    current_sub_buffer = sub_a;
    next_sub_buffer = sub_b;

    msgvecs = calloc( 2 * UDP_num_slots, sizeof(struct mmsghdr) );      // NB Make twice as big an array as the number of actual UDP packets we are going to buffer
    iovecs = calloc( UDP_num_slots, sizeof(struct iovec) );             // NB Make the *same* number of entries as the number of actual UDP packets we are going to buffer
    UDPbuf = calloc( UDP_num_slots, sizeof( ni_packet_t ) );            // NB Make the *same* number of entries as the number of actual UDP packets we are going to buffer

    for (loop = 0; loop < UDP_num_slots; loop++) {

        iovecs[ loop ].iov_base = &UDPbuf[loop];
        iovecs[ loop ].iov_len = sizeof( ni_packet_t );

        msgvecs[ loop ].msg_hdr.msg_iov = &iovecs[loop];
        msgvecs[ loop ].msg_hdr.msg_iovlen = 1;

        msgvecs[ loop + UDP_num_slots ].msg_hdr.msg_iov = &iovecs[loop];
        msgvecs[ loop + UDP_num_slots ].msg_hdr.msg_iovlen = 1;
    }

    if ( correlate ) {                                                  // Are we going to generate sub files for the correlator
      pthread_create(&UDP_parse_pt,NULL,UDP_parse2sub,NULL);            // then we need this thread
    } else {                                                            // or is it just writing to disk of the screen
      //pthread_create(&UDP_parse_pt,NULL,UDP_parse,NULL);                // in which case the old one is fine
    }

    sleep(1);

    if ( readfile[0] == 0x00 ) {                                        // Are we reading the udp packets in from a multicast port
      pthread_create(&UDP_recv_pt,NULL,UDP_recv,NULL);                  // then this is the right receive function
    } else {                                                            // or is it reading from a disk file
      //pthread_create(&UDP_recv_pt,NULL,UDP_recv_disk,NULL);             // in which case we want this one
    }

    while(!terminate) sleep(1);                 // The master thread currently does nothing! Zip! Nada!  What a waste eh?

    printf("Joining UDP_parse.\n");
    pthread_join(UDP_parse_pt,NULL);
    printf("UDP_parse joined.\n");
    printf("Joining UDP_recv.\n");
    pthread_join(UDP_recv_pt,NULL);
    printf("UDP_recv joined.\n");

/*  WIP!!! free up the malloced buffers */

    printf("Exiting process\n");

    exit(EXIT_SUCCESS);
}

// End of code.  Comments, snip and examples only follow
