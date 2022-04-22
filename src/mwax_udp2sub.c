//===================================================================================================================================================
// Capture UDP packets into a temporary, user space buffer and then sort/copy/process them into some output array
//
// Author(s)  BWC Brian Crosse brian.crosse@curtin.edu.au
// Commenced 2017-05-25
//
// 2.02e-046	2020-09-17 BWC	Rename to mwax_udp2sub for consistency.  Comment out some unused variables to stop compile warnings
//				Change the cfitsio call for reading strings from ffgsky to ffgkls in hope it fixes the compile on BL server. (Spoiler: It does)
//
// 2.02d-045	2020-09-09 BWC	Switch back to long baseline configuration
//				Make UDP_num_slots a config variable so different servers can have different values
//				Do the coarse channel reversal above (chan>=129)
//				Change MODE to NO_CAPTURE after the observation ends
//
// 2.02c-044	2020-09-08 BWC	Read Metafits directly from vulcan. No more metabin files
//
// 2.02b-043	2020-09-03 BWC	Write ASCII header onto subfiles.  Need to fake some details for now
//
// 2.02a-042	2020-09-03 BWC	Force switch to short baseline tile ids to match medconv
//				Add heaps of debugs
//
// 2.01a-041	2020-09-02 BWC	Read metabin from directory specified in config
//
// 2.00a-040	2020-08-25 BWC	Change logic to write to .sub file so that udp packet payloads are not copied until after the data arrives for that sub
//				Clear out a lot of historical code that isn't relevant for actual sub file generation (shifted to other utilities)
//
// 1.00a-039    2020-02-07 BWC  Tell OS not to buffer disk writes.  This should improve memory usage.
//
// 1.00a-038    2020-02-04 BWC  Add feature to support multiple coarse channels arriving from the one input (multicast *or* file) on recsim only
//				Change recsim to make .sub files with only 1 rf_input.
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
// To compile:	gcc mwax_udp2sub_46.c -omwax_u2s -lpthread -Ofast -march=native -lrt -lcfitsio -Wall
//              There should be NO warnings or errors on compile!
//
// To run:	numactl --membind=0 dd if=/dev/zero of=/dev/shm/a.free bs=4096 count=1288001
//		numactl --cpunodebind=0 --membind=0 ./udp2sub
//
// To run:      From Helios type:
//                      for i in {01..06};do echo mwax$i;ssh mwax$i 'cd /data/solarplatinum;numactl --cpunodebind=0 --membind=0 ./udp2sub -c 23 > output.log &';done
//                      for i in {01..06};do echo mwax$i;ssh mwax$i 'cat /data/solarplatinum/20190405.log';done
//
//===================================================================================================================================================
//
// To do:               Too much to say!

#define BUILD 46
#define THISVER "2.02e"

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
#include <dirent.h>

#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/shm.h>

#include <fitsio.h>

//---------------- Define our old friends -------------------

#define FALSE 0
//#define TRUE !(FALSE)

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

//---------------- and some new friends -------------------

#define MAX_INPUTS (544LL)
#define UDP_PAYLOAD_SIZE (4096LL)

#define SUBFILE_HEADER_SIZE (4096LL)
#define SAMPLES_PER_SEC (1280000LL)
#define COARSECHAN_BANDWIDTH (1280000LL)
#define ULTRAFINE_BW (200ll)
#define BLOCKS_PER_SUB (160LL)

#define SUB_LINE_SIZE ((SAMPLES_PER_SEC * 8LL * 2LL) / BLOCKS_PER_SUB)
#define NTIMESAMPLES ((SAMPLES_PER_SEC * 8LL) / BLOCKS_PER_SUB)

#define UDP_PER_RF_PER_SUB ( ((SAMPLES_PER_SEC * 8LL * 2LL) / UDP_PAYLOAD_SIZE) + 2 )
#define SUBSECSPERSEC ( (SAMPLES_PER_SEC * 2LL)/ UDP_PAYLOAD_SIZE )
#define SUBSECSPERSUB ( SUBSECSPERSEC * 8LL )

//---------------- MWA external Structure definitions --------------------

#pragma pack(push,1)                          // We're writing this header into all our packets, so we want/need to force the compiler not to add it's own idea of structure padding

typedef struct mwa_udp_packet {               // Structure format for the MWA data packets

    uint8_t packet_type;                      // Packet type (0x00 == Binary Header, 0x20 == 2K samples of Voltage Data in complex 8 bit real + 8 bit imaginary format)
    uint8_t freq_channel;                     // The current coarse channel frequency number [0 to 255 inclusive].  This number is available from the header, but repeated here to simplify archiving and monitoring multiple frequencies.
    uint16_t rf_input;                        // maps to tile/antenna and polarisation (LSB is 0 for X, 1 for Y)
    uint32_t GPS_time;                        // [second] GPS time of packet (bottom 32 bits only)
    uint16_t subsec_time;                     // count from 0 to PACKETS_PER_SEC - 1 (from header)  Typically PACKETS_PER_SEC is 625 in MWA
    uint8_t spare1;                           // spare padding byte.  Reserved for future use.
    uint8_t edt2udp_id;                       // Which edt2udp instance generated this packet and therefore a lookup to who to ask for a retransmission (0 implies no retransmission possible)
    uint16_t edt2udp_token;                   // value to pass back to edt2udp instance to help identify source packet.  A token's value is constant for a given source rf_input and freq_channel for entire sub-observation
    uint8_t spare2[2];                        // Spare bytes for future use
    uint16_t volts[2048];                     // 2048 complex voltage samples.  Each one is 8 bits real, followed by 8 bits imaginary but we'll treat them as a single 16 bit 'bit pattern'

} mwa_udp_packet_t ;

//----------------
/* WIP
typedef struct udp2sub_monitor {               // Structure for the placing monitorable statistics in shared memory so they can be read by external applications

    uint8_t udp2sub_id;                       // Which instance number we are.  Probably from 01 to 26, at least initially.
    char hostname[21];			      // Host name is looked up to against these strings
    int coarse_chan;         		      // Which coarse chan from 01 to 24.
    // Last sub file created
    // most advanced udp packet
    // Lowest remaining buffer left
    // Stats on lost packets
    uint64_t sub_write_sleeps;			// record how often we went to sleep while looking for a sub file to write out (should grow quickly)

mon->ignored_for_a_bad_type++;				// Keep track for debugging of how many packets we've seen that we can't handle
mon->ignored_for_being_too_old++;				// Keep track for debugging of how many packets were too late to process
mon->sub_meta_sleeps++;						// record we went to sleep
mon->sub_write_sleeps++;					// record we went to sleep
mon->sub_write_dummy++;						// record we needed to use the dummy packet to fill in for a missing udp packet


} udp2sub_monitor_t ;
*/
#pragma pack(pop)                               // Set the structure packing back to 'normal' whatever that is

//---------------- internal structure definitions --------------------

typedef struct subobs_udp_meta {			// Structure format for the MWA subobservation metadata that tracks the sorted location of the udp packets

    volatile uint32_t subobs;				// The sub observation number.  ie the GPS time of the first second in this sub-observation
    volatile int state;					// 0==Empty, 1==Adding udp packets, 2==closed off ready for sub write, 3==sub write in progress, 4==sub write complete and ready for reuse, >=5 is failed

    int msec_wait;					// The number of milliseconds the sub write thread had waited doing nothing before starting to write out this sub.  Only valid for state = 3 or 4 or 5 or higher
    int msec_took;					// The number of milliseconds the sub write thread took to write out this sub.  Only valid for state = 4 or 5 or higher

    INT64 first_udp;
    INT64 last_udp;
    INT64 udp_at_start_write;
    INT64 udp_at_end_write;

    int udp_count;					// The number of udp packets collected from the NIC
    int udp_dummy;					// The number of dummy packets we needed to insertto pad things out

    volatile int meta_done;				// Has metafits data been appended to this sub observation yet?  0 == No.
    int meta_msec_wait;					// The number of milliseconds it took to from reading the last metafits to finding a new one
    int meta_msec_took;					// The number of milliseconds it took to read the metafits

    INT64 GPSTIME;					// Following fields are straight from the metafits file (via the metabin) This is the GPSTIME *FOR THE OBS* not the subobs!!!
    int EXPOSURE;
    char PROJECT[32];
    char MODE[32];

    int CHANNELS[24];
    float FINECHAN;
    float INTTIME;

    int NINPUTS;
    INT64 UNIXTIME;

    int COARSE_CHAN;
    int FINECHAN_hz;
    int INTTIME_msec;

    uint16_t rf_seen;					// The number of different rf_input sources seen so far this sub observation
    uint16_t rf2ndx[65536];				// A mapping from the rf_input value to what row in the pointer array its pointers are stored
    char *udp_volts[MAX_INPUTS][UDP_PER_RF_PER_SUB];	// array of pointers to every udp packet's payload that may be needed for this sub-observation

} subobs_udp_meta_t;

//---------- A structure of information from the metafits/metabin but which needs modification at run-time ----------
/*
typedef struct MandC_obs {				// Structure format for the header MWA obs and subobservation metadata

    INT64 GPSTIME;					// Following fields are straight from the metafits file (via the metabin)
    int EXPOSURE;
    char PROJECT[32];
    char MODE[32];
    int CHANNELS[100];
    int FINECHAN_hz;
    int INTTIME_msec;
    int NINPUTS;
    INT64 UNIXTIME;

} MandC_obs_t;
*/

typedef struct MandC_meta {				// Structure format for the MWA subobservation metadata that tracks the sorted location of the udp packets.  Array with one entry per rf_input

    uint16_t rf_input;					// tile/antenna and polarisation (LSB is 0 for X, 1 for Y)
    int start_byte;					// What byte (not sample) to start at.  Remember we store a whole spare packet before the real data starts
    uint16_t seen_order;				// For this subobs (only). What order was this rf input seen in?  That tells us where we need to look in the udp_volts pointer array

} MandC_meta_t;

//---------------------------------------------------------------------------------------------------------------------------------------------------
// These variables are shared by all threads.  "file scope"
//---------------------------------------------------------------------------------------------------------------------------------------------------

#define CLOSEDOWN (0xFFFFFFFF)

volatile BOOL terminate = FALSE;                        // Global request for everyone to close down

volatile INT64 UDP_added_to_buff = 0;                   // Total number of packets received and placed in the buffer.  If it overflows we are in trouble and need a restart.
volatile INT64 UDP_removed_from_buff = 0;               // Total number of packets pulled from buffer and space freed up.  If it overflows we are in trouble and need a restart.

INT64 UDP_num_slots;					// Must be at least 3000000 for mwax07 with 128T. 3000000 will barely make it!
UINT32 GPS_offset = 315964782;				// Logging only.  Needs to be updated on leap seconds

struct mmsghdr *msgvecs;
struct iovec *iovecs;
mwa_udp_packet_t *UDPbuf;

subobs_udp_meta_t *sub;					// Pointer to the four subobs metadata arrays
char *blank_sub_line;					// Pointer to a buffer of zeros that's the size of an empty line.  We'll allocate it later and use it for padding out sub files
char *sub_header;					// Pointer to a buffer that's the size of a sub file header.

//---------------------------------------------------------------------------------------------------------------------------------------------------
// read_config - use our hostname and a command line parameter to find ourselves in the list of possible configurations
// Populate a single structure with the relevant information and return it
//---------------------------------------------------------------------------------------------------------------------------------------------------

//#pragma pack(push,1)                          // We're writing this header into all our packets, so we want/need to force the compiler not to add it's own idea of structure padding

typedef struct udp2sub_config {               // Structure for the configuration of each udp2sub instance.

    int udp2sub_id;			      // Which correlator id number we are.  Probably from 01 to 26, at least initially.
    char hostname[64];			      // Host name is looked up against these strings to select the correct line of configuration settings
    int host_instance;			      // Is compared with a value that can be put on the command line for multiple copies per server

    INT64 UDP_num_slots;		      // The number of UDP buffers to assign.  Must be ~>=3000000 for 128T array

    char shared_mem_dir[40];		      // The name of the shared memory directory where we get .free files from and write out .sub files
    char temp_file_name[40];		      // The name of the temporary file we use.  If multiple copies are running on each server, these may need to be different
    char stats_file_name[40];		      // The name of the shared memory file we use to write statistics and debugging to.  Again if multiple copies per server, it may need different names
    char spare_str[40];			      //
    char metafits_dir[60];		      // The directory where metafits files are looked for.  NB Likely to be on an NFS mount such as /vulcan/metafits

    char local_if[20];			      // Address of the local NIC interface we want to receive the multicast stream on.
    int coarse_chan;         		      // Which coarse chan from 01 to 24.
    char multicast_ip[20];		      // The multicast address and port (below) we wish to join.
    int UDPport;			      // Multicast port address

} udp2sub_config_t ;

//#pragma pack(pop)                               // Set the structure packing back to 'normal' whatever that is

udp2sub_config_t conf;				// A place to store the configuration data for this instance of the program.  ie of the 60 copies running on 10 computers or whatever

void read_config ( char *file, char *us, int inst, udp2sub_config_t *config )
{

#define MAXINSTANCE (30)

    int instance_ndx = 0;                                                                       // Start out assuming we don't appear in the list

    udp2sub_config_t available_config[MAXINSTANCE] = {

       {0,"unknown",0,5000000,"","","","","","",0,"",0}

      ,{1,"mwax01",0,5000000,"/dev/shm","/dev/shm/temp.temp","/mwax_stats","","/vulcan/metafits","192.168.90.201",9,"239.255.90.9",59009}
      ,{2,"mwax02",0,5000000,"/dev/shm","/dev/shm/temp.temp","/mwax_stats","","/vulcan/metafits","192.168.90.202",10,"239.255.90.10",59010}
      ,{3,"mwax03",0,5000000,"/dev/shm","/dev/shm/temp.temp","/mwax_stats","","/vulcan/metafits","192.168.90.203",11,"239.255.90.11",59011}
      ,{4,"mwax04",0,5000000,"/dev/shm","/dev/shm/temp.temp","/mwax_stats","","/vulcan/metafits","192.168.90.204",12,"239.255.90.12",59012}
      ,{5,"mwax05",0,5000000,"/dev/shm","/dev/shm/temp.temp","/mwax_stats","","/vulcan/metafits","192.168.90.205",13,"239.255.90.13",59013}
      ,{6,"mwax06",0,5000000,"/dev/shm","/dev/shm/temp.temp","/mwax_stats","","/vulcan/metafits","192.168.90.206",14,"239.255.90.14",59014}
      ,{7,"mwax07",0,5000000,"/dev/shm/CC15","/dev/shm/CC15.temp","/mwax_stats","","/vulcan/metafits","192.168.90.207",15,"239.255.90.15",59015}
      ,{29,"mwax07",16,5000000,"/dev/shm/CC16","/dev/shm/CC16.temp","/mwax_stats","","/vulcan/metafits","192.168.90.207",16,"239.255.90.16",59016}

      ,{8,"medconv01",1,5000000,"/dev/shm","/dev/shm/temp.temp1","/mwax1_stats","","/vulcan/metafits","192.168.90.121",10,"239.255.90.10",59010}
      ,{9,"medconv01",2,5000000,"/dev/shm","/dev/shm/temp.temp2","/mwax2_stats","","/vulcan/metafits","192.168.90.122",11,"239.255.90.11",59011}

      ,{10,"recsim",0,5000000,"/dev/shm","/dev/shm/temp.temp","/mwax_stats","","/vulcan/metafits","192.168.90.227",11,"239.255.90.11",59011}
      ,{11,"blc00",0,3500000,"/dev/shm","/dev/shm/temp.temp","/mwax_stats","","/vulcan/metafits","192.168.90.240",11,"239.255.90.11",59011}
      ,{12,"ibm-p8-01",0,5000000,"/dev/shm","/dev/shm/temp.temp","/mwax_stats","","/vulcan/metafits","192.168.90.212",11,"239.255.90.11",59011}

      ,{13,"vcs01",0,5000000,"/dev/shm","/dev/shm/temp.temp","/mwax_stats","","/vulcan/metafits","202.9.9.131",11,"239.255.90.11",59011}
      ,{14,"vcs02",0,5000000,"/dev/shm","/dev/shm/temp.temp","/mwax_stats","","/vulcan/metafits","202.9.9.132",11,"239.255.90.11",59011}
      ,{15,"vcs03",0,5000000,"/dev/shm","/dev/shm/temp.temp","/mwax_stats","","/vulcan/metafits","202.9.9.133",11,"239.255.90.11",59011}
      ,{16,"vcs04",0,5000000,"/dev/shm","/dev/shm/temp.temp","/mwax_stats","","/vulcan/metafits","202.9.9.134",11,"239.255.90.11",59011}
      ,{17,"vcs05",0,5000000,"/dev/shm","/dev/shm/temp.temp","/mwax_stats","","/vulcan/metafits","202.9.9.135",11,"239.255.90.11",59011}
      ,{18,"vcs06",0,5000000,"/dev/shm","/dev/shm/temp.temp","/mwax_stats","","/vulcan/metafits","202.9.9.136",11,"239.255.90.11",59011}
      ,{19,"vcs07",0,5000000,"/dev/shm","/dev/shm/temp.temp","/mwax_stats","","/vulcan/metafits","202.9.9.137",11,"239.255.90.11",59011}
      ,{20,"vcs08",0,5000000,"/dev/shm","/dev/shm/temp.temp","/mwax_stats","","/vulcan/metafits","202.9.9.138",11,"239.255.90.11",59011}
      ,{21,"vcs09",0,5000000,"/dev/shm","/dev/shm/temp.temp","/mwax_stats","","/vulcan/metafits","202.9.9.139",11,"239.255.90.11",59011}
      ,{22,"vcs10",0,5000000,"/dev/shm","/dev/shm/temp.temp","/mwax_stats","","/vulcan/metafits","202.9.9.140",11,"239.255.90.11",59011}
      ,{23,"vcs11",0,5000000,"/dev/shm","/dev/shm/temp.temp","/mwax_stats","","/vulcan/metafits","202.9.9.141",11,"239.255.90.11",59011}
      ,{24,"vcs12",0,5000000,"/dev/shm","/dev/shm/temp.temp","/mwax_stats","","/vulcan/metafits","202.9.9.142",11,"239.255.90.11",59011}
      ,{25,"vcs13",0,5000000,"/dev/shm","/dev/shm/temp.temp","/mwax_stats","","/vulcan/metafits","202.9.9.143",11,"239.255.90.11",59011}
      ,{26,"vcs14",0,5000000,"/dev/shm","/dev/shm/temp.temp","/mwax_stats","","/vulcan/metafits","202.9.9.144",11,"239.255.90.11",59011}
      ,{27,"vcs15",0,5000000,"/dev/shm","/dev/shm/temp.temp","/mwax_stats","","/vulcan/metafits","202.9.9.145",11,"239.255.90.11",59011}
      ,{28,"vcs16",0,5000000,"/dev/shm","/dev/shm/temp.temp","/mwax_stats","","/vulcan/metafits","202.9.9.146",11,"239.255.90.11",59011}

    };

    for ( int loop = 0 ; loop < MAXINSTANCE ; loop++ ) {        // Check through all possible configurations

      if ( ( strcmp( available_config[loop].hostname, us ) == 0 ) && ( available_config[loop].host_instance == inst ) ) {
        instance_ndx = loop;                                    // if the edt card config matches the command line and the hostname matches
        break;                                                  // We don't need to keep looking
      }

    }
    *config = available_config[ instance_ndx ];                 // Copy the relevant line into the structure we were passed a pointer to
}

//===================================================================================================================================================






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

    printf ( "Set up to receive from multicast %s:%d on interface %s\n", conf.multicast_ip, conf.UDPport, conf.local_if );

    struct sockaddr_in addr;                                            // Standard socket setup stuff needed even for multicast UDP
    memset(&addr,0,sizeof(addr));

    int fd;
    struct ip_mreq mreq;

    if ((fd=socket(AF_INET,SOCK_DGRAM,0)) == -1) {			// create what looks like an ordinary UDP socket
      perror("socket");
      terminate = TRUE;
      pthread_exit(NULL);
    }

    addr.sin_family=AF_INET;
    addr.sin_addr.s_addr=htonl(INADDR_ANY);
    addr.sin_port=htons(conf.UDPport);

    if (setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &(int){1}, sizeof(int)) == -1) {
      perror("setsockopt SO_REUSEADDR");
      close(fd);
      terminate = TRUE;
      pthread_exit(NULL);
    }

    if (bind(fd,(struct sockaddr *) &addr,sizeof(addr)) == -1) {	// bind to the receive address
      perror("bind");
      close(fd);
      terminate = TRUE;
      pthread_exit(NULL);
    }

    mreq.imr_multiaddr.s_addr=inet_addr( conf.multicast_ip );			// use setsockopt() to request that the kernel join a multicast group
    mreq.imr_interface.s_addr=inet_addr( conf.local_if );

    if (setsockopt(fd,IPPROTO_IP,IP_ADD_MEMBERSHIP,&mreq,sizeof(mreq)) == -1) {
      perror("setsockopt IP_ADD_MEMBERSHIP");
      close(fd);
      terminate = TRUE;
      pthread_exit(NULL);
    }

//--------------------------------------------------------------------------------------------------------------------------------------------------------

    printf( "Ready to start\n" );

    UDP_slots_empty = UDP_num_slots;                                    // We haven't written to any yet, so all slots are available at the moment
    UDP_first_empty = 0;                                                // The first of those is number zero
    struct mmsghdr *UDP_first_empty_ptr = &msgvecs[UDP_first_empty];	// Set our first empty pointer to the address of the 0th element in the array

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

//  sleep(1);
    printf( "looped on full %lld times.  min = %lld\n", Num_loops_when_full, UDP_slots_empty_min );

    mreq.imr_multiaddr.s_addr=inet_addr( conf.multicast_ip );
    mreq.imr_interface.s_addr=inet_addr( conf.local_if );

    if (setsockopt(fd,IPPROTO_IP,IP_DROP_MEMBERSHIP,&mreq,sizeof(mreq)) == -1) {  // Say we don't want multicast packets any more.
      perror("setsockopt IP_DROP_MEMBERSHIP");
      close(fd);
      pthread_exit(NULL);
    }

    close(fd);                                                                  // Close the file descriptor for the port now we're about the leave

    printf( "Exiting UDP_recv\n");
    pthread_exit(NULL);
}

//------------------------------------------------------------------------------------------------------------------------------------------------------
// UDP_parse - Check UDP packets as they arrive and update the array of pointers to them so we can later address them in sorted order
//------------------------------------------------------------------------------------------------------------------------------------------------------

void *UDP_parse()
{
    printf("UDP_parse started\n");

//---------------- Initialize and declare variables ------------------------

    mwa_udp_packet_t *my_udp;						// Make a local pointer to the UDP packet we're working on.

    uint32_t last_good_packet_sub_time = 0;				// What sub obs was the last packet (for caching)
    uint32_t subobs_mask = 0xFFFFFFF8;					// Mask to apply with '&' to get sub obs from GPS second

    uint32_t start_window = 0;						// What's the oldest packet we're accepting right now?
    uint32_t end_window = 0;						// What's the highest GPS time we can accept before recalculating our usable window?  Hint: We *need* to recalculate that window

    int slot_ndx;							// index into which of the 4 subobs metadata blocks we want
    int rf_ndx;								// index into the position in the meta array we are using for this rf input (for this sub).  NB May be different for the same rf on a different sub.

    subobs_udp_meta_t *this_sub = NULL;					// Pointer to the relevant one of the four subobs metadata arrays

    int loop;

//---------------- Main loop to process incoming udp packets -------------------

    while (!terminate) {

      if ( UDP_removed_from_buff < UDP_added_to_buff ) {		// If there is at least one packet waiting to be processed

        my_udp = &UDPbuf[ UDP_removed_from_buff % UDP_num_slots ];	// then this is a pointer to it.

//---------- At this point in the loop, we are about to process the next arrived UDP packet, and it is located at my_udp ----------

        if ( my_udp->packet_type == 0x20 ) {				// If it's a real packet off the network with some fields in network order, they need to be converted to host order and have a few tweaks made before we can use the packet

          my_udp->subsec_time = ntohs( my_udp->subsec_time );		// Convert subsec_time to a usable uint16_t which at this stage is still within a single second
          my_udp->GPS_time = ntohl( my_udp->GPS_time );			// Convert GPS_time (bottom 32 bits only) to a usable uint32_t

          my_udp->rf_input = ntohs( my_udp->rf_input );			// Convert rf_input to a usable uint16_t
//          my_udp->edt2udp_token = ntohs( my_udp->edt2udp_token );	// Convert edt2udp_token to a usable uint16_t

                                                                        // the bottom three bits of the GPS_time is 'which second within the subobservation' this second is
          my_udp->subsec_time += ( (my_udp->GPS_time & 0b111) * SUBSECSPERSEC ) + 1;	// Change the subsec field to be the packet count within a whole subobs, not just this second.  was 0 to 624.  now 1 to 5000 inclusive. (0x21 packets can be 0 to 5001!)

          my_udp->packet_type = 0x21;					// Now change the packet type to a 0x21 to say it's similar to a 0x20 but in host byte order

        } else {							// If is wasn't a 0x20 packet, the only other packet we handle is a 0x21

//---------- Not for us?

          if ( my_udp->packet_type != 0x21 ) {				// This packet is not a valid packet for us.
//WIP            mon->ignored_for_a_bad_type++;				// Keep track for debugging of how many packets we've seen that we can't handle
            UDP_removed_from_buff++;					// Flag it as used and release the buffer slot.  We don't want to see it again.
            continue;							// start the loop again
          }

        }

//---------- If this packet is for the same sub obs as the previous packet, we can assume a bunch of things haven't changed or rolled over, otherwise we have things to check

        if ( ( my_udp->GPS_time & subobs_mask ) != last_good_packet_sub_time ) {		// If this is a different sub obs than the last packet we allowed through to be processed.

//---------- Arrived too late to be usable?

          if (my_udp->GPS_time < start_window) {			// This packet has a time stamp before the earliest open sub-observation
//WIP            mon->ignored_for_being_too_old++;				// Keep track for debugging of how many packets were too late to process
            UDP_removed_from_buff++;					// Throw it away.  ie flag it as used and release the buffer slot.  We don't want to see it again.
            continue;							// start the loop again
          }

//---------- Further ahead in time than we're ready for?

          if (my_udp->GPS_time > end_window) {				// This packet has a time stamp after the end of the latest open sub-observation.  We need to do some preparatory work before we can process this packet.

            // First we need to know if this is simply the next chronological subobs, or if we have skipped ahead.  NB that a closedown packet will look like we skipped ahead to 2106.  If we've skipped ahead then all current subobs need to be closed.

            if ( end_window == (( my_udp->GPS_time | 0b111 ) - 8) ) {	// our proposed new end window is the udp packet's time with the bottom 3 bits all set.  If that's (only) 8 seconds after the current end window...
              start_window = end_window - 7;				// then our new start window is (and may already have been) the beginning of the subobs which was our last subobs a moment ago. (7 seconds because the second counts are inclusive).
              end_window = my_udp->GPS_time | 0b111;			// new end window is the last second of the subobs for the second we just saw, so round up (ie set) the last three bits
                                                                        // Ensure every subobs but the last one are closed
            } else {							// otherwise the packet stream is so far into the future that we need to close *all* open subobs.
              end_window = my_udp->GPS_time | 0b111;			// new end window is the last second of the subobs for the second we just saw, so round up (ie set) the last three bits
              start_window = end_window - 7;				// then our new start window is the beginning second of the subobs for the packet we just saw.  (7 seconds because the second counts are inclusive).
            }

            for ( loop = 0 ; loop < 4 ; loop++ ) {							// We'll check all subobs meta slots.  If they're too old, we'll rule them off.  NB this will likely not be in time order of subobs
              if ( ( sub[ loop ].subobs < start_window ) && ( sub[ loop ].state == 1 ) ) {		// If this sub obs slot is currently in use by us (ie state==1) and has now reached its timeout ( < start_window )
                sub[ loop ].state = 2;									// set a state flag to tell another thread it's their problem from now on
              }
            }

            if ( my_udp->GPS_time == CLOSEDOWN ) {			// If we've been asked to close down via a udp packet with the time set to the year 2106
              terminate = TRUE;						// tell everyone to shut down.
              UDP_removed_from_buff++;					// Flag it as used and release the buffer slot.  We don't want to see it again.
              continue;							// This will take us out to the while !terminate loop that will shut us down.  (Could have used a break too. It would have the same effect)
            }

            // We now have a new subobs that we need to set up for.  Hopefully the slot we want to use is either empty or finished with and free for reuse.  If not we've overrun the sub writing threads

            slot_ndx = ( my_udp->GPS_time >> 3 ) & 0b11;		// The 3 LSB are 'what second in the subobs' we are.  We want the 2 bits immediately to the left of that.  They will give us our index into the subobs metadata array

            if ( sub[ slot_ndx ].state >= 4 ) {				// If the state is a 4 or 5 or higher, then it's free for reuse, but someone needs to wipe the contents.  I guess that 'someone' means me!
              memset( &sub[ slot_ndx ], 0, sizeof( subobs_udp_meta_t ));	// Among other things, like wiping the array of pointers back to NULL, this memset should set the value of 'state' back to 0.
            }

            if ( sub[ slot_ndx ].state != 0 ) {				// If state isn't 0 now, then it's bad.  We have a transaction to store and the subobs it belongs to can't be set up because its slot is still in use by an earlier subobs

              printf ( "Error: Subobs slot %d still in use. Currently in state %d.\n", slot_ndx, sub[slot_ndx].state );		// If this actually happens during normal operation, we will need to fix the cause or (maybe) bandaid it with a delay here, but for now
              printf ( "Wanted to put gps=%d, subobs=%d, first=%lld, state=1\n", my_udp->GPS_time, ( my_udp->GPS_time & subobs_mask ), UDP_removed_from_buff );
              printf ( "Found so=%d,st=%d,wait=%d,took=%d,first=%lld,last=%lld,startw=%lld,endw=%lld,count=%d,seen=%d\n",
                sub[slot_ndx].subobs, sub[slot_ndx].state, sub[slot_ndx].msec_wait, sub[slot_ndx].msec_took, sub[slot_ndx].first_udp, sub[slot_ndx].last_udp, sub[slot_ndx].udp_at_start_write, sub[slot_ndx].udp_at_end_write, sub[slot_ndx].udp_count, sub[slot_ndx].rf_seen );

              terminate = TRUE;						// Lets make a fatal error so we know we'll spot it
              continue;							// abort what we're attempting and all go home
            }

            sub[ slot_ndx ].subobs = ( my_udp->GPS_time & subobs_mask );	// The subobs number is the GPS time of the beginning second so mask off the bottom 3 bits
            sub[ slot_ndx ].first_udp = UDP_removed_from_buff;		// This was the first udp packet seen for this sub. (0 based)
            sub[ slot_ndx ].state = 1;					// Let's remember we're using this slot now and tell other threads.  NB: The subobs is assumed to be populated *before* this becomes 1

          }

//---------- This packet isn't similar enough to previous ones (ie from the same sub-obs) to assume things, so let's get new pointers

          this_sub = &sub[(( my_udp->GPS_time >> 3 ) & 0b11 )];		// The 3 LSB are 'what second in the subobs' we are.  We want the 2 bits left of that.  They will give us an index into the subobs metadata array which we use to get the pointer to the struct
          last_good_packet_sub_time = ( my_udp->GPS_time & subobs_mask );	// Remember this so next packet we proabably don't need to do these checks and lookups again

        }

//---------- We have a udp packet to add and a place for it to go.  Time to add its details to the sub obs metadata

        rf_ndx = this_sub->rf2ndx[ my_udp->rf_input ];			// Look up the position in the meta array we are using for this rf input (for this sub).  NB May be different for the same rf on a different sub

        if ( rf_ndx == 0 ) {						// If the lookup for this rf input for this sub is still a zero, it means this is the first time we've seen one this sub from this rf input
          this_sub->rf_seen++;						// Increase the number of different rf inputs seen so far
          this_sub->rf2ndx[ my_udp->rf_input ] = this_sub->rf_seen;	// and assign that number for this rf input's metadata index
          rf_ndx = this_sub->rf_seen;					// and get the correct index for this packet too because we'll need that
        }	// WIP Should check for array overrun.  ie that rf_seen has grown past MAX_INPUTS (or is it plus or minus one?)

        this_sub->udp_volts[ rf_ndx ][ my_udp->subsec_time ] = (char *) my_udp->volts;		// This is an important line so lets unpack what it does and why.
        // The 'this_sub' struct stores a 2D array of pointers (udp_volt) to all the udp payloads that apply to that sub obs.  The dimensions are rf_input (sorted by the order in which they were seen on
        // the incoming packet stream) and the packet count (0 to 5001) inside the subobs. My this stage, seconds and subsecs have been merged into a single number so subsec time is already in the range 0 to 5001

        this_sub->last_udp = UDP_removed_from_buff;			// This was the last udp packet seen so far for this sub. (0 based) Eventually it won't be updated and the final value will remain there
        this_sub->udp_count++;						// Add one to the number of udp packets seen this sub obs.  NB Includes duplicates and faked delay packets so can't reliably be used to check if we're finished a sub

//---------- We are done with this packet, EXCEPT if this was the very first for an rf_input for a subobs, or the very last, then we want to duplicate them in the adjacent subobs.
//---------- This is because one packet may contain data that goes in two subobs (or even separate obs!) due to delay tracking

        if ( my_udp->subsec_time == 1 ) {				// If this was the first packet (for an rf input) for a subobs
          my_udp->subsec_time = SUBSECSPERSUB + 1;			// then say it's at the end of a subobs
          my_udp->GPS_time--;						// and back off the second count to put it in the last subobs
        } else {
          if ( my_udp->subsec_time == SUBSECSPERSUB ) {			// If this was the last packet (for an rf input) for a subobs
            my_udp->subsec_time = 0;					// then say it's at the start of a subobs
            my_udp->GPS_time++;						// and increment the second count to put it in the next subobs
          } else {
            UDP_removed_from_buff++;					// We don't need to duplicate this packet, so increment the number of packets we've ever processed (which automatically releases them from the buffer).
          }
        }

//---------- End of processing of this UDP packet ----------

      } else {								// 
//WIP        mon->sub_meta_sleeps++;						// record we went to sleep
        usleep(10000);							// Chill for a bit
      }

    }

//---------- We've been told to shut down ----------

    printf( "Exiting UDP_parse\n" );
    pthread_exit(NULL);
}

//---------------------------------------------------------------------------------------------------------------------------------------------------
// Add metafits info - Every time a new 8 second block of udp packets starts, try to find the metafits data applicable and write it into the subm structure
//---------------------------------------------------------------------------------------------------------------------------------------------------

void add_meta_fits()
{

    DIR *dir;
    struct dirent *dp;
    int len_filename;

    char metafits_file[300];						// The metafits file name

    INT64 bcsf_obsid;							// 'best candidate so far' for the metafits file
    INT64 mf_obsid;

    int loop;
    int subobs_ready2write;
    int meta_result;							// Temp storage for the result before we write it back to the array for others to see.  Once we do that, we can't change our mind.
    BOOL go4meta;

    subobs_udp_meta_t *subm;						// pointer to the sub metadata array I'm working on

    struct timespec started_meta_write_time;
    struct timespec ended_meta_write_time;

    fitsfile *fptr;										// FITS file pointer, defined in fitsio.h
//    char card[FLEN_CARD];									// Standard string lengths defined in fitsio.h
    int status;											// CFITSIO status value MUST be initialized to zero!
    int hdupos;
//    int single, nkeys, ii;

    char channels_ascii[200];						// Space to read in the metafits channel string before we parse it into individual channels
//    int len;
    unsigned int ch_index;						// assorted variables to assist parsing the channel ascii csv into something useful like an array of ints
    char* token;
    char* saveptr;
    char* ptr;
    int temp_CHANNELS[24];
    int course_swap_index;

//---------------- Main loop to live in until shutdown -------------------

    printf("add_meta_fits started\n");

    clock_gettime( CLOCK_REALTIME, &ended_meta_write_time);		// Fake the ending time for the last metafits file ('cos like there wasn't one y'know but the logging will expect something)

    while (!terminate) {						// If we're not supposed to shut down, let's find something to do

//---------- look for a sub which needs metafits info ----------

      subobs_ready2write = -1;						// Start by assuming there's nothing to do

      for ( loop = 0 ; loop < 4 ; loop++ ) {				// Look through all four subobs meta arrays

        if ( ( sub[loop].state == 1 ) && ( sub[loop].meta_done == 0 ) ) {	// If this sub is ready to have M&C metadata added

          if ( subobs_ready2write == -1 ) {				// check if we've already found a different one to do and if we haven't

            subobs_ready2write = loop;					// then mark this one as the best so far

          } else {							// otherwise we need to work out

            if ( sub[subobs_ready2write].subobs > sub[loop].subobs ) {	// if that other one was for a later sub than this one
              subobs_ready2write = loop;				// in which case, mark this one as the best so far
            }
          }
        }
      }									// We left this loop with an index to the best sub to do or a -1 if none available.

//---------- if we don't have one ----------

      if ( subobs_ready2write == -1 ) {					// if there is nothing to do
        usleep(100000);							// Chill for a longish time.  NO point in checking more often than a couple of times a second.
      } else {								// or we have some work to do!  Like a whole metafits file to process!

//---------- but if we *do* have metafits info needing to be written out ----------

        clock_gettime( CLOCK_REALTIME, &started_meta_write_time);	// Record the start time before we actually get started.  The clock starts ticking from here.

        subm = &sub[subobs_ready2write];				// Temporary pointer to our sub's metadata array

        subm->meta_msec_wait = ( ( started_meta_write_time.tv_sec - ended_meta_write_time.tv_sec ) * 1000 ) + ( ( started_meta_write_time.tv_nsec - ended_meta_write_time.tv_nsec ) / 1000000 );	// msec since the last metafits processed

        subm->meta_done = 2;						// Record that we're working on this one!

        meta_result = 5;						// Start by assuming we failed  (ie result==5).  If we get everything working later, we'll swap this for a 4.

        go4meta = TRUE;							// All good so far

//---------- Look in the metafits directory and find the most applicable metafits file ----------

        if ( go4meta ) {										// If everything is okay so far, enter the next block of code
          go4meta = FALSE;										// but go back to assuming a failure unless we succeed in the next bit

          bcsf_obsid = 0;										// 'best candidate so far' is very bad indeed (ie non-existent)

          dir = opendir( conf.metafits_dir );								// Open up the directory where metafits live

          while ( (dp=readdir(dir)) != NULL) {                                         			// Read an entry and while there are still directory entries to look at
            if ( ( dp->d_type == DT_REG ) && ( dp->d_name[0] != '.' ) ) {					// If it's a regular file (ie not a directory or a named pipe etc) and it's not hidden
              if ( ( (len_filename = strlen( dp->d_name )) >= 15) && ( strcmp( &dp->d_name[len_filename-14], "_metafits.fits" ) == 0 ) ) {           // If the filename is at least 15 characters long and the last 14 characters are "_metafits.fits"

                mf_obsid = strtoll( dp->d_name,0,0 );							// Get the obsid from the name

                if ( mf_obsid <= subm->subobs ) {							// if the obsid is less than or the same as our subobs id, then it's a candidate for the one we need

                  if ( mf_obsid > bcsf_obsid ) {							// If this metafits is later than the 'best candidate so far'?
                    bcsf_obsid = mf_obsid;								// that would make it our new best candidate so far

//printf( "best metafits #%lld\n", bcsf_obsid );

                    go4meta = TRUE;									// We've found at least one file worth looking at.
                  }

                }
              }
            }
          }
          closedir(dir);
        }

//---------- Open the 'best candidate so far' metafits file ----------

        if ( go4meta ) {										// If everything is okay so far, enter the next block of code
          go4meta = FALSE;										// but go back to assuming a failure unless we succeed in the next bit

          sprintf( metafits_file, "%s/%lld_metafits.fits", conf.metafits_dir, bcsf_obsid );		// Construct the full file name including path

//    printf( "About to read: %s/%lld_metafits.fits\n", conf.metafits_dir, bcsf_obsid );

          status = 0;											// CFITSIO status value MUST be initialized to zero!

          if ( !fits_open_file( &fptr, metafits_file, READONLY, &status ) ) {				// Try to open the file and if it works

            fits_get_hdu_num( fptr, &hdupos );								// get the current HDU position

//            fits_get_hdrspace(fptr, &nkeys, NULL, &status);						// get # of keywords
//            printf("Header listing for HDU #%d:\n", hdupos);
//            for (ii = 1; ii <= nkeys; ii++) {								// Read and print each keywords
//              fits_read_record(fptr, ii, card, &status);
//              printf("%s\n", card);
//            }
//            printf("END\n\n");									// terminate listing with END

//---------- Pull out the header info we need.  Format must be one of TSTRING, TLOGICAL (== int), TBYTE, TSHORT, TUSHORT, TINT, TUINT, TLONG, TULONG, TLONGLONG, TFLOAT, TDOUBLE ----------

            fits_read_key( fptr, TLONGLONG, "GPSTIME", &(subm->GPSTIME), NULL, &status );		// Read the GPSTIME of the metafits observation (should be same as bcsf_obsid but read anyway)
            if (status) {
              printf ( "Failed to read GPSTIME\n" );
            }

            fits_read_key( fptr, TINT, "EXPOSURE", &(subm->EXPOSURE), NULL, &status );			// Read the EXPOSURE time from the metafits
            if (status) {
              printf ( "Failed to read Exposure\n" );
            }

            fits_read_key( fptr, TSTRING, "PROJECT", &(subm->PROJECT), NULL, &status );
            if (status) {
              printf ( "Failed to read project id\n" );
            }

            fits_read_key( fptr, TSTRING, "MODE", &(subm->MODE), NULL, &status );
            if (status) {
              printf ( "Failed to read mode\n" );
            }

            if ( (subm->GPSTIME + (INT64)subm->EXPOSURE - 1) < (INT64)subm->subobs ) {			// If the last observation has expired. (-1 because inclusive)
              strcpy( subm->MODE, "NO_CAPTURE" );							// then change the mode to NO_CAPTURE
            }

//---------- Parsing the sky frequency (coarse) channel is a whole job in itself! ----------

            fits_read_key_longstr(fptr, "CHANNELS", &saveptr, NULL, &status);
            if (status) {
              printf ( "Failed to read Channels\n" );
            }

            strcpy(channels_ascii, saveptr);
            free(saveptr);

//            fits_read_string_key( fptr, "CHANNELS", 1, 190, channels_ascii, &len, NULL, &status );	// First read the metafits line and its 'CONTINUE' lines into a string
//            ffgsky( fptr, "CHANNELS", 1, 190, channels_ascii, &len, NULL, &status );	// First read the metafits line and its 'CONTINUE' lines into a string
//            if (status) {
//              printf ( "Failed to read Channels\n" );
//            }

            ch_index = 0;										// Start at channel number zero (of 0 to 23)
            saveptr = NULL;
            ptr = channels_ascii;									// Get a temp copy (but only of the pointer. NOT THE STRING!) that we can update as we step though the channels in the csv list

            while ( ( ( token = strtok_r(ptr, ",", &saveptr) ) != NULL ) & ( ch_index < 24 ) ) {	// Get a pointer to the next number and assuming there *is* one and we still want more
              temp_CHANNELS[ch_index++] = atoi(token);							// turn it into an int and remember it (although it isn't sorted yet)
              ptr = saveptr;										// Not convinced this line is needed, but it was there in 'recombine' so I left it
            }

            if (ch_index != 24) {
	      printf("Did not find 24 channels in metafits file.\n");
            }

// From the RRI user manual:
// "1. The DR coarse PFB outputs the 256 channels in a fashion that the first 128 channels appear in sequence
// followed by the 129 channels and then 256 down to 130 appear. The setfreq is user specific command wherein
// the user has to enter the preferred 24 channesl in sequence to be transported using the 3 fibers. [ line 14 Appendix- E]"
// Clear as mud?  Yeah.  I thought so too.

// So we want to look through for where a channel number is greater than, or equal to 129.  We'll assume there are already sorted by M&C

            course_swap_index = 24;									// start by assuming there are no channels to swap

            // find the index where the channels are swapped i.e. where 129 exists
            for (int i = 0; i < 24; ++i) {
              if ( temp_CHANNELS[i] >= 129 ) {
		course_swap_index = i;
		break;
	      }
            }

            // Now reorder freq array based on the course channel boundary around 129
            for (int i = 0; i < 24; ++i) {
	      if (i < course_swap_index) {
		subm->CHANNELS[i] = temp_CHANNELS[i];
              } else {
		subm->CHANNELS[23-i+(course_swap_index)] = temp_CHANNELS[i];				// I was confident this line was correct back when 'recombine' was written!
              }
            }

            subm->COARSE_CHAN = subm->CHANNELS[ conf.coarse_chan - 1 ];					// conf.coarse_chan numbers are 1 to 24 inclusive, but the array index is 0 to 23 incl.

            if ( subm->COARSE_CHAN == 0 ) printf ( "Failed to parse valid coarse channel\n" );		// Check we found something plausible

/*
fits_read_string_key( fptr, "CHANNELS", 1, 190, channels_ascii, &len, NULL, &status );	// First read the metafits line and its 'CONTINUE' lines into a string
printf( "'%s' parses into ", channels_ascii );
for (int i = 0; i < 24; ++i) {
  printf( "%d,", subm->CHANNELS[i] );
}
printf( " to give %d for coarse chan %d\n", subm->COARSE_CHAN, conf.coarse_chan );
*/

//---------- Hopefully we did that okay, although we better check it handles the reversing above channel 128 correctly ----------

            fits_read_key( fptr, TFLOAT, "FINECHAN", &(subm->FINECHAN), NULL, &status );
            if (status) {
              printf ( "Failed to read FineChan\n" );
            }
            subm->FINECHAN_hz = (int) (subm->FINECHAN * 1000.0);					// We'd prefer the fine channel width in Hz rather than kHz.

            fits_read_key( fptr, TFLOAT, "INTTIME", &(subm->INTTIME), NULL, &status );
            if (status) {
              printf ( "Failed to read Integration Time\n" );
            }
            subm->INTTIME_msec = (int) (subm->INTTIME * 1000.0);					// We'd prefer the integration time in msecs rather than seconds.

            fits_read_key( fptr, TINT, "NINPUTS", &(subm->NINPUTS), NULL, &status );
            if (status) {
              printf ( "Failed to read NINPUTS\n" );
            }

            fits_read_key( fptr, TLONGLONG, "UNIXTIME", &(subm->UNIXTIME), NULL, &status );
            if (status) {
              printf ( "Failed to read UNIXTIME\n" );
            }

//---------- We now have everything we need from the 1st HDU ----------

            clock_gettime( CLOCK_REALTIME, &ended_meta_write_time);

            subm->meta_msec_took = ( ( ended_meta_write_time.tv_sec - started_meta_write_time.tv_sec ) * 1000 ) + ( ( ended_meta_write_time.tv_nsec - started_meta_write_time.tv_nsec ) / 1000000 );	// msec since this sub started

            subm->meta_done = meta_result;					// Record that we've finished working on this one even if we gave up.  Will be a 4 or a 5 depending on whether it worked or not.

//            printf("meta: so=%d,bcsf=%lld,GPSTIME=%lld,EXPOSURE=%d,PROJECT=%s,MODE=%s,CHANNELS=%s,FINECHAN_hz=%d,INTTIME_msec=%d,NINPUTS=%d,UNIXTIME=%lld,len=%d,st=%d,wait=%d,took=%d\n",
//              subm->subobs, bcsf_obsid, subm->GPSTIME, subm->EXPOSURE, subm->PROJECT, subm->MODE, subm->CHANNELS, subm->FINECHAN_hz, subm->INTTIME_msec, subm->NINPUTS, subm->UNIXTIME, len, subm->meta_done, subm->meta_msec_wait, subm->meta_msec_took);

//meta: so=1283591768,bcsf=1283585640,GPSTIME=1283585640,EXPOSURE=600,CHANNELS=57,58,59,60,61,62,63,64,65,66,67,68,121,122,123,124,125,126,127,128,129,130,131,132&,len=84,st=5,wait=100,took=0
//meta: so=1283591776,bcsf=1283585640,GPSTIME=1283585640,EXPOSURE=600,CHANNELS=57,58,59,60,61,62,63,64,65,66,67,68,121,122,123,124,125,126,127,128,129,130,131,132&,len=84,st=5,wait=300,took=0
//meta: so=1283591784,bcsf=1283585640,GPSTIME=1283585640,EXPOSURE=600,CHANNELS=57,58,59,60,61,62,63,64,65,66,67,68,121,122,123,124,125,126,127,128,129,130,131,132&,len=84,st=5,wait=8008,took=0

          }

          if (status == END_OF_FILE)  status = 0;							// Reset after normal error

          fits_close_file(fptr, &status);
          if (status) fits_report_error(stderr, status);						// print any error message

        }
      }
    }

/*

preinitialise all the start_bytes in the array to ( UDP_PAYLOAD_SIZE - ( whole sample delay for this rf_input ) * 2 )  NB: Each delay is a sample, ie two bytes, not one!!!

*/

}

//---------------------------------------------------------------------------------------------------------------------------------------------------
// makesub - Every time an 8 second block of udp packets is available, try to generate a sub files for the correlator
//---------------------------------------------------------------------------------------------------------------------------------------------------

void *makesub()
{

//---------------- Initialize and declare variables ------------------------

    DIR *dir;
    struct dirent *dp;
    struct stat filestats;
    int len_filename;
    char this_file[300],best_file[300],dest_file[300];			// The name of the file we're looking at, the name of the best file we've found so far, and the sub's final destination name

    time_t earliest_file;						// Holding space for the date we need to beat when looking for old .free files

    char *ext_shm_buf;							// Pointer to the header of where all the external data is after the mmap
    char *dest;								// Working copy of the pointer into shm
    int ext_shm_fd;                                                     // File descriptor for the shmem block

    size_t transfer_size;
    size_t desired_size;						// The total size (including header and zeroth block) that this sub file needs to be

    int loop;
    int subobs_ready2write;
    int left_this_line;
    int sub_result;							// Temp storage for the result before we write it back to the array for others to see.  Once we do that, we can't change our mind.
    BOOL go4sub;

    mwa_udp_packet_t dummy_udp={0};					// Make a dummy udp packet full of zeros and NULLs.  We'll use this to stand in for every missing packet!
    void *dummy_volt_ptr = &dummy_udp.volts[0];				// and we'll remember where we can find UDP_PAYLOAD_SIZE (4096LL) worth of zeros

    subobs_udp_meta_t *subm;						// pointer to the sub metadata array I'm working on

    int block;								// loop variable
    int MandC_rf;							// loop variable
    int ninputs_xgpu;							// The number of inputs the sub file must be padded out to

//    MandC_obs_t my_MandC_obs;						// Room for the observation metafits/metabin info
    MandC_meta_t my_MandC_meta[MAX_INPUTS];				// Working copies of the changeable metafits/metabin data for this subobs
    MandC_meta_t *my_MandC;						// Make a temporary pointer to the M&C metadata for one rf input

    int delay;								// temp variable for delay calculations

    int source_packet;							// which packet (of 5002) contains the first byte of data we need
    int source_offset;							// what is the offset in that packet of the first byte we need
    int source_remain;							// How much data is left from that offset to the end of the udp packet

    char *source_ptr;
    int bytes2copy;

    char months[] = "Jan,Feb,Mar,Apr,May,Jun,Jul,Aug,Sep,Oct,Nov,Dec ";	// Needed for date to ASCII conversion
    char utc_start[200];						// Needed for date to ASCII conversion
    int mnth;								// The current month
    char srch[4] = {0,0,0,0};						// Really only need the last byte set to a null as a string terminator for 3 character months

    long int my_time;
    char* t;								// General pointer to char but used for ctime output

    struct timespec started_sub_write_time;
    struct timespec ended_sub_write_time;

//---------------- Prepare the hard coded rf_input to correlator order mapping array ------------------------

    UINT16 hc_rf_inputs[256] = {

//      rf_input values in 128T correlator order.  Use to sparce populate a 65536 element back-lookup table.  ie Enter this value, return the correlator order 0-255

//    LONG BASELINE CONFIGURATION

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

//      SHORT BASELINE CONFIGURATION
/*
      22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,
      42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,
      62,63,64,65,66,67,68,69,70,71,72,73,74,75,76,77,
      82,83,84,85,86,87,88,89,90,91,92,93,94,95,96,97,
      122,123,124,125,126,127,128,129,130,131,132,133,134,135,136,137,
      162,163,164,165,166,167,168,169,170,171,172,173,174,175,176,177,
      182,183,184,185,186,187,188,189,190,191,192,193,194,195,196,197,
      2002,2003,2004,2005,2006,2007,2008,2009,2010,2011,2012,2013,2014,2015,2016,2017,
      2018,2019,2020,2021,2022,2023,2024,2025,2026,2027,2028,2029,2030,2031,2032,2033,
      2034,2035,2036,2037,2038,2039,2040,2041,2042,2043,2044,2045,2046,2047,2048,2049,
      2050,2051,2052,2053,2054,2055,2056,2057,2058,2059,2060,2061,2062,2063,2064,2065,
      2066,2067,2068,2069,2070,2071,2072,2073,2074,2075,2076,2077,2078,2079,2080,2081,
      2082,2083,2084,2085,2086,2087,2088,2089,2090,2091,2092,2093,2094,2095,2096,2097,
      2098,2099,2100,2101,2102,2103,2104,2105,2106,2107,2108,2109,2110,2111,2112,2113,
      2114,2115,2116,2117,2118,2119,2120,2121,2122,2123,2124,2125,2126,2127,2128,2129,
      2130,2131,2132,2133,2134,2135,2136,2137,2138,2139,2140,2141,2142,2143,2144,2145
*/
    };

//---------------- Main loop to live in until shutdown -------------------

    printf("Makesub started\n");

    clock_gettime( CLOCK_REALTIME, &ended_sub_write_time);		// Fake the ending time for the last sub file ('cos like there wasn't one y'know but the logging will expect something)

    while (!terminate) {						// If we're not supposed to shut down, let's find something to do

//---------- look for a sub to write out ----------

      subobs_ready2write = -1;						// Start by assuming there's nothing to write out

      for ( loop = 0 ; loop < 4 ; loop++ ) {				// Look through all four subobs meta arrays

        if ( sub[loop].state == 2 ) {					// If this sub is ready to write out

          if ( subobs_ready2write == -1 ) {				// check if we've already found a different one to write out and if we haven't

            subobs_ready2write = loop;					// then mark this one as the best so far

          } else {							// otherwise we need to work out

            if ( sub[subobs_ready2write].subobs > sub[loop].subobs ) {	// if that other one was for a later sub than this one
              subobs_ready2write = loop;				// in which case, mark this one as the best so far
            }
          }
        }
      }									// We left this loop with an index to the best sub to write or a -1 if none available.

//---------- if we don't have one ----------

      if ( subobs_ready2write == -1 ) {					// if there is nothing to do
//WIP        mon->sub_write_sleeps++;					// record we went to sleep
        usleep(20000);							// Chill for a bit.  Well 50 msecs is 
      } else {								// or we have some work to do!  Like a whole sub file to write out!

//---------- but if we *do* have a sub file ready to write out ----------

        clock_gettime( CLOCK_REALTIME, &started_sub_write_time);	// Record the start time before we actually get started.  The clock starts ticking from here.

        subm = &sub[subobs_ready2write];				// Temporary pointer to our sub's metadata array

        subm->msec_wait = ( ( started_sub_write_time.tv_sec - ended_sub_write_time.tv_sec ) * 1000 ) + ( ( started_sub_write_time.tv_nsec - ended_sub_write_time.tv_nsec ) / 1000000 );	// msec since the last sub ending

        subm->udp_at_start_write = UDP_added_to_buff;			// What's the udp packet number we've received (EVEN IF WE HAVEN'T LOOKED AT IT!) at the time we start to process this sub for writing

        subm->state = 3;						// Record that we're working on this one!

        sub_result = 5;							// Start by assuming we failed  (ie result==5).  If we get everything working later, we'll swap this for a 4.

        go4sub = FALSE;							// Start by assuming we failed.  We can set this TRUE if everything works out later

//---------- Fake a metafits file ----------

        go4sub = TRUE;													// Say it all looks okay

//        my_MandC_obs.GPSTIME = subm->subobs;										// Assume this subobs is the first of an observation
//        my_MandC_obs.EXPOSURE = 8;
//        strcpy( my_MandC_obs.MODE, "HW_LFILES" );
//        my_MandC_obs.CHANNELS[100] = {0,57,58,59,60,61,62,63,64,65,66,67,68,69,70,71,72,73,74,75,76,77,78,79,80,81,82,83,84,85,86};
//        my_MandC_obs.FINECHAN_hz = 10000;
//        my_MandC_obs.INTTIME_msec = 1000;
//        my_MandC_obs.NINPUTS = 256;
//        my_MandC_obs.UNIXTIME = ( my_MandC_obs.GPSTIME + GPS_offset );

        if ( subm->NINPUTS > MAX_INPUTS ) subm->NINPUTS = MAX_INPUTS;							// Don't allow more inputs than MAX_INPUTS

        ninputs_xgpu = ((subm->NINPUTS + 15) & 0xfff0);									// Get this from 'ninputs' rounded up to multiples of 16

        transfer_size = ( ( SUB_LINE_SIZE * (BLOCKS_PER_SUB+1LL) ) * ninputs_xgpu );					// Should be 5275648000 for 256T in 160+1 blocks
        desired_size = transfer_size + SUBFILE_HEADER_SIZE;								// Should be 5275652096 for 256T in 160+1 blocks plus 4K header (1288001 x 4K for dd to make)

        for ( loop = 0 ; loop < ninputs_xgpu ; loop++ ) {								// populate the metadata array for all rf_inputs in this subobs incl padding ones
          delay = 0;
          my_MandC_meta[loop].rf_input = hc_rf_inputs[loop];								// Hardcode for now
          my_MandC_meta[loop].start_byte = ( UDP_PAYLOAD_SIZE - ( delay * 2  ) );					// NB: Each delay is a sample, ie two bytes, not one!!!
          my_MandC_meta[loop].seen_order = subm->rf2ndx[ my_MandC_meta[loop].rf_input ];				// If they weren't seen, they will be 0 which maps to NULL pointers which will be replaced with padded zeros
        }

        sprintf( dest_file, "%s/%lld_%d_%d.sub", conf.shared_mem_dir, subm->GPSTIME, subm->subobs, subm->COARSE_CHAN );	// Construct the full file name including path
//        sprintf( dest_file, "%s/%lld_%d_%d.free", conf.shared_mem_dir, subm->GPSTIME, subm->subobs, subm->COARSE_CHAN );	// Construct the full file name including path

//---------- Make a 4K ASCII header for the sub file ----------

        memset( sub_header, 0, SUBFILE_HEADER_SIZE );									// Pre fill with null chars just to ensure we don't leave junk anywhere in the header

        INT64 obs_offset = subm->subobs - subm->GPSTIME;								// How long since the observation started?

        my_time = subm->UNIXTIME - (8 * 60 * 60);									// Adjust for time zone AWST (Perth time to GMT)
        t = ctime( &my_time );

        memcpy ( srch,&t[4],3 );
        mnth = ( strstr ( months, srch ) - months )/4 + 1;

        sprintf( utc_start, "%.4s-%02d-%.2s-%.8s", &t[20], mnth, &t[8], &t[11] );					// Shift all the silly date stuff into a string

        if ( utc_start[8] == ' ' ) utc_start[8] = '0';

        char* head_mask =
          "HDR_SIZE %d\n"
          "POPULATED 1\n"
          "OBS_ID %d\n"
          "SUBOBS_ID %d\n"
          "MODE %s\n"
          "UTC_START %s\n"
          "OBS_OFFSET %lld\n"
          "NBIT 8\n"
          "NPOL 2\n"
          "NTIMESAMPLES %lld\n"
          "NINPUTS %d\n"
          "NINPUTS_XGPU %d\n"
          "APPLY_PATH_WEIGHTS 0\n"
          "APPLY_PATH_DELAYS 0\n"
          "INT_TIME_MSEC %d\n"
          "FSCRUNCH_FACTOR %d\n"
          "APPLY_VIS_WEIGHTS 0\n"
          "TRANSFER_SIZE %lld\n"
          "PROJ_ID %s\n"
          "EXPOSURE_SECS %d\n"
          "COARSE_CHANNEL %d\n"
          "CORR_COARSE_CHANNEL %d\n"
          "SECS_PER_SUBOBS 8\n"
          "UNIXTIME %d\n"
          "UNIXTIME_MSEC 0\n"
          "FINE_CHAN_WIDTH_HZ %d\n"
          "NFINE_CHAN %d\n"
          "BANDWIDTH_HZ %lld\n"
          "SAMPLE_RATE %lld\n"
          "MC_IP 0.0.0.0\n"
          "MC_PORT 0\n"
          "MC_SRC_IP 0.0.0.0\n"
          ;

        sprintf( sub_header, head_mask, SUBFILE_HEADER_SIZE, subm->GPSTIME, subm->subobs, subm->MODE, utc_start, obs_offset,
              NTIMESAMPLES, subm->NINPUTS, ninputs_xgpu, subm->INTTIME_msec, (subm->FINECHAN_hz/ULTRAFINE_BW), transfer_size, subm->PROJECT, subm->EXPOSURE, subm->COARSE_CHAN,
              conf.coarse_chan, subm->UNIXTIME, subm->FINECHAN_hz, (COARSECHAN_BANDWIDTH/subm->FINECHAN_hz), COARSECHAN_BANDWIDTH, SAMPLES_PER_SEC );

//        printf( "--------------------\n%s--------------------\n", sub_header );

//---------- Look in the shared memory directory and find the oldest .free file of the correct size ----------

        if ( go4sub ) {											// If everything is okay so far, enter the next block of code
          go4sub = FALSE;										// but go back to assuming a failure unless we succeed in the next bit

          dir = opendir( conf.shared_mem_dir );								// Open up the shared memory directory

          earliest_file = 0xFFFFFFFFFFFF;								// Pick some ridiculous date in the future.  This is the date we need to beat.

          while ( (dp=readdir(dir)) != NULL) {                                         			// Read an entry and while there are still directory entries to look at
            if ( ( dp->d_type == DT_REG ) && ( dp->d_name[0] != '.' ) ) {				// If it's a regular file (ie not a directory or a named pipe etc) and it's not hidden
              if ( ( (len_filename = strlen( dp->d_name )) >= 5) && ( strcmp( &dp->d_name[len_filename-5], ".free" ) == 0 ) ) {           // If the filename is at least 5 characters long and the last 5 characters are ".free"

                sprintf( this_file, "%s/%s", conf.shared_mem_dir, dp->d_name );				// Construct the full file name including path
                if ( stat ( this_file, &filestats ) == 0 ) {						// Try to read the file statistics and if they are available
                  if ( filestats.st_size == desired_size ) {						// If the file is exactly the size we need

//printf( "File %s has size = %ld and a ctime of %ld\n", this_file, filestats.st_size, filestats.st_ctim.tv_sec );

                    if ( filestats.st_ctim.tv_sec < earliest_file ) {					// and this file has been there longer than the longest one we've found so far (ctime)
                      earliest_file = filestats.st_ctim.tv_sec;						// We have a new 'oldest' time to beat
                      strcpy( best_file, this_file );							// and we'll store its name
                      go4sub = TRUE;									// We've found at least one file we can reuse.  It may not be the best, but we know we can do this now.
                    }
                  }
                }
              }
            }
          }
          closedir(dir);
        }

//---------- Try to mmap that file (assuming we found one) so we can treat it like RAM (which it actually is) ----------

        if ( go4sub ) {											// If everything is okay so far, enter the next block of code
          go4sub = FALSE;										// but go back to assuming a failure unless we succeed in the next bit

//printf( "The winner is %s\n", best_file );

          if ( rename( best_file, conf.temp_file_name ) != -1 ) {					// If we can rename the file to our temporary name

            if ( (ext_shm_fd = shm_open( &conf.temp_file_name[8], O_RDWR, 0666)) != -1 ) {		// Try to open the shmem file (after removing "/dev/shm" ) and if it opens successfully

              if ( (ext_shm_buf = (char *) mmap (NULL, desired_size, PROT_READ | PROT_WRITE, MAP_SHARED , ext_shm_fd, 0)) != ((char *)(-1)) ) {      // and if we can mmap it successfully
                go4sub = TRUE;										// Then we are all ready to use the buffer so remember that
              } else {
                printf( "mmap failed\n" );
              }

              close( ext_shm_fd );									// If the shm_open worked we want to close the file whether or not the mmap worked

            } else {
              printf( "shm_open failed\n" ) ;
            }

          } else {
            printf( "Failed rename\n" );
          }
        }

//---------- Hopefully we have an mmap to a sub file in shared memory and we're ready to fill it with data ----------

        if ( go4sub ) {								// If everything is good to go so far

          dest = ext_shm_buf;							// we'll be trying to write to this block of RAM in strict address order, starting at the beginning (ie ext_shm_buf)

          dest = mempcpy( dest, sub_header, SUBFILE_HEADER_SIZE );		// Do the memory copy from the preprepared 4K subfile header to the beginning of the sub file

          for ( MandC_rf = 0; MandC_rf < ninputs_xgpu; MandC_rf++ ) {		// The zeroth block is the size of the padded number of inputs times SUB_LINE_SIZE
            dest = mempcpy( dest, blank_sub_line, SUB_LINE_SIZE );		// write them out one at a time
          }

          for ( block = 1; block <= BLOCKS_PER_SUB; block++ ) {			// We have 160 (or whatever) blocks of real data to write out. We'll do them in time order (50ms each)

            for ( MandC_rf = 0; MandC_rf < ninputs_xgpu; MandC_rf++ ) {		// Now step through the M&C ordered rf inputs for the inputs the M&C says should be included in the sub file.  This is likely to be similar to the list we actually saw, but may not be identical!

              my_MandC = &my_MandC_meta[MandC_rf];				// Make a temporary pointer to the M&C metadata for this rf input.  NB these are sorted in order they need to be written out as opposed to the order the packets arrived.

              left_this_line = SUB_LINE_SIZE;

	      while ( left_this_line > 0 ) {					// Keep going until we've finished writing out a whole line of the sub file

	        source_packet = ( my_MandC->start_byte / UDP_PAYLOAD_SIZE );	// Initially the udp payload size is a power of 2 so this could be done faster with a bit shift, but what would happen if the packet size changed?
	        source_offset = ( my_MandC->start_byte % UDP_PAYLOAD_SIZE );	// Initially the udp payload size is a power of 2 so this could be done faster with a bit mask, but what would happen if the packet size changed?
	        source_remain = ( UDP_PAYLOAD_SIZE - source_offset );		// How much of the packet is left in bytes?  It should be an even number and at least 2 or I have a bug in the logic or code

	        source_ptr = subm->udp_volts[my_MandC->seen_order][source_packet];	// Pick up the pointer to the udp volt data we need (assuming we ever saw it arrive)
	        if ( source_ptr == NULL ) {						// but if it never arrived
	          source_ptr = dummy_volt_ptr;						// point to our pre-prepared, zero filled, fake packet we use when the real one isn't available
                  subm->udp_dummy++;							// The number of dummy packets we needed to insert to pad things out. Make a note for reporting and debug purposes
                }

                bytes2copy = ( (source_remain < left_this_line) ? source_remain : left_this_line );		// Get the minimum of the remaining length of the udp volt payload or the length of line left to populate

                dest = mempcpy( dest, source_ptr, bytes2copy );				// Do the memory copy from the udp payload to the sub file and update the destination pointer

                left_this_line -= bytes2copy;						// Keep up to date with the remaining amount of work needed on this line
                my_MandC->start_byte += bytes2copy;					// and where we are up to in reading them

              }									// We've finished the udp packet

            }									// We've finished the signal chain for this block

          }									// We've finished the block

          // By here, the entire sub has been written out to shared memory and it's time to close it out and rename it so it becomes available for other programs

          munmap(ext_shm_buf, desired_size);					// Release the mmap for the whole sub file

          if ( rename( conf.temp_file_name, dest_file ) != -1 ) {		// Rename my temporary file to a final sub file name, thus releasing it to other programs
            sub_result = 4;							// This was our last check.  If we got to here, the sub file worked, so prepare to write that to monitoring system
          } else {
            sub_result = 10;							// This was our last check.  If we got to here, the sub file rename failed, so prepare to write that to monitoring system
            printf( "Final rename failed.\n" ) ;				// but if that fails, I'm not really sure what to do.  Let's just note it and see if it ever happens
          }

        }								// We've finished the sub file writing and closed and renamed the file.

//---------- We're finished or we've given up.  Either way record the new state and elapsed time ----------

        subm->udp_at_end_write = UDP_added_to_buff;			// What's the udp packet number we've received (EVEN IF WE HAVEN'T LOOKED AT IT!) at the time we finish processing this sub for writing

        clock_gettime( CLOCK_REALTIME, &ended_sub_write_time);

        subm->msec_took = ( ( ended_sub_write_time.tv_sec - started_sub_write_time.tv_sec ) * 1000 ) + ( ( ended_sub_write_time.tv_nsec - started_sub_write_time.tv_nsec ) / 1000000 );	// msec since this sub started

/*
        if ( ( subm->udp_count == 1280512 ) && ( subm->udp_dummy == 1310720 ) ) {						//count=1280512,dummy=1310720 is a perfect storm.  No missing packets, but I couldn't find any packets at all.
          printf ( "my_MandC_meta array\n" );
          for ( loop = 0 ; loop < ninputs_xgpu ; loop++ ) {
            printf( "loop=%d,rf=%d,startb=%d,order=%d\n", loop, my_MandC_meta[loop].rf_input, my_MandC_meta[loop].start_byte, my_MandC_meta[loop].seen_order );
          }

          printf ( "rf2ndx array (non-zero entries). Seen=%d\n", subm->rf_seen );
          for ( loop = 0 ; loop < 65536 ; loop++ ) {
            if ( subm->rf2ndx[ loop ] != 0 ) {
              printf ( "loop=%d,ndx=%d\n", loop, subm->rf2ndx[ loop ] );
            }
          }
        }
*/

        subm->state = sub_result;					// Record that we've finished working on this one even if we gave up.  Will be a 4 or a 5 depending on whether it worked or not.

        printf("now=%lld,so=%d,ob=%lld,st=%d,wait=%d,took=%d,first=%lld,last=%lld,startw=%lld,endw=%lld,count=%d,dummy=%d,seen=%d\n",
            (INT64)(ended_sub_write_time.tv_sec - GPS_offset), subm->subobs, subm->GPSTIME, subm->state, subm->msec_wait, subm->msec_took, subm->first_udp, subm->last_udp, subm->udp_at_start_write, subm->udp_at_end_write, subm->udp_count, subm->udp_dummy, subm->rf_seen);

      }

    }									// Jump up to the top and look again (as well as checking if we need to shut down)

//---------- We've been told to shut down ----------

    printf( "Exiting makesub\n" );

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
    printf("\n\n To run:        /home/mwa/udp2sub -f mwax.conf -i 1\n\n");

    printf("                    -f Configuration file to use for settings\n");
    printf("                    -i Instance number on server, if multiple copies per server in use\n");
}

// ------------------------ Start of world -------------------------

int main(int argc, char **argv)
{
    int prog_build = BUILD;                             // Build number.  Remember to update each build revision!

//---------------- Parse command line parameters ------------------------

    int instance = 0;
    char *conf_file = "mwax.conf";				// Default configuration file. Probably should be on vulcan eventually.

    while (argc > 1 && argv[1][0] == '-') {
      switch (argv[1][1]) {

        case 'i':
          ++argv ;
          --argc ;
          instance = atoi(argv[1]);
          break;

        case 'f':
          ++argv ;
          --argc ;
          conf_file = argv[1];
          printf ( "Config file to read = %s\n", conf_file );
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

//---------------- Look up our configuration options ------------------------

    char hostname[300];                                 // Long enough to fit a 255 host name.  Probably it will only be short, but -hey- it's just a few bytes

    if ( gethostname( hostname, sizeof hostname ) == -1 ) strcpy( hostname, "unknown" );	// Get the hostname or default to unknown if we fail
    printf("\nRunning udp2sub on %s.  ver " THISVER " Build %d\n", hostname, prog_build);

    read_config( conf_file, hostname, instance, &conf );		// Use the config file, the host name and the instance number (if there is one) to look up all our configuration and settings

    if ( conf.udp2sub_id == 0 ) {					// If the lookup returned an id of 0, we don't have enough information to continue
      printf("Hostname not found in configuration\n");
      exit(EXIT_FAILURE);						// Abort!
    }

//---------------- We have our config details now. Create/open a monitoring file in shared memory to tell M&C how we're doing ------------------------

//WIP!

//---------------- Trap signals from outside the program ------------------------

    signal(SIGINT, sigint_handler);                     // Tell the OS that we want to trap SIGINT calls
    signal(SIGUSR1, sigusr1_handler);                   // Tell the OS that we want to trap SIGUSR1 calls

//---------------- Allocate the RAM we need for the incoming udp buffer and initialise it ------------------------

    UDP_num_slots = conf.UDP_num_slots;					// We moved this to a config variable, but most of the code still assumes the old name so make both names valid

    msgvecs = calloc( 2 * UDP_num_slots, sizeof(struct mmsghdr) );      // NB Make twice as big an array as the number of actual UDP packets we are going to buffer
    if ( msgvecs == NULL ) {						// If that failed
      printf("msgvecs calloc failed\n");				// log a message
      exit(EXIT_FAILURE);						// and give up!
    }

    iovecs = calloc( UDP_num_slots, sizeof(struct iovec) );             // NB Make the *same* number of entries as the number of actual UDP packets we are going to buffer
    if ( iovecs == NULL ) {						// If that failed
      printf("iovecs calloc failed\n");					// log a message
      exit(EXIT_FAILURE);						// and give up!
    }

    UDPbuf = calloc( UDP_num_slots, sizeof( mwa_udp_packet_t ) );       // NB Make the *same* number of entries as the number of actual UDP packets we are going to buffer
    if ( UDPbuf == NULL ) {						// If that failed
      printf("UDPbuf calloc failed\n");					// log a message
      exit(EXIT_FAILURE);						// and give up!
    }

//---------- Now initialize the arrays

    for (int loop = 0; loop < UDP_num_slots; loop++) {

        iovecs[ loop ].iov_base = &UDPbuf[loop];
        iovecs[ loop ].iov_len = sizeof( mwa_udp_packet_t );

        msgvecs[ loop ].msg_hdr.msg_iov = &iovecs[loop];		// Populate the first copy
        msgvecs[ loop ].msg_hdr.msg_iovlen = 1;

        msgvecs[ loop + UDP_num_slots ].msg_hdr.msg_iov = &iovecs[loop];	// Now populate a second copy of msgvecs that point back to the first set
        msgvecs[ loop + UDP_num_slots ].msg_hdr.msg_iovlen = 1;			// That way we don't need to worry (as much) about rolling over buffers during reads
    }

//---------------- Allocate the RAM we need for the subobs pointers and metadata and initialise it ------------------------

    sub = calloc( 4, sizeof( subobs_udp_meta_t ) );       		// Make 4 slots to store the metadata against the maximum 4 subobs that can be open at one time
    if ( sub == NULL ) {						// If that failed
      printf("sub calloc failed\n");					// log a message
      exit(EXIT_FAILURE);						// and give up!
    }

    blank_sub_line = calloc( SUB_LINE_SIZE, sizeof(char));		// Make ourselves a buffer of zeros that's the size of an empty line.  We'll use this for padding out sub file
    if ( blank_sub_line == NULL ) {					// If that failed
      printf("blank_sub_line calloc failed\n");				// log a message
      exit(EXIT_FAILURE);						// and give up!
    }

    sub_header = calloc( SUBFILE_HEADER_SIZE, sizeof(char));		// Make ourselves a buffer that's initially of zeros that's the size of a sub file header.
    if ( sub_header == NULL ) {						// If that failed
      printf("sub_header calloc failed\n");				// log a message
      exit(EXIT_FAILURE);						// and give up!
    }

//---------------- We're ready to fire up the three worker threads now ------------------------

    printf("Firing up pthreads\n");

    pthread_t UDP_recv_pt;
    pthread_create(&UDP_recv_pt,NULL,UDP_recv,NULL);			// Fire up the process to receive the udp packets into the large buffers we just created

    pthread_t UDP_parse_pt;
    pthread_create(&UDP_parse_pt,NULL,UDP_parse,NULL);			// Fire up the process to parse the udp packets and generate arrays of sorted pointers

    pthread_t makesub_pt;
    pthread_create(&makesub_pt,NULL,makesub,NULL);			// Fire up the process to generate sub files from raw packets and pointers

//---------------- The threads are off and running.  Now we just wait for a message to terminate, like a signal or a fatal error ------------------------

    printf("Master thread switching to metafits reading.\n");

    add_meta_fits();

//    while(!terminate) sleep(1);                 // The master thread currently does nothing! Zip! Nada!  What a waste eh?

//---------------- Clean up the mess we made and go home ------------------------

    printf("Master thread waiting for child threads to end.\n");

    pthread_join(UDP_recv_pt,NULL);
    printf("UDP_recv joined.\n");

    pthread_join(UDP_parse_pt,NULL);
    printf("UDP_parse joined.\n");

    pthread_join(makesub_pt,NULL);
    printf("makesub joined.\n");

//---------- The other threads are all closed down now. Perfect opportunity to have a look at the memory status (while nobody is changing it in the background ----------

    subobs_udp_meta_t *subm;						// pointer to the sub metadata array I'm looking at

    for ( int loop = 0 ; loop < 4 ; loop++ ) {				// Look through all four subobs meta arrays
      subm = &sub[ loop ];						// Temporary pointer to our sub's metadata array

      printf( "slot=%d,so=%d,st=%d,wait=%d,took=%d,first=%lld,last=%lld,startw=%lld,endw=%lld,count=%d,seen=%d\n",
            loop, subm->subobs, subm->state, subm->msec_wait, subm->msec_took, subm->first_udp, subm->last_udp, subm->udp_at_start_write, subm->udp_at_end_write, subm->udp_count, subm->rf_seen );
    }

    uint32_t subobs_mask = 0xFFFFFFF8;					// Mask to apply with '&' to get sub obs from GPS second
    INT64 UDP_closelog = UDP_removed_from_buff - 20;               	// Go back 20 udp packets
    mwa_udp_packet_t *my_udp;						// Make a local pointer to the UDP packet we're working on.

    if ( UDP_closelog < 0 ) UDP_closelog = 0;				// In case we haven't really started yet

    while ( UDP_closelog <= UDP_removed_from_buff ) {
      my_udp = &UDPbuf[ UDP_closelog % UDP_num_slots ];
      printf( "num=%lld,rf=%d,gps=%d,subsec=%d,subobs=%d,slot=%d\n", UDP_closelog, my_udp->rf_input, my_udp->GPS_time, my_udp->subsec_time, (my_udp->GPS_time & subobs_mask ), (( my_udp->GPS_time >> 3 ) & 0b11) );
      UDP_closelog++;
    }

//WIP Print out the next 20 udp buffers starting from UDP_removed_from_buffer (assuming they exist).
// Before printing them do the ntoh conversion for all relevant fields
// This is cause UDP_removed_from_buffer itself to be printed twice, once with and once without conversion

//---------- Free up everything from the heap ----------

    free( msgvecs );							// The threads are dead, so nobody needs this memory now. Let it be free!
    free( iovecs );							// Give back to OS (heap)
    free( UDPbuf );							// This is the big one.  Probably many GB!

    free( sub );							// Free the metadata array storage area

    printf("Exiting process\n");

    exit(EXIT_SUCCESS);
}

// End of code.  Comments, snip and examples only follow
/*
HDR_SIZE 4096
POPULATED 1
OBS_ID 1229977336		*
SUBOBS_ID 1229977360		* ****
MODE HW_LFILES			*
UTC_START 2018-12-27-20:21:58	* ****
OBS_OFFSET 24			* ****
NBIT 8
NPOL 2
NTIMESAMPLES 64000		*
NINPUTS 512			* must be 256 or 512
NINPUTS_XGPU 512		* must be multiple of 16
APPLY_PATH_WEIGHTS 0
APPLY_PATH_DELAYS 0
INT_TIME_MSEC 1000		*
FSCRUNCH_FACTOR 50		*
APPLY_VIS_WEIGHTS 0
TRANSFER_SIZE 10551296000	*
PROJ_ID C001			*
EXPOSURE_SECS 120		*
COARSE_CHANNEL 105		*
CORR_COARSE_CHANNEL 2		*
SECS_PER_SUBOBS 8
UNIXTIME 1545942118		*
UNIXTIME_MSEC 0
FINE_CHAN_WIDTH_HZ 10000	*
NFINE_CHAN 128			*
BANDWIDTH_HZ 1280000		*
SAMPLE_RATE 1280000		*
MC_IP 0.0.0.0
MC_PORT 0
MC_SRC_IP 0.0.0.0
*/
/*
Header listing for HDU #1:
GPSTIME =           1279520688 / [s] GPS time of observation start
EXPOSURE=                  600 / [s] duration of observation
PROJECT = 'G0060   '           / Project ID
MODE    = 'HW_LFILES'          / Observation mode
CHANNELS= '57,58,59,60,61,62,63,64,65,66,67,68,121,122,123,124,125,126,127,128&'
CONTINUE  ',129,130,131,132&'
CONTINUE  '' / Coarse channels
CHANSEL = '0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23' / Indi
FINECHAN=                   10 / [kHz] Fine channel width - correlator freq_res
INTTIME =                  0.5 / [s] Individual integration time
NINPUTS =                  256 / Number of inputs into the correlation products
UNIXTIME=           1595485470 / [s] Unix timestamp of observation start
*//*
About to read: /vulcan/metafits/1283585640_metafits.fitsHeader listing for HDU #1:
SIMPLE  =                    T / conforms to FITS standard
BITPIX  =                    8 / array data type
NAXIS   =                    0 / number of array dimensions
EXTEND  =                    T
GPSTIME =           1283585640 / [s] GPS time of observation start
EXPOSURE=                  600 / [s] duration of observation
FILENAME= 'morgan2020A_ips_NE_AzEl329.0,46.0_Ch[57,58,...,131,132]' / Name of ob
MJD     =    59100.31506944444 / [days] MJD of observation
DATE-OBS= '2020-09-08T07:33:42' / [UT] Date and time of observation
LST     =     219.170438974719 / [deg] LST
HA      = '-01:25:13.88'       / [hours] hour angle of pointing center
AZIMUTH =              329.036 / [deg] Azimuth of pointing center
ALTITUDE=              46.2671 / [deg] Altitude of pointing center
RA      =    196.3551293452646 / [deg] RA of pointing center
DEC     =    11.92982936865595 / [deg] Dec of pointing center
RAPHASE =     197.458305961609 / [deg] RA of desired phase center
DECPHASE=     12.1532286309174 / [deg] DEC of desired phase center
ATTEN_DB=                  1.0 / [dB] global analogue attenuation, in dB
SUN-DIST=     30.9244739683617 / [deg] Distance from pointing center to Sun
MOONDIST=     136.719104001395 / [deg] Distance from pointing center to Moon
JUP-DIST=    95.53026084751529 / [deg] Distance from pointing center to Jupiter
GRIDNAME= 'sweet   '           / Pointing grid name
GRIDNUM =                  108 / Pointing grid number
CREATOR = 'jmorgan '           / Observation creator
PROJECT = 'G0060   '           / Project ID
MODE    = 'HW_LFILES'          / Observation mode
RECVRS  = '1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16' / Active receivers
DELAYS  = '32,32,32,32,32,32,32,32,32,32,32,32,32,32,32,32' / Beamformer delays
CALIBRAT=                    F / Intended for calibration
CENTCHAN=                  121 / Center coarse channel
CHANNELS= '57,58,59,60,61,62,63,64,65,66,67,68,121,122,123,124,125,126,127,128&'
CONTINUE  ',129,130,131,132&'
CONTINUE  '' / Coarse channels
CHANSEL = '0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23' / Indi
SUN-ALT =     30.3414596672889 / [deg] Altitude of Sun
FINECHAN=                   10 / [kHz] Fine channel width - correlator freq_res
INTTIME =                  0.5 / [s] Individual integration time
NAV_FREQ=                    1 / Assumed frequency averaging
NSCANS  =                 1200 / Number of time instants in correlation products
NINPUTS =                  256 / Number of inputs into the correlation products
NCHANS  =                 3072 / Number of (averaged) fine channels in spectrum
BANDWDTH=                30.72 / [MHz] Total bandwidth
FREQCENT=               154.24 / [MHz] Center frequency of observation
TIMEOFF =                    0 / [s] Deprecated, use QUACKTIM or GOODTIME
DATESTRT= '2020-09-08T07:33:42' / [UT] Date and time of correlations start
VERSION =                  2.0 / METAFITS version number
TELESCOP= 'MWA     '
INSTRUME= '128T    '
QUACKTIM=                  3.0 / Seconds of bad data after observation starts
GOODTIME=         1599550425.0 / OBSID+QUACKTIME as Unix timestamp
DATE    = '2020-09-08T07:33:46' / UT Date of file creation
UNIXTIME=           1599550422 / [s] Unix timestamp of observation start
COMMENT After an observation starts, the receiver hardware changes take a
COMMENT few seconds to stabilise. The 24 GPU boxes start saving data to
COMMENT their output files anywhere from a second _before_ the start of
COMMENT the observation to a few seconds after, and not necessarily at the
COMMENT same time on each gpubox.
COMMENT QUACKTIM and GOODTIME represent the start of the first uncontaminated
COMMENT data, rounded up to the next time-averaged data packet. Note that this
COMMENT time may be before the first actual data in some or all gpubox files.
HISTORY Created by user mwa
HISTORY Created on host vulcan.mwa128t.org
HISTORY Command: "./config_daemon.py"
END
*/