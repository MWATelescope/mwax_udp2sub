//===================================================================================================================================================
// Capture UDP packets into a temporary, user space buffer and then sort/copy/process them into some output array
//
// Author(s)  BWC Brian Crosse brian.crosse@curtin.edu.au
// Commenced 2017-05-25
//
#define BUILD 82
#define THISVER "2.04f"
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

#define LIGHTSPEED (299792458000.0L)

#define SUBFILE_HEADER_SIZE (4096LL)
#define SAMPLES_PER_SEC (1280000LL)
#define COARSECHAN_BANDWIDTH (1280000LL)
#define ULTRAFINE_BW (200ll)
#define BLOCKS_PER_SUB (160LL)
#define FFT_PER_BLOCK (10LL)

#define POINTINGS_PER_SUB (BLOCKS_PER_SUB * FFT_PER_BLOCK)
// NB: POINTINGS_PER_SUB gives 1600 so that every 5ms we write a new delay to the sub file.

#define SUB_LINE_SIZE ((SAMPLES_PER_SEC * 8LL * 2LL) / BLOCKS_PER_SUB)
// They might be 6400 samples each for critically sampled data or 8192 for 32/25 oversampled data. Ether way, the 5ms is constant while Ultrafine_bw is 200Hz

#define NTIMESAMPLES ((SAMPLES_PER_SEC * 8LL) / BLOCKS_PER_SUB)

#define UDP_PER_RF_PER_SUB ( ((SAMPLES_PER_SEC * 8LL * 2LL) / UDP_PAYLOAD_SIZE) + 2 )
#define SUBSECSPERSEC ( (SAMPLES_PER_SEC * 2LL)/ UDP_PAYLOAD_SIZE )
#define SUBSECSPERSUB ( SUBSECSPERSEC * 8LL )

//#define RECVMMSG_MODE ( MSG_DONTWAIT )
#define RECVMMSG_MODE ( MSG_WAITFORONE )

#define HOSTNAME_LENGTH 21

#define MONITOR_IP "224.0.2.2"
#define MONITOR_PORT 8007
#define MONITOR_TTL 3

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

typedef struct udp2sub_monitor {               // Structure for the placing monitorable statistics in shared memory so they can be read by external applications
    uint8_t udp2sub_id;                        // Which instance number we are.  Probably from 01 to 26, at least initially.
    char hostname[HOSTNAME_LENGTH];            // Host name is looked up to against these strings
    int coarse_chan;                           // Which coarse chan from 01 to 24.
    uint64_t sub_write_sleeps;                 // record how often we went to sleep while looking for a sub file to write out (should grow quickly)
    uint32_t current_subobs;                   // Subobservation in progress
    int subobs_state;                          // Subobservation state
    int udp_count;                             // Number of UDP packets collected from the NIC for this subobservation
    int udp_dummy;                             // Number of dummy packets we have needed to insert to pad things out for this subobservation
} udp2sub_monitor_t ;

    // TODO:
    // Last sub file created
    // most advanced udp packet
    // Lowest remaining buffer left
    // Stats on lost packets
    // mon->ignored_for_a_bad_type++;                          // Keep track for debugging of how many packets we've seen that we can't handle
    // mon->ignored_for_being_too_old++;                               // Keep track for debugging of how many packets were too late to process
    // mon->sub_meta_sleeps++;                                         // record we went to sleep
    // mon->sub_write_sleeps++;                                        // record we went to sleep
    // mon->sub_write_dummy++;                                         // record we needed to use the dummy packet to fill in for a missing udp packet

//        printf("now=%lld,so=%d,ob=%lld,%s,st=%d,free=%d:%d,wait=%d,took=%d,used=%lld,count=%d,dummy=%d,rf_inps=w%d:s%d:c%d\n",
//            (INT64)(ended_sub_write_time.tv_sec - GPS_offset), subm->subobs, subm->GPSTIME, subm->MODE, subm->state, free_files, bad_free_files, subm->msec_wait, subm->msec_took, subm->udp_at_end_write - subm->first_udp, subm->udp_count, subm->udp_dummy, subm->NINPUTS, subm->rf_seen, active_rf_inputs );


#pragma pack(pop)                               // Set the structure packing back to 'normal' whatever that is

//---------------- internal structure definitions --------------------

typedef struct altaz_meta {				// Structure format for the metadata associated with the pointing at the beginning, middle and end of the sub-observation

    INT64 gpstime;
    float Alt;
    float Az;
    float Dist_km;

    long double SinAzCosAlt;				// will be multiplied by tile East
    long double CosAzCosAlt;				// will be multiplied by tile North
    long double SinAlt;					// will be multiplied by tile Height

} altaz_meta_t;

typedef struct tile_meta {                              // Structure format for the metadata associated with one rf input for one subobs

    int Input;
    int Antenna;
    int Tile;
    char TileName[9];                                   // Leave room for a null on the end!
    char Pol[2];                                        // Leave room for a null on the end!
    int Rx;
    int Slot;
    int Flag;
//    char Length[15];
    long double Length_f;				// Floating point format version of the weird ASCII 'EL_###' length string above.
    long double North;
    long double East;
    long double Height;
    int Gains[24];
    float BFTemps;
    int Delays[16];
//    int VCSOrder;
    char Flavors[11];
    float Calib_Delay;
    float Calib_Gains[24];

    uint16_t rf_input;                                  // What's the tile & pol identifier we'll see in the udp packets for this input?

    int16_t ws_delay;					// The whole sample delay IN SAMPLES (NOT BYTES). Can be -ve. Each extra delay will move the starting position later in the sample sequence
    int32_t initial_delay;
    int32_t delta_delay;
    int32_t delta_delta_delay;

} tile_meta_t;

typedef struct block_0_tile_metadata {			// Structure format for the metadata line for each tile stored in the first block of the sub file.

    uint16_t rf_input;                                  // What's the tile & pol identifier we'll see in the udp packets for this input?

    int16_t ws_delay;					// The whole sample delay for this tile, for this subobservation?  Will often be negative!
    int32_t initial_delay;
    int32_t delta_delay;
    int32_t delta_delta_delay;

    int16_t num_pointings;				// Initially 1, but might grow to 10 or more if beamforming.  The first pointing is for delay tracking in the correlator.
    int16_t frac_delay[POINTINGS_PER_SUB];		// 1601 Fractional delays in units of 1000th of a whole sample time.  Not supposed to be out of range of -2000 to 2000 millisamples (inclusive).

} block_0_tile_metadata_t;

typedef struct subobs_udp_meta {                        // Structure format for the MWA subobservation metadata that tracks the sorted location of the udp packets

    volatile uint32_t subobs;                           // The sub observation number.  ie the GPS time of the first second in this sub-observation
    volatile int state;                                 // 0==Empty, 1==Adding udp packets, 2==closed off ready for sub write, 3==sub write in progress, 4==sub write complete and ready for reuse, >=5 is failed

    int msec_wait;                                      // The number of milliseconds the sub write thread had waited doing nothing before starting to write out this sub.  Only valid for state = 3 or 4 or 5 or higher
    int msec_took;                                      // The number of milliseconds the sub write thread took to write out this sub.  Only valid for state = 4 or 5 or higher

    INT64 first_udp;
    INT64 last_udp;
    INT64 udp_at_start_write;
    INT64 udp_at_end_write;

    int udp_count;                                      // The number of udp packets collected from the NIC
    int udp_dummy;                                      // The number of dummy packets we needed to insert to pad things out

    volatile int meta_done;                             // Has metafits data been appended to this sub observation yet?  0 == No.
    int meta_msec_wait;                                 // The number of milliseconds it took to from reading the last metafits to finding a new one
    int meta_msec_took;                                 // The number of milliseconds it took to read the metafits

    INT64 GPSTIME;                                      // Following fields are straight from the metafits file (via the metabin) This is the GPSTIME *FOR THE OBS* not the subobs!!!
    int EXPOSURE;
    char FILENAME[300];
    int CABLEDEL;
    int GEODEL;
    int CALIBDEL;
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

    uint16_t rf_seen;                                   // The number of different rf_input sources seen so far this sub observation
    uint16_t rf2ndx[65536];                             // A mapping from the rf_input value to what row in the pointer array its pointers are stored
    char *udp_volts[MAX_INPUTS+1][UDP_PER_RF_PER_SUB];    // array of pointers to every udp packet's payload that may be needed for this sub-observation.  NB: THIS ARRAY IS IN THE ORDER INPUTS WERE SEEN STARTING AT 1!, NOT THE SUB FILE ORDER!

    tile_meta_t rf_inp[MAX_INPUTS];                     // Metadata about each rf input in an array indexed by the order the input needs to be in the output sub file, NOT the order udp packets were seen in.

    altaz_meta_t altaz[3];				// The AltAz at the beginning, middle and end of the 8 second sub-observation

} subobs_udp_meta_t;

typedef struct MandC_meta {                             // Structure format for the MWA subobservation metadata that tracks the sorted location of the udp packets.  Array with one entry per rf_input

    uint16_t rf_input;                                  // tile/antenna and polarisation (LSB is 0 for X, 1 for Y)
    int start_byte;                                     // What byte (not sample) to start at.  Remember we store a whole spare packet before the real data starts
    uint16_t seen_order;                                // For this subobs (only). What order was this rf input seen in?  That tells us where we need to look in the udp_volts pointer array

} MandC_meta_t;

//---------------------------------------------------------------------------------------------------------------------------------------------------
// These variables are shared by all threads.  "file scope"
//---------------------------------------------------------------------------------------------------------------------------------------------------

#define CLOSEDOWN (0xFFFFFFFF)

volatile BOOL terminate = FALSE;                        // Global request for everyone to close down

volatile INT64 UDP_added_to_buff = 0;                   // Total number of packets received and placed in the buffer.  If it overflows we are in trouble and need a restart.
volatile INT64 UDP_removed_from_buff = 0;               // Total number of packets pulled from buffer and space freed up.  If it overflows we are in trouble and need a restart.

INT64 UDP_num_slots;                                    // Must be at least 3000000 for mwax07 with 128T. 3000000 will barely make it!
UINT32 GPS_offset = 315964782;                          // Logging only.  Needs to be updated on leap seconds

struct mmsghdr *msgvecs;
struct iovec *iovecs;
mwa_udp_packet_t *UDPbuf;
udp2sub_monitor_t monitor;

subobs_udp_meta_t *sub;                                 // Pointer to the four subobs metadata arrays
char *blank_sub_line;                                   // Pointer to a buffer of zeros that's the size of an empty line.  We'll allocate it later and use it for padding out sub files
char *sub_header;                                       // Pointer to a buffer that's the size of a sub file header.

BOOL debug_mode = FALSE;                                // Default to not being in debug mode

//---------------------------------------------------------------------------------------------------------------------------------------------------
// read_config - use our hostname and a command line parameter to find ourselves in the list of possible configurations
// Populate a single structure with the relevant information and return it
//---------------------------------------------------------------------------------------------------------------------------------------------------

//#pragma pack(push,1)                          // We're writing this header into all our packets, so we want/need to force the compiler not to add it's own idea of structure padding

typedef struct udp2sub_config {               // Structure for the configuration of each udp2sub instance.

    int udp2sub_id;                           // Which correlator id number we are.  Probably from 01 to 26, at least initially.
    char hostname[64];                        // Host name is looked up against these strings to select the correct line of configuration settings
    int host_instance;                        // Is compared with a value that can be put on the command line for multiple copies per server

    INT64 UDP_num_slots;                      // The number of UDP buffers to assign.  Must be ~>=3000000 for 128T array

    unsigned int cpu_mask_parent;             // Allowed cpus for the parent thread which reads metafits file
    unsigned int cpu_mask_UDP_recv;           // Allowed cpus for the thread that needs to pull data out of the NIC super fast
    unsigned int cpu_mask_UDP_parse;          // Allowed cpus for the thread that checks udp packets to create backwards pointer lists
    unsigned int cpu_mask_makesub;           // Allowed cpus for the thread that writes the sub files out to memory

    char shared_mem_dir[40];                  // The name of the shared memory directory where we get .free files from and write out .sub files
    char temp_file_name[40];                  // The name of the temporary file we use.  If multiple copies are running on each server, these may need to be different
    char stats_file_name[40];                 // The name of the shared memory file we use to write statistics and debugging to.  Again if multiple copies per server, it may need different names
    char spare_str[40];                       //
    char metafits_dir[60];                    // The directory where metafits files are looked for.  NB Likely to be on an NFS mount such as /vulcan/metafits

    char local_if[20];                        // Address of the local NIC interface we want to receive the multicast stream on.
    int coarse_chan;                          // Which coarse chan from 01 to 24.
    char multicast_ip[30];                    // The multicast address and port (below) we wish to join.
    int UDPport;                              // Multicast port address

} udp2sub_config_t ;

//#pragma pack(pop)                               // Set the structure packing back to 'normal' whatever that is

udp2sub_config_t conf;                          // A place to store the configuration data for this instance of the program.  ie of the 60 copies running on 10 computers or whatever


//---------------------------------------------------------------------------------------------------------------------------------------------------
// load_channel_map - Load config from a CSV file.
//---------------------------------------------------------------------------------------------------------------------------------------------------

int load_config_file(char *path, udp2sub_config_t **config_records) {
  fprintf(stderr, "Reading configuration from %s\n", path);
  // Read the whole input file into a buffer.
  char *data, *datap;
  FILE *file;
  size_t sz;
  file = fopen(path, "r");
    if(file == NULL) {
    fprintf(stderr, "Failed to open %s", path);
    return 1;
  }
  fseek(file, 0, SEEK_END);
  sz = ftell(file);
  rewind(file);
  if(sz == -1) {
    fprintf(stderr, "Failed to determine file size: %s", path);
    return 2;
  }
  data = datap = malloc(sz + 1);
  data[sz] = '\n'; // Simplifies parsing slightly
  if(fread(data, 1, sz, file) != sz) {
    fprintf(stderr, "Failed reading %s - unexpected data length.", path);
    return 3;
  }
  fclose(file);

  udp2sub_config_t *records;
  int max_rows = sz / 26;                  // Minimum row size is 26, pre-allocate enough
  int row_sz = sizeof(udp2sub_config_t);   // room for the worst case and `realloc` later
  records = calloc(max_rows, row_sz);      // when the size is known.

  int row = 0, col = 0;              // Current row and column in input table
  char *sep = ",";                   // Next expected separator
  char *end = NULL;                  // Mark where `strtol` stops parsing
  char *tok = strsep(&datap, sep);   // Pointer to start of next value in input
  while (row < max_rows) {
    while (col < 17) {
      switch (col) {
      case 0:
        records[row].udp2sub_id = strtol(tok, &end, 10); // Parse the current token as a number,
        if (end == NULL || *end != '\0') goto done;           // consuming the whole token, or abort.
        break;
      case 1:
        strcpy(records[row].hostname, tok);
        break;
      case 2:
        records[row].host_instance = strtol(tok, &end, 10);
        if (end == NULL || *end != '\0') goto done;
        break;
      case 3:
        records[row].UDP_num_slots = strtol(tok, &end, 10);
        if (end == NULL || *end != '\0') goto done;
        break;
      case 4:
        records[row].cpu_mask_parent = strtol(tok, &end, 10);
        if (end == NULL || *end != '\0') goto done;
        break;
      case 5:
        records[row].cpu_mask_UDP_recv = strtol(tok, &end, 10);
        if (end == NULL || *end != '\0') goto done;
        break;
      case 6:
        records[row].cpu_mask_UDP_parse = strtol(tok, &end, 10);
        if (end == NULL || *end != '\0') goto done;
        break;
      case 7:
        records[row].cpu_mask_makesub = strtol(tok, &end, 10);
        if (end == NULL || *end != '\0') goto done;
        break;
      case 8:
        strcpy(records[row].shared_mem_dir, tok);
        break;
      case 9:
        strcpy(records[row].temp_file_name, tok);
        break;
      case 10:
        strcpy(records[row].stats_file_name, tok);
        break;
      case 11:
        strcpy(records[row].spare_str, tok);
        break;
      case 12:
        strcpy(records[row].metafits_dir, tok);
        break;
      case 13:
        strcpy(records[row].local_if, tok);
        break;
      case 14:
        records[row].coarse_chan = strtol(tok, &end, 10);
        if (end == NULL || *end != '\0') goto done;
        break;
      case 15:
        strcpy(records[row].multicast_ip, tok);
        break;
      case 16:
        records[row].UDPport = strtol(tok, &end, 10);
        if (end == NULL || *end != '\0') goto done;
        break;
      }

      if (col == 15)      // If we've parsed the second-to-last column,
        sep = "\n";       // the next separator to expect will be LF.
      else if (col == 16) // If we've parsed the last column, the next
        sep = ",";        // separator will be a comma again.
      col++;

      tok = strsep(&datap, sep); // Get the next token from the input and
      if (tok == NULL) goto done; // abort the row if we don't find one.
    }
    if (col == 17) col = 0; // Wrap to column 0 if we parsed a full row
    else break;             // or abort if we didn't.
    row++;
  }
done:
  *config_records = reallocarray(records, row + 1, sizeof(udp2sub_config_t));
  free(data);
  fprintf(stderr, "%d instance record(s) found.\n", row + 1);
  return row + 1;
}


void read_config ( char *file, char *us, int inst, int coarse_chan, udp2sub_config_t *config ) {

    int num_instances = 0;                                      // Number of instance records loaded
    int instance_ndx = 0;                                       // Start out assuming we don't appear in the list

    udp2sub_config_t *available_config = NULL;
    num_instances = load_config_file(file, &available_config);

    if(available_config == NULL) {
      fprintf(stderr, "Failed to load instance configuration data.");
      exit(1);
    }

    for (int loop = 0 ; loop < num_instances; loop++ ) {        // Check through all possible configurations
      if ( ( strcmp( available_config[loop].hostname, us ) == 0 ) && ( available_config[loop].host_instance == inst ) ) {
        instance_ndx = loop;                                    // if the edt card config matches the command line and the hostname matches
        break;                                                  // We don't need to keep looking
      }
    }

    *config = available_config[ instance_ndx ];                 // Copy the relevant line into the structure we were passed a pointer to

    if ( coarse_chan > 0 ) {                                    // If there is a coarse channel override on the command line
      config->coarse_chan = coarse_chan;                        // Force the coarse chan from 01 to 24 with that value
      sprintf( config->multicast_ip, "239.255.90.%d", coarse_chan );            // Multicast ip is 239.255.90.xx
      config->UDPport = 59000 + coarse_chan;                    // Multicast port address is the forced coarse channel number plus an offset of 59000
    }

    monitor.coarse_chan = coarse_chan;
    memcpy(monitor.hostname, config->hostname, HOSTNAME_LENGTH);
    monitor.udp2sub_id = config->udp2sub_id;
}

//===================================================================================================================================================


// ------------------------ Function to set CPU affinity so that we can control which socket & core we run on -------------------------

int set_cpu_affinity ( unsigned int mask )
{

    cpu_set_t my_cpu_set;                       // Make a CPU affinity set for this thread
    CPU_ZERO( &my_cpu_set );                    // Zero it out completely

    for ( int loop = 0 ; loop < 32 ; loop++ ) {                 // Support up to 32 cores for now.  We need to step through them from LSB to MSB

      if ( ( ( mask >> loop ) & 0x01 ) == 0x01 ) {              // If that bit is set
        CPU_SET( loop, &my_cpu_set );                           // then add it to the allowed list
      }
    }

    pthread_t my_thread;                        // I need to look up my own TID / LWP
    my_thread = pthread_self();                 // So what's my TID / LWP?

    if (conf.udp2sub_id > 26 ) {                                                         // Don't do any mwax servers for now
      return ( pthread_setaffinity_np( my_thread, sizeof(cpu_set_t), &my_cpu_set ));    // Make the call and return the result back to the caller
    } else {
      return ( -1 );
    }
}

//===================================================================================================================================================
// THREAD BLOCK.  The following functions are complete threads
//===================================================================================================================================================

//---------------------------------------------------------------------------------------------------------------------------------------------------
// UDP_recv - Pull UDP packets out of the kernel and place them in a large user-space circular buffer
//---------------------------------------------------------------------------------------------------------------------------------------------------

void *UDP_recv()
{
    printf("UDP_recv started\n");
    fflush(stdout);

//--------------- Set CPU affinity ---------------

    printf("Set process UDP_recv cpu affinity returned %d\n", set_cpu_affinity ( conf.cpu_mask_UDP_recv ) );
    fflush(stdout);

//---------------- Initialize and declare variables ------------------------

    INT64 UDP_slots_empty;                                              // How much room is left unused in the application's UDP receive buffer
    INT64 UDP_first_empty;                                              // Index to the first empty slot 0 to (UDP_num_slots-1)

    INT64 UDP_slots_empty_min = UDP_num_slots + 1 ;                     // what's the smallest number of empty slots we've seen this batch?  (Set an initial value that will always be beaten)

    INT64 Num_loops_when_full = 0 ;                                     // How many times (since program start) have we checked if there was room in the buffer and there wasn't any left  :-(

    int retval;                                                         // General return value variable.  Context dependant.

//--------------------------------

    printf ( "Set up to receive from multicast %s:%d on interface %s\n", conf.multicast_ip, conf.UDPport, conf.local_if );
    fflush(stdout);

    struct sockaddr_in addr;                                            // Standard socket setup stuff needed even for multicast UDP
    memset(&addr,0,sizeof(addr));

    int fd;
    struct ip_mreq mreq;

    if ((fd=socket(AF_INET,SOCK_DGRAM,0)) == -1) {                      // create what looks like an ordinary UDP socket
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

    if (bind(fd,(struct sockaddr *) &addr,sizeof(addr)) == -1) {        // bind to the receive address
      perror("bind");
      close(fd);
      terminate = TRUE;
      pthread_exit(NULL);
    }

    mreq.imr_multiaddr.s_addr=inet_addr( conf.multicast_ip );                   // use setsockopt() to request that the kernel join a multicast group
    mreq.imr_interface.s_addr=inet_addr( conf.local_if );

    if (setsockopt(fd,IPPROTO_IP,IP_ADD_MEMBERSHIP,&mreq,sizeof(mreq)) == -1) {
      perror("setsockopt IP_ADD_MEMBERSHIP");
      close(fd);
      terminate = TRUE;
      pthread_exit(NULL);
    }

//--------------------------------------------------------------------------------------------------------------------------------------------------------

    printf( "Ready to start\n" );
    fflush(stdout);

    UDP_slots_empty = UDP_num_slots;                                    // We haven't written to any yet, so all slots are available at the moment
    UDP_first_empty = 0;                                                // The first of those is number zero
    struct mmsghdr *UDP_first_empty_ptr = &msgvecs[UDP_first_empty];    // Set our first empty pointer to the address of the 0th element in the array

    while (!terminate) {

      if ( UDP_slots_empty > 0 ) {                                              // There's room for at least 1 UDP packet to arrive!  We should go ask the OS for it.

//if ( UDP_slots_empty > 16LL ) UDP_slots_empty = 16LL;

        if ( ( retval = recvmmsg( fd, UDP_first_empty_ptr, UDP_slots_empty, RECVMMSG_MODE, NULL ) ) == -1 )                              //
          if ( ( retval = recvmmsg( fd, UDP_first_empty_ptr, UDP_slots_empty, RECVMMSG_MODE, NULL ) ) == -1 )                            //
            if ( ( retval = recvmmsg( fd, UDP_first_empty_ptr, UDP_slots_empty, RECVMMSG_MODE, NULL ) ) == -1 )                          //
              if ( ( retval = recvmmsg( fd, UDP_first_empty_ptr, UDP_slots_empty, RECVMMSG_MODE, NULL ) ) == -1 )                        //
                if ( ( retval = recvmmsg( fd, UDP_first_empty_ptr , UDP_slots_empty, RECVMMSG_MODE, NULL ) ) == -1 ) continue;

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
    fflush(stdout);

    mreq.imr_multiaddr.s_addr=inet_addr( conf.multicast_ip );
    mreq.imr_interface.s_addr=inet_addr( conf.local_if );

    if (setsockopt(fd,IPPROTO_IP,IP_DROP_MEMBERSHIP,&mreq,sizeof(mreq)) == -1) {  // Say we don't want multicast packets any more.
      perror("setsockopt IP_DROP_MEMBERSHIP");
      close(fd);
      pthread_exit(NULL);
    }

    close(fd);                                                                  // Close the file descriptor for the port now we're about the leave

    printf( "Exiting UDP_recv\n");
    fflush(stdout);
    pthread_exit(NULL);
}

//------------------------------------------------------------------------------------------------------------------------------------------------------
// UDP_parse - Check UDP packets as they arrive and update the array of pointers to them so we can later address them in sorted order
//------------------------------------------------------------------------------------------------------------------------------------------------------

void *UDP_parse()
{
    printf("UDP_parse started\n");
    fflush(stdout);

//--------------- Set CPU affinity ---------------

    printf("Set process UDP_parse cpu affinity returned %d\n", set_cpu_affinity ( conf.cpu_mask_UDP_parse ) );
    fflush(stdout);

//---------------- Initialize and declare variables ------------------------

    mwa_udp_packet_t *my_udp;                                           // Make a local pointer to the UDP packet we're working on.

    uint32_t last_good_packet_sub_time = 0;                             // What sub obs was the last packet (for caching)
    uint32_t subobs_mask = 0xFFFFFFF8;                                  // Mask to apply with '&' to get sub obs from GPS second

    uint32_t start_window = 0;                                          // What's the oldest packet we're accepting right now?
    uint32_t end_window = 0;                                            // What's the highest GPS time we can accept before recalculating our usable window?  Hint: We *need* to recalculate that window

    int slot_ndx;                                                       // index into which of the 4 subobs metadata blocks we want
    int rf_ndx;                                                         // index into the position in the meta array we are using for this rf input (for this sub).  NB May be different for the same rf on a different sub.

    subobs_udp_meta_t *this_sub = NULL;                                 // Pointer to the relevant one of the four subobs metadata arrays

    int loop;

//---------------- Main loop to process incoming udp packets -------------------

    while (!terminate) {



      if ( UDP_removed_from_buff < UDP_added_to_buff ) {                // If there is at least one packet waiting to be processed

        my_udp = &UDPbuf[ UDP_removed_from_buff % UDP_num_slots ];      // then this is a pointer to it.

//---------- At this point in the loop, we are about to process the next arrived UDP packet, and it is located at my_udp ----------

        if ( my_udp->packet_type == 0x20 ) {                            // If it's a real packet off the network with some fields in network order, they need to be converted to host order and have a few tweaks made before we can use the packet

          my_udp->subsec_time = ntohs( my_udp->subsec_time );           // Convert subsec_time to a usable uint16_t which at this stage is still within a single second
          my_udp->GPS_time = ntohl( my_udp->GPS_time );                 // Convert GPS_time (bottom 32 bits only) to a usable uint32_t



          my_udp->rf_input = ntohs( my_udp->rf_input );                 // Convert rf_input to a usable uint16_t
          my_udp->edt2udp_token = ntohs( my_udp->edt2udp_token );       // Convert edt2udp_token to a usable uint16_t

                                                                        // the bottom three bits of the GPS_time is 'which second within the subobservation' this second is
          my_udp->subsec_time += ( (my_udp->GPS_time & 0b111) * SUBSECSPERSEC ) + 1;    // Change the subsec field to be the packet count within a whole subobs, not just this second.  was 0 to 624.  now 1 to 5000 inclusive. (0x21 packets can be 0 to 5001!)

          my_udp->packet_type = 0x21;                                   // Now change the packet type to a 0x21 to say it's similar to a 0x20 but in host byte order

        } else {                                                        // If is wasn't a 0x20 packet, the only other packet we handle is a 0x21

//---------- Not for us?

          if ( my_udp->packet_type != 0x21 ) {                          // This packet is not a valid packet for us.
//WIP            mon->ignored_for_a_bad_type++;                         // Keep track for debugging of how many packets we've seen that we can't handle
            UDP_removed_from_buff++;                                    // Flag it as used and release the buffer slot.  We don't want to see it again.
            continue;                                                   // start the loop again
          }

        }

//---------- If this packet is for the same sub obs as the previous packet, we can assume a bunch of things haven't changed or rolled over, otherwise we have things to check

        if ( ( my_udp->GPS_time & subobs_mask ) != last_good_packet_sub_time ) {                // If this is a different sub obs than the last packet we allowed through to be processed.

//---------- Arrived too late to be usable?

          if (my_udp->GPS_time < start_window) {                        // This packet has a time stamp before the earliest open sub-observation
//WIP            mon->ignored_for_being_too_old++;                              // Keep track for debugging of how many packets were too late to process
            UDP_removed_from_buff++;                                    // Throw it away.  ie flag it as used and release the buffer slot.  We don't want to see it again.
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
              if ( ( sub[ loop ].subobs < start_window ) && ( sub[ loop ].state == 1 ) ) {              // If this sub obs slot is currently in use by us (ie state==1) and has now reached its timeout ( < start_window )
                if ( sub[ loop ].subobs == ( start_window - 8 ) ) {     // then if it's a very recent subobs (which is what we'd expect during normal operations)
                  sub[ loop ].state = 2;                                // set the state flag to tell another thread it's their job to write out this subobs and pass the data on down the line
                } else {                                                // or if it isn't recent then it's probably because the receivers (or medconv array) went away so...
                  sub[ loop ].state = 6;                                // set the state flag to indicate that this subobs should be abandoned as too old to be useful
                }
              }
            }

            if ( my_udp->GPS_time == CLOSEDOWN ) {                      // If we've been asked to close down via a udp packet with the time set to the year 2106
              terminate = TRUE;                                         // tell everyone to shut down.
              UDP_removed_from_buff++;                                  // Flag it as used and release the buffer slot.  We don't want to see it again.
              continue;                                                 // This will take us out to the while !terminate loop that will shut us down.  (Could have used a break too. It would have the same effect)
            }

            // We now have a new subobs that we need to set up for.  Hopefully the slot we want to use is either empty or finished with and free for reuse.  If not we've overrun the sub writing threads

            slot_ndx = ( my_udp->GPS_time >> 3 ) & 0b11;                // The 3 LSB are 'what second in the subobs' we are.  We want the 2 bits immediately to the left of that.  They will give us our index into the subobs metadata array

            if ( sub[ slot_ndx ].state >= 4 ) {                         // If the state is a 4 or 5 or higher, then it's free for reuse, but someone needs to wipe the contents.  I guess that 'someone' means me!
              memset( &sub[ slot_ndx ], 0, sizeof( subobs_udp_meta_t ));        // Among other things, like wiping the array of pointers back to NULL, this memset should set the value of 'state' back to 0.
            }

            if ( sub[ slot_ndx ].state != 0 ) {                         // If state isn't 0 now, then it's bad.  We have a transaction to store and the subobs it belongs to can't be set up because its slot is still in use by an earlier subobs

              printf ( "Error: Subobs slot %d still in use. Currently in state %d.\n", slot_ndx, sub[slot_ndx].state );         // If this actually happens during normal operation, we will need to fix the cause or (maybe) bandaid it with a delay here, but for now
              printf ( "Wanted to put gps=%d, subobs=%d, first=%lld, state=1\n", my_udp->GPS_time, ( my_udp->GPS_time & subobs_mask ), UDP_removed_from_buff );
              printf ( "Found so=%d,st=%d,wait=%d,took=%d,first=%lld,last=%lld,startw=%lld,endw=%lld,count=%d,seen=%d\n",
                sub[slot_ndx].subobs, sub[slot_ndx].state, sub[slot_ndx].msec_wait, sub[slot_ndx].msec_took, sub[slot_ndx].first_udp, sub[slot_ndx].last_udp, sub[slot_ndx].udp_at_start_write, sub[slot_ndx].udp_at_end_write, sub[slot_ndx].udp_count, sub[slot_ndx].rf_seen );

              fflush(stdout);
              terminate = TRUE;                                         // Lets make a fatal error so we know we'll spot it
              continue;                                                 // abort what we're attempting and all go home
            }

            sub[ slot_ndx ].subobs = ( my_udp->GPS_time & subobs_mask );        // The subobs number is the GPS time of the beginning second so mask off the bottom 3 bits
            sub[ slot_ndx ].first_udp = UDP_removed_from_buff;          // This was the first udp packet seen for this sub. (0 based)
            sub[ slot_ndx ].state = 1;                                  // Let's remember we're using this slot now and tell other threads.  NB: The subobs field is assumed to have been populated *before* this becomes 1

          }

//---------- This packet isn't similar enough to previous ones (ie from the same sub-obs) to assume things, so let's get new pointers

          this_sub = &sub[(( my_udp->GPS_time >> 3 ) & 0b11 )];         // The 3 LSB are 'what second in the subobs' we are.  We want the 2 bits left of that.  They will give us an index into the subobs metadata array which we use to get the pointer to the struct
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

        this_sub->last_udp = UDP_removed_from_buff;                     // This was the last udp packet seen so far for this sub. (0 based) Eventually it won't be updated and the final value will remain there
        this_sub->udp_count++;                                          // Add one to the number of udp packets seen this sub obs.  NB Includes duplicates and faked delay packets so can't reliably be used to check if we're finished a sub

//---------- We are done with this packet, EXCEPT if this was the very first for an rf_input for a subobs, or the very last, then we want to duplicate them in the adjacent subobs.
//---------- This is because one packet may contain data that goes in two subobs (or even separate obs!) due to delay tracking

        if ( my_udp->subsec_time == 1 ) {                               // If this was the first packet (for an rf input) for a subobs
          my_udp->subsec_time = SUBSECSPERSUB + 1;                      // then say it's at the end of a subobs
          my_udp->GPS_time--;                                           // and back off the second count to put it in the last subobs
        } else {
          if ( my_udp->subsec_time == SUBSECSPERSUB ) {                 // If this was the last packet (for an rf input) for a subobs
            my_udp->subsec_time = 0;                                    // then say it's at the start of a subobs
            my_udp->GPS_time++;                                         // and increment the second count to put it in the next subobs
          } else {
            UDP_removed_from_buff++;                                    // We don't need to duplicate this packet, so increment the number of packets we've ever processed (which automatically releases them from the buffer).
          }
        }

//---------- End of processing of this UDP packet ----------

      } else {                                                          //
//WIP        mon->sub_meta_sleeps++;                                            // record we went to sleep
        usleep(10000);                                                  // Chill for a bit
      }

    }

//---------- We've been told to shut down ----------

    printf( "Exiting UDP_parse\n" );
    fflush(stdout);
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

    char metafits_file[300];                                            // The metafits file name

    INT64 bcsf_obsid;                                                   // 'best candidate so far' for the metafits file
    INT64 mf_obsid;

    int loop;
    int subobs_ready2write;
    int meta_result;                                                    // Temp storage for the result before we write it back to the array for others to see.  Once we do that, we can't change our mind.
    BOOL go4meta;

    subobs_udp_meta_t *subm;                                            // pointer to the sub metadata array I'm working on

    struct timespec started_meta_write_time;
    struct timespec ended_meta_write_time;

    static long double mm2s_conv_factor = (long double) SAMPLES_PER_SEC / (long double) LIGHTSPEED;      // Frequently used conversion factor of delay in units of millimetres to samples.

    static long double two_over_num_blocks_sqrd = ( ((long double) 2L) / ( (long double) (BLOCKS_PER_SUB*FFT_PER_BLOCK) * (long double) (BLOCKS_PER_SUB*FFT_PER_BLOCK) ) ); // Frequently used during fitting parabola
//    static long double two_over_num_blocks_sqrd = ( ((long double) 2L) / ( (long double) (1600L) * (long double) (1600L) ) ); // Frequently used during fitting parabola
    static long double one_over_num_blocks = ( ((long double) 1L) / (long double) (BLOCKS_PER_SUB*FFT_PER_BLOCK) );      // Frequently used during fitting parabola

    static long double conv2int32 = (long double)( 1 << 20 ) * (long double) 1000L;                     // Frequently used conversion factor to millisamples and making use of extra bits in int32
    static long double res_max = (long double) 2L;                                                      // Frequently used in range checking. Largest allowed residual.
    static long double res_min = (long double) -2L;                                                     // Frequently used in range checking. Smallest allowed residual.

    long double d2r = M_PIl / 180.0L;					// Calculate a long double conversion factor from degrees to radians (we'll use it a few times)
    long double SinAlt, CosAlt, SinAz, CosAz;				// temporary variables for storing the trig results we need to do delay tracking

    long double a,b,c;							// coefficients of the delay fitting parabola

    fitsfile *fptr;                                                                             // FITS file pointer, defined in fitsio.h
//    char card[FLEN_CARD];                                                                     // Standard string lengths defined in fitsio.h
    int status;                                                                                 // CFITSIO status value MUST be initialized to zero!
    int hdupos;
//    int single, nkeys, ii;

    char channels_ascii[200];                                           // Space to read in the metafits channel string before we parse it into individual channels
//    int len;
    unsigned int ch_index;                                              // assorted variables to assist parsing the channel ascii csv into something useful like an array of ints
    char* token;
    char* saveptr;
    char* ptr;
    int temp_CHANNELS[24];
    int course_swap_index;

//---------------- Main loop to live in until shutdown -------------------

    printf("add_meta_fits started\n");
    fflush(stdout);

    clock_gettime( CLOCK_REALTIME, &ended_meta_write_time);             // Fake the ending time for the last metafits file ('cos like there wasn't one y'know but the logging will expect something)

    while (!terminate) {                                                // If we're not supposed to shut down, let's find something to do

//---------- look for a sub which needs metafits info ----------

      subobs_ready2write = -1;                                          // Start by assuming there's nothing to do

      for ( loop = 0 ; loop < 4 ; loop++ ) {                            // Look through all four subobs meta arrays

        if ( ( sub[loop].state == 1 ) && ( sub[loop].meta_done == 0 ) ) {       // If this sub is ready to have M&C metadata added

          if ( subobs_ready2write == -1 ) {                             // check if we've already found a different one to do and if we haven't

            subobs_ready2write = loop;                                  // then mark this one as the best so far

          } else {                                                      // otherwise we need to work out

            if ( sub[subobs_ready2write].subobs > sub[loop].subobs ) {  // if that other one was for a later sub than this one
              subobs_ready2write = loop;                                // in which case, mark this one as the best so far
            }
          }
        }
      }                                                                 // We left this loop with an index to the best sub to do or a -1 if none available.

//---------- if we don't have one ----------

      if ( subobs_ready2write == -1 ) {                                 // if there is nothing to do
        usleep(100000);                                                 // Chill for a longish time.  NO point in checking more often than a couple of times a second.
      } else {                                                          // or we have some work to do!  Like a whole metafits file to process!

//---------- but if we *do* have metafits info needing to be read ----------

//      This metafits file we are about to read is the metafits that matches the subobs that we began to see packets for 8 seconds ago.  The subobs still has another 8 seconds
//      before we close it off, during which late packets and retries can be requested and received (if we ever finish that).  It's had plenty of time to be written and even
//      updated with tile flags from pointing errors by the M&C

        clock_gettime( CLOCK_REALTIME, &started_meta_write_time);       // Record the start time before we actually get started.  The clock starts ticking from here.

        subm = &sub[subobs_ready2write];                                // Temporary pointer to our sub's metadata array

        subm->meta_msec_wait = ( ( started_meta_write_time.tv_sec - ended_meta_write_time.tv_sec ) * 1000 ) + ( ( started_meta_write_time.tv_nsec - ended_meta_write_time.tv_nsec ) / 1000000 );        // msec since the last metafits processed

        subm->meta_done = 2;                                            // Record that we're working on this one!

        meta_result = 5;                                                // Start by assuming we failed  (ie result==5).  If we get everything working later, we'll swap this for a 4.

        go4meta = TRUE;                                                 // All good so far

//---------- Look in the metafits directory and find the most applicable metafits file ----------

        if ( go4meta ) {                                                                                // If everything is okay so far, enter the next block of code
          go4meta = FALSE;                                                                              // but go back to assuming a failure unless we succeed in the next bit

          bcsf_obsid = 0;                                                                               // 'best candidate so far' is very bad indeed (ie non-existent)

          dir = opendir( conf.metafits_dir );                                                           // Open up the directory where metafits live

          if ( dir == NULL ) {                                                                          // If the directory doesn't exist we must be running on an incorrectly set up server
            printf( "Fatal error: Directory %s does not exist\n", conf.metafits_dir );
            fflush(stdout);
            terminate = TRUE;                                                                           // Tell every thread to close down
            pthread_exit(NULL);                                                                         // and close down ourselves.
          }

          while ( (dp=readdir(dir)) != NULL) {                                                          // Read an entry and while there are still directory entries to look at
            if ( ( dp->d_type == DT_REG ) && ( dp->d_name[0] != '.' ) ) {                                       // If it's a regular file (ie not a directory or a named pipe etc) and it's not hidden
              if ( ( (len_filename = strlen( dp->d_name )) >= 15) && ( strcmp( &dp->d_name[len_filename-14], "_metafits.fits" ) == 0 ) ) {           // If the filename is at least 15 characters long and the last 14 characters are "_metafits.fits"

                mf_obsid = strtoll( dp->d_name,0,0 );                                                   // Get the obsid from the name

                if ( mf_obsid <= subm->subobs ) {                                                       // if the obsid is less than or the same as our subobs id, then it's a candidate for the one we need

                  if ( mf_obsid > bcsf_obsid ) {                                                        // If this metafits is later than the 'best candidate so far'?
                    bcsf_obsid = mf_obsid;                                                              // that would make it our new best candidate so far

//printf( "best metafits #%lld\n", bcsf_obsid );

                    go4meta = TRUE;                                                                     // We've found at least one file worth looking at.
                  }

                }
              }
            }
          }
          closedir(dir);
        }

//---------- Open the 'best candidate so far' metafits file ----------

        if ( go4meta ) {                                                                                // If everything is okay so far, enter the next block of code
          go4meta = FALSE;                                                                              // but go back to assuming a failure unless we succeed in the next bit

          sprintf( metafits_file, "%s/%lld_metafits.fits", conf.metafits_dir, bcsf_obsid );             // Construct the full file name including path

//    printf( "About to read: %s/%lld_metafits.fits\n", conf.metafits_dir, bcsf_obsid );

          status = 0;                                                                                   // CFITSIO status value MUST be initialized to zero!

          if ( !fits_open_file( &fptr, metafits_file, READONLY, &status ) ) {                           // Try to open the file and if it works

            fits_get_hdu_num( fptr, &hdupos );                                                          // get the current HDU position

//            fits_get_hdrspace(fptr, &nkeys, NULL, &status);                                           // get # of keywords
//            printf("Header listing for HDU #%d:\n", hdupos);
//            for (ii = 1; ii <= nkeys; ii++) {                                                         // Read and print each keywords
//              fits_read_record(fptr, ii, card, &status);
//              printf("%s\n", card);
//            }
//            printf("END\n\n");                                                                        // terminate listing with END

//---------- Pull out the header info we need.  Format must be one of TSTRING, TLOGICAL (== int), TBYTE, TSHORT, TUSHORT, TINT, TUINT, TLONG, TULONG, TLONGLONG, TFLOAT, TDOUBLE ----------

            fits_read_key( fptr, TLONGLONG, "GPSTIME", &(subm->GPSTIME), NULL, &status );               // Read the GPSTIME of the metafits observation (should be same as bcsf_obsid but read anyway)
            if (status) {
              printf ( "Failed to read GPSTIME\n" );
              fflush(stdout);
            }

            fits_read_key( fptr, TINT, "EXPOSURE", &(subm->EXPOSURE), NULL, &status );                  // Read the EXPOSURE time from the metafits
            if (status) {
              printf ( "Failed to read Exposure\n" );
              fflush(stdout);
            }

            fits_read_key( fptr, TSTRING, "FILENAME", &(subm->FILENAME), NULL, &status );               // WIP!!! SHould be changed to allow reading more than one line
            if (status) {
              printf ( "Failed to read observation filename\n" );
              fflush(stdout);
            }

//---------- Pull out delay handling settings ----------

            fits_read_key( fptr, TINT, "CABLEDEL", &(subm->CABLEDEL), NULL, &status );                  // Read the CABLEDEL field. 0=Don't apply. 1=apply only the cable delays. 2=apply cable delays _and_ average beamformer dipole delays.

if (debug_mode) subm->CABLEDEL = 1;

            if (status) {
              printf ( "Failed to read CABLEDEL\n" );
              fflush(stdout);
            }

            fits_read_key( fptr, TINT, "GEODEL", &(subm->GEODEL), NULL, &status );			// Read the GEODEL field. (0=nothing, 1=zenith, 2=tile-pointing, 3=az/el table tracking)

if (debug_mode) subm->GEODEL = 3;

            if (status) {
              printf ( "Failed to read GEODEL\n" );
              fflush(stdout);
            }

            fits_read_key( fptr, TINT, "CALIBDEL", &(subm->CALIBDEL), NULL, &status );                  // Read the CALIBDEL field. (0=Don't apply calibration solutions. 1=Do apply)
            if (status) {
              printf ( "Failed to read CALIBDEL\n" );
              fflush(stdout);
            }

//---------- Get Project ID and Observing mode ----------

            fits_read_key( fptr, TSTRING, "PROJECT", &(subm->PROJECT), NULL, &status );
            if (status) {
              printf ( "Failed to read project id\n" );
              fflush(stdout);
            }

            fits_read_key( fptr, TSTRING, "MODE", &(subm->MODE), NULL, &status );
            if (status) {
              printf ( "Failed to read mode\n" );
              fflush(stdout);
            }

//if ( strstr( subm->FILENAME, "mwax_vcs" ) != NULL ) {   // If the FILENAME (obsname) contains mwax_vcs then
//  strcpy( subm->MODE, "VOLTAGE_START" );                // change mode to VOLTAGE_START
//}

//if ( strstr( subm->FILENAME, "mwax_corr" ) != NULL ) {  // If the FILENAME (obsname) contains mwax_corr then
//  strcpy( subm->MODE, "HW_LFILES" );                    // change mode to HW_LFILES
//}

            if ( (subm->GPSTIME + (INT64)subm->EXPOSURE - 1) < (INT64)subm->subobs ) {                  // If the last observation has expired. (-1 because inclusive)
              strcpy( subm->MODE, "NO_CAPTURE" );                                                       // then change the mode to NO_CAPTURE
            }

//---------- Parsing the sky frequency (coarse) channel is a whole job in itself! ----------

            fits_read_key_longstr(fptr, "CHANNELS", &saveptr, NULL, &status);
            if (status) {
              printf ( "Failed to read Channels\n" );
              fflush(stdout);
            }

            strcpy(channels_ascii, saveptr);
            free(saveptr);

//            fits_read_string_key( fptr, "CHANNELS", 1, 190, channels_ascii, &len, NULL, &status );    // First read the metafits line and its 'CONTINUE' lines into a string
//            ffgsky( fptr, "CHANNELS", 1, 190, channels_ascii, &len, NULL, &status );  // First read the metafits line and its 'CONTINUE' lines into a string
//            if (status) {
//              printf ( "Failed to read Channels\n" );
//            }

            ch_index = 0;                                                                               // Start at channel number zero (of 0 to 23)
            saveptr = NULL;
            ptr = channels_ascii;                                                                       // Get a temp copy (but only of the pointer. NOT THE STRING!) that we can update as we step though the channels in the csv list

            while ( ( ( token = strtok_r(ptr, ",", &saveptr) ) != NULL ) & ( ch_index < 24 ) ) {        // Get a pointer to the next number and assuming there *is* one and we still want more
              temp_CHANNELS[ch_index++] = atoi(token);                                                  // turn it into an int and remember it (although it isn't sorted yet)
              ptr = saveptr;                                                                            // Not convinced this line is needed, but it was there in 'recombine' so I left it
            }

            if (ch_index != 24) {
              printf("Did not find 24 channels in metafits file.\n");
              fflush(stdout);
            }

// From the RRI user manual:
// "1. The DR coarse PFB outputs the 256 channels in a fashion that the first 128 channels appear in sequence
// followed by the 129 channels and then 256 down to 130 appear. The setfreq is user specific command wherein
// the user has to enter the preferred 24 channesl in sequence to be transported using the 3 fibers. [ line 14 Appendix- E]"
// Clear as mud?  Yeah.  I thought so too.

// So we want to look through for where a channel number is greater than, or equal to 129.  We'll assume they are already sorted by M&C

            course_swap_index = 24;                                                                     // start by assuming there are no channels to swap

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
                subm->CHANNELS[23-i+(course_swap_index)] = temp_CHANNELS[i];                            // I was confident this line was correct back when 'recombine' was written!
              }
            }

            subm->COARSE_CHAN = subm->CHANNELS[ conf.coarse_chan - 1 ];                                 // conf.coarse_chan numbers are 1 to 24 inclusive, but the array index is 0 to 23 incl.

            if ( subm->COARSE_CHAN == 0 ) printf ( "Failed to parse valid coarse channel\n" );          // Check we found something plausible

/*
fits_read_string_key( fptr, "CHANNELS", 1, 190, channels_ascii, &len, NULL, &status );  // First read the metafits line and its 'CONTINUE' lines into a string
printf( "'%s' parses into ", channels_ascii );
for (int i = 0; i < 24; ++i) {
  printf( "%d,", subm->CHANNELS[i] );
}
printf( " to give %d for coarse chan %d\n", subm->COARSE_CHAN, conf.coarse_chan );
*/

//---------- Hopefully we did that okay, although we better check during debugging that it handles the reversing above channel 128 correctly ----------

            fits_read_key( fptr, TFLOAT, "FINECHAN", &(subm->FINECHAN), NULL, &status );
            if (status) {
              printf ( "Failed to read FineChan\n" );
              fflush(stdout);
            }
            subm->FINECHAN_hz = (int) (subm->FINECHAN * 1000.0);                                        // We'd prefer the fine channel width in Hz rather than kHz.

            fits_read_key( fptr, TFLOAT, "INTTIME", &(subm->INTTIME), NULL, &status );
            if (status) {
              printf ( "Failed to read Integration Time\n" );
              fflush(stdout);
            }
            subm->INTTIME_msec = (int) (subm->INTTIME * 1000.0);                                        // We'd prefer the integration time in msecs rather than seconds.

            fits_read_key( fptr, TINT, "NINPUTS", &(subm->NINPUTS), NULL, &status );
            if (status) {
              printf ( "Failed to read NINPUTS\n" );
              fflush(stdout);
            }
            if ( subm->NINPUTS > MAX_INPUTS ) subm->NINPUTS = MAX_INPUTS;				// Don't allow more inputs than MAX_INPUTS (probably die reading the tile list anyway)

            fits_read_key( fptr, TLONGLONG, "UNIXTIME", &(subm->UNIXTIME), NULL, &status );
            if (status) {
              printf ( "Failed to read UNIXTIME\n" );
              fflush(stdout);
            }

//---------- We now have everything we need from the 1st HDU ----------

            int hdutype=0;
            int colnum;
            int anynulls;
            long nrows;
            long ntimes;

            long frow, felem;

            int cfitsio_ints[MAX_INPUTS];                                       // Temp storage for integers read from the metafits file (in metafits order) before copying to final structure (in sub file order)
            INT64 cfitsio_J[3];							// Temp storage for long "J" type integers read from the metafits file (used in pointing HDU)
            float cfitsio_floats[MAX_INPUTS];                                   // Temp storage for floats read from the metafits file (in metafits order) before copying to final structure (in sub file order)

            char cfitsio_strings[MAX_INPUTS][15];                               // Temp storage for strings read from the metafits file (in metafits order) before copying to final structure (in sub file order)
            char *cfitsio_str_ptr[MAX_INPUTS];                                  // We also need an array of pointers to the stings
            for (int loop = 0; loop < MAX_INPUTS; loop++) {
              cfitsio_str_ptr[loop] = &cfitsio_strings[loop][0];                // That we need to fill with the addresses of the first character in each string in the list of inputs
            }

            int metafits2sub_order[MAX_INPUTS];                                 // index is the position in the metafits file starting at 0.  Value is the order in the sub file starting at 0.

            fits_movrel_hdu( fptr, 1 , &hdutype, &status );                             // Shift to the next HDU, where the antenna table is
            if (status) {
              printf ( "Failed to move to 2nd HDU\n" );
              fflush(stdout);
            }

            fits_get_num_rows( fptr, &nrows, &status );
            if ( nrows != subm->NINPUTS ) {
              printf ( "NINPUTS (%d) doesn't match number of rows in tile data table (%ld)\n", subm->NINPUTS, nrows );
            }

        frow = 1;
        felem = 1;
//        nullval = -99.;

        fits_get_colnum( fptr, CASEINSEN, "Antenna", &colnum, &status );
        fits_read_col( fptr, TINT, colnum, frow, felem, nrows, 0, cfitsio_ints, &anynulls, &status );

        fits_get_colnum( fptr, CASEINSEN, "Pol", &colnum, &status );
        fits_read_col( fptr, TSTRING, colnum, frow, felem, nrows, 0, &cfitsio_str_ptr, &anynulls, &status );

        for (int loop = 0; loop < nrows; loop++) {
          metafits2sub_order[loop] = ( cfitsio_ints[loop] << 1 ) | ( ( *cfitsio_str_ptr[loop] == 'Y' ) ? 1 : 0 );                       // Take the "Antenna" number, multiply by 2 via lshift and iff the 'Pol' is Y, then add in a 1. That's how you know where in the sub file it goes.
//          printf( "%d,", metafits2sub_order[loop] );
        }
//        printf( "\n" );

        // Now we know how to map the the order from the metafits file to the sub file (and internal structure), it's time to start reading in the fields one at a time

//---------- write the 'Antenna' and 'Pol' fields -------- NB: These data are sitting in the temporary arrays already, so we don't need to reread them.

        for (int loop = 0; loop < nrows; loop++) {
          subm->rf_inp[ metafits2sub_order[loop] ].Antenna = cfitsio_ints[loop];                // Copy each integer from the array we got from the metafits (via cfitsio) into one element of the rf_inp array structure
          strcpy( subm->rf_inp[ metafits2sub_order[loop] ].Pol, cfitsio_str_ptr[loop] );        // Copy each string from the array we got from the metafits (via cfitsio) into one element of the rf_inp array structure
        }

//---------- Read and write the 'Input' field --------

        fits_get_colnum( fptr, CASEINSEN, "Input", &colnum, &status );
        fits_read_col( fptr, TINT, colnum, frow, felem, nrows, 0, cfitsio_ints, &anynulls, &status );
        for (int loop = 0; loop < nrows; loop++) subm->rf_inp[ metafits2sub_order[loop] ].Input = cfitsio_ints[loop];           // Copy each integer from the array we got from the metafits (via cfitsio) into one element of the rf_inp array structure

//---------- Read and write the 'Tile' field --------

        fits_get_colnum( fptr, CASEINSEN, "Tile", &colnum, &status );
        fits_read_col( fptr, TINT, colnum, frow, felem, nrows, 0, cfitsio_ints, &anynulls, &status );
        for (int loop = 0; loop < nrows; loop++) subm->rf_inp[ metafits2sub_order[loop] ].Tile = cfitsio_ints[loop];            // Copy each integer from the array we got from the metafits (via cfitsio) into one element of the rf_inp array structure

//---------- Read and write the 'TileName' field --------

        fits_get_colnum( fptr, CASEINSEN, "TileName", &colnum, &status );
        fits_read_col( fptr, TSTRING, colnum, frow, felem, nrows, 0, &cfitsio_str_ptr, &anynulls, &status );
        for (int loop = 0; loop < nrows; loop++) strcpy( subm->rf_inp[ metafits2sub_order[loop] ].TileName, cfitsio_str_ptr[loop] );

//---------- Read and write the 'Rx' field --------

        fits_get_colnum( fptr, CASEINSEN, "Rx", &colnum, &status );
        fits_read_col( fptr, TINT, colnum, frow, felem, nrows, 0, cfitsio_ints, &anynulls, &status );
        for (int loop = 0; loop < nrows; loop++) subm->rf_inp[ metafits2sub_order[loop] ].Rx = cfitsio_ints[loop];              // Copy each integer from the array we got from the metafits (via cfitsio) into one element of the rf_inp array structure

//---------- Read and write the 'Slot' field --------

        fits_get_colnum( fptr, CASEINSEN, "Slot", &colnum, &status );
        fits_read_col( fptr, TINT, colnum, frow, felem, nrows, 0, cfitsio_ints, &anynulls, &status );
        for (int loop = 0; loop < nrows; loop++) subm->rf_inp[ metafits2sub_order[loop] ].Slot = cfitsio_ints[loop];            // Copy each integer from the array we got from the metafits (via cfitsio) into one element of the rf_inp array structure

//---------- Read and write the 'Flag' field --------

        fits_get_colnum( fptr, CASEINSEN, "Flag", &colnum, &status );
        fits_read_col( fptr, TINT, colnum, frow, felem, nrows, 0, cfitsio_ints, &anynulls, &status );
        for (int loop = 0; loop < nrows; loop++) subm->rf_inp[ metafits2sub_order[loop] ].Flag = cfitsio_ints[loop];            // Copy each integer from the array we got from the metafits (via cfitsio) into one element of the rf_inp array structure

//---------- Read and write the 'Length' field --------

        fits_get_colnum( fptr, CASEINSEN, "Length", &colnum, &status );
        fits_read_col( fptr, TSTRING, colnum, frow, felem, nrows, 0, &cfitsio_str_ptr, &anynulls, &status );
//      for (int loop = 0; loop < nrows; loop++) strcpy( subm->rf_inp[ metafits2sub_order[loop] ].Length, cfitsio_str_ptr[loop] );
        for (int loop = 0; loop < nrows; loop++) subm->rf_inp[ metafits2sub_order[loop] ].Length_f = roundl( strtold( cfitsio_str_ptr[loop] + 3, NULL ) * 1000.0);		// Not what it might first appear. Convert the weird ASCII 'EL_123' format 'Length' string into a usable float, The +3 is 'step in 3 characters'

//---------- Read and write the 'North' field --------

        fits_get_colnum( fptr, CASEINSEN, "North", &colnum, &status );
        fits_read_col( fptr, TFLOAT, colnum, frow, felem, nrows, 0, cfitsio_floats, &anynulls, &status );
        for (int loop = 0; loop < nrows; loop++) subm->rf_inp[ metafits2sub_order[loop] ].North = roundl(cfitsio_floats[loop] * 1000.0);	// Convert to long double in mm and round

//---------- Read and write the 'East' field --------

        fits_get_colnum( fptr, CASEINSEN, "East", &colnum, &status );
        fits_read_col( fptr, TFLOAT, colnum, frow, felem, nrows, 0, cfitsio_floats, &anynulls, &status );
        for (int loop = 0; loop < nrows; loop++) subm->rf_inp[ metafits2sub_order[loop] ].East = roundl(cfitsio_floats[loop] * 1000.0);		// Convert to long double in mm and round

//---------- Read and write the 'Height' field --------

        fits_get_colnum( fptr, CASEINSEN, "Height", &colnum, &status );
        fits_read_col( fptr, TFLOAT, colnum, frow, felem, nrows, 0, cfitsio_floats, &anynulls, &status );
        for (int loop = 0; loop < nrows; loop++) subm->rf_inp[ metafits2sub_order[loop] ].Height = roundl(cfitsio_floats[loop] * 1000.0);	// Convert to long double in mm and round

//---------- Read and write the 'Gains' field --------

        fits_get_colnum( fptr, CASEINSEN, "Gains", &colnum, &status );
        // Gains is a little different because it is an array of ints. We're going to read each row (ie rf input) with a separate cfitsio call. Maybe there's a better way to do this, but I don't know it!
        for (int loop = 0; loop < nrows; loop++) {
          fits_read_col( fptr, TINT, colnum, loop+1, felem, 24, 0, subm->rf_inp[ metafits2sub_order[loop] ].Gains, &anynulls, &status );
        }

//---------- Read and write the 'BFTemps' field --------

        fits_get_colnum( fptr, CASEINSEN, "BFTemps", &colnum, &status );
        fits_read_col( fptr, TFLOAT, colnum, frow, felem, nrows, 0, cfitsio_floats, &anynulls, &status );
        for (int loop = 0; loop < nrows; loop++) subm->rf_inp[ metafits2sub_order[loop] ].BFTemps = cfitsio_floats[loop];               // Copy each float from the array we got from the metafits (via cfitsio) into one element of the rf_inp array structure

//---------- Read and write the 'Delays' field --------

        fits_get_colnum( fptr, CASEINSEN, "Delays", &colnum, &status );
        // Like 'Gains', this is a little different because it is an array of ints. See comments against 'Gains' for more detail
        for (int loop = 0; loop < nrows; loop++) {
          fits_read_col( fptr, TINT, colnum, loop+1, felem, 16, 0, subm->rf_inp[ metafits2sub_order[loop] ].Delays, &anynulls, &status );
        }

//---------- Read and write the 'VCSOrder' field --------
//
//        fits_get_colnum( fptr, CASEINSEN, "VCSOrder", &colnum, &status );
//        fits_read_col( fptr, TFLOAT, colnum, frow, felem, nrows, 0, cfitsio_floats, &anynulls, &status );
//        for (int loop = 0; loop < nrows; loop++) subm->rf_inp[ metafits2sub_order[loop] ].VCSOrder = cfitsio_floats[loop];            // Copy each float from the array we got from the metafits (via cfitsio) into one element of the rf_inp array structure
//
//---------- Read and write the 'Flavors' field --------

        fits_get_colnum( fptr, CASEINSEN, "Flavors", &colnum, &status );
        fits_read_col( fptr, TSTRING, colnum, frow, felem, nrows, 0, &cfitsio_str_ptr, &anynulls, &status );
        for (int loop = 0; loop < nrows; loop++) strcpy( subm->rf_inp[ metafits2sub_order[loop] ].Flavors, cfitsio_str_ptr[loop] );

//---------- Read and write the 'Calib_Delay' field --------

        fits_get_colnum( fptr, CASEINSEN, "Calib_Delay", &colnum, &status );
        fits_read_col( fptr, TFLOAT, colnum, frow, felem, nrows, 0, cfitsio_floats, &anynulls, &status );
        for (int loop = 0; loop < nrows; loop++) subm->rf_inp[ metafits2sub_order[loop] ].Calib_Delay = cfitsio_floats[loop];           // Copy each float from the array we got from the metafits (via cfitsio) into one element of the rf_inp array structure

//---------- Read and write the 'Calib_Gains' field --------

        fits_get_colnum( fptr, CASEINSEN, "Calib_Gains", &colnum, &status );
        // Like 'Gains' and 'Delays, this is an array. This time of floats.
        for (int loop = 0; loop < nrows; loop++) {
          fits_read_col( fptr, TFLOAT, colnum, loop+1, felem, 24, 0, subm->rf_inp[ metafits2sub_order[loop] ].Calib_Gains, &anynulls, &status );
        }

//---------- Now we have read everything available from the 2nd HDU but we want to do some conversions and calculations per tile.  That can wait until after we read the 3rd HDU ----------
//           We need to read in the AltAz information (ie the 3rd HDU) for the beginning, middle and end of this subobservation
//           Note the indent change caused by moving code around. Maybe I'll fix that later... Maybe not.

            fits_movrel_hdu( fptr, 1 , &hdutype, &status );                             // Shift to the next HDU, where the AltAz table is
            if (status) {
              printf ( "Failed to move to 3rd HDU\n" );
              fflush(stdout);
            }

            fits_get_num_rows( fptr, &ntimes, &status );				// How many rows (times) are written to the metafits?

            // They *should* start at GPSTIME and have an entry every 4 seconds for EXPOSURE seconds, inclusive of the beginning and end.  ie 3 times for 8 seconds exposure @0sec, @4sec & @8sec
            // So the number of them should be '(subm->EXPOSURE >> 2) + 1'
            // The 3 we want for the beginning, middle and end of this subobs are '((subm->subobs - subm->GPSTIME)>>2)+1', '((subm->subobs - subm->GPSTIME)>>2)+2' & '((subm->subobs - subm->GPSTIME)>>2)+3'
            // a lot of the time, we'll be past the end of the exposure time, so if we are, we'll need to fill the values in with something like a zenith pointing.

            if ( ( subm->GEODEL == 1 ) ||												// If we have been specifically asked for zenith pointings *or*
              ( (((subm->subobs - subm->GPSTIME) >>2)+3) > ntimes ) ) {									// if we want times which are past the end of the list available in the metafits

              for (int loop = 0; loop < 3; loop++) {											// then we need to put some default values in (ie between observations)
                subm->altaz[ loop ].gpstime = ( subm->subobs + loop * 4 );								// populate the true gps times for the beginning, middle and end of this *sub*observation
                subm->altaz[ loop ].Alt = 90.0;												// Point straight up (in degrees above horizon)
                subm->altaz[ loop ].Az = 0.0;												// facing North (in compass degrees)
                subm->altaz[ loop ].Dist_km = 0.0;											// No 'near field' supported between observations so just use 0.

                subm->altaz[ loop ].SinAzCosAlt = 0L;											// will be multiplied by tile East
                subm->altaz[ loop ].CosAzCosAlt = 0L;											// will be multiplied by tile North
                subm->altaz[ loop ].SinAlt = 1L;											// will be multiplied by tile Height
              }

            } else {
              // We know from the condition test above that we have 3 valid pointings available to read from the metafits 3rd HDU

              frow = ((subm->subobs - subm->GPSTIME)>>2) + 1;										// We want to start at the first pointing for this *subobs* not the obs, so we need to step into the list

              //---------- Read and write the 'gpstime' field --------

              fits_get_colnum( fptr, CASEINSEN, "gpstime", &colnum, &status );
              fits_read_col( fptr, TLONGLONG, colnum, frow, felem, 3, 0, cfitsio_J, &anynulls, &status );				// Read start, middle and end time values, beginning at *this* subobs in the observation
              for (int loop = 0; loop < 3; loop++) subm->altaz[ loop ].gpstime = cfitsio_J[loop];					// Copy each 'J' integer from the array we got from the metafits (via cfitsio) into one element of the pointing array structure

              //---------- Read and write the 'Alt' field --------

              fits_get_colnum( fptr, CASEINSEN, "Alt", &colnum, &status );
              fits_read_col( fptr, TFLOAT, colnum, frow, felem, 3, 0, cfitsio_floats, &anynulls, &status );				// Read start, middle and end time values, beginning at *this* subobs in the observation
              for (int loop = 0; loop < 3; loop++) subm->altaz[ loop ].Alt = cfitsio_floats[loop];					// Copy each float from the array we got from the metafits (via cfitsio) into one element of the rf_inp array structure

              //---------- Read and write the 'Az' field --------

              fits_get_colnum( fptr, CASEINSEN, "Az", &colnum, &status );
              fits_read_col( fptr, TFLOAT, colnum, frow, felem, 3, 0, cfitsio_floats, &anynulls, &status );				// Read start, middle and end time values, beginning at *this* subobs in the observation
              for (int loop = 0; loop < 3; loop++) subm->altaz[ loop ].Az = cfitsio_floats[loop];					// Copy each float from the array we got from the metafits (via cfitsio) into one element of the rf_inp array structure

              //---------- Read and write the 'Dist_km' field --------

              fits_get_colnum( fptr, CASEINSEN, "Dist_km", &colnum, &status );
              fits_read_col( fptr, TFLOAT, colnum, frow, felem, 3, 0, cfitsio_floats, &anynulls, &status );				// Read start, middle and end time values, beginning at *this* subobs in the observation
              for (int loop = 0; loop < 3; loop++) subm->altaz[ loop ].Dist_km = cfitsio_floats[loop];					// Copy each float from the array we got from the metafits (via cfitsio) into one element of the rf_inp array structure

              //---------- Now calculate the East/North/Height conversion factors for the three times --------

              for (int loop = 0; loop < 3; loop++) {
                sincosl( d2r * (long double) subm->altaz[ loop ].Alt, &SinAlt, &CosAlt );						// Calculate the Sin and Cos of Alt in one operation.
                sincosl( d2r * (long double) subm->altaz[ loop ].Az,  &SinAz,  &CosAz  );						// Calculate the Sin and Cos of Az in one operation.

                subm->altaz[ loop ].SinAzCosAlt = SinAz * CosAlt;									// this conversion factor will be multiplied by tile East
                subm->altaz[ loop ].CosAzCosAlt = CosAz * CosAlt;									// this conversion factor will be multiplied by tile North
                subm->altaz[ loop ].SinAlt = SinAlt;											// this conversion factor will be multiplied by tile Height
              }

            }

/*
printf ( "Exposure=%d, Calc=%d, Found=%ld, obs=%lld, subobs=%d, 1st=%lld\n", subm->EXPOSURE, (subm->EXPOSURE >> 2) + 1, ntimes, subm->GPSTIME, subm->subobs, ((subm->subobs - subm->GPSTIME)>>2)+1 );
for (int loop = 0; loop < 3; loop++) {											// then we need to put some default values in (ie between observations)
  printf ( "    time=%lld, Alt=%f, Az=%f, dist=%f\n", subm->altaz[loop].gpstime, subm->altaz[loop].Alt, subm->altaz[loop].Az, subm->altaz[loop].Dist_km );
}

altaz[0].gpstime
altaz[0].Alt
altaz[0].Az
altaz[0].Dist_km
*/

//---------- We now have everything we need from the fits file ----------

          }			// End of 'if the metafits file opened correctly'

          if (status == END_OF_FILE)  status = 0;                                                       // Reset after normal error

          fits_close_file(fptr, &status);
          if (status) fits_report_error(stderr, status);                                                // print any error message

        }			// End of 'go for meta' metafile reading

//---------- Let's take all that metafits info, do some maths and other processing and get it ready to use, for when we need to actually write out the sub file

        tile_meta_t *rfm;
        long double delay_so_far_start_mm;									// Delay to apply IN MILLIMETRES calculated so far at start of 8 sec subobservation
        long double delay_so_far_middle_mm;									// Delay to apply IN MILLIMETRES calculated so far at middle of 8 sec subobservation
        long double delay_so_far_end_mm;									// Delay to apply IN MILLIMETRES calculated so far at end of 8 sec subobservation

        for (int loop = 0; loop < subm->NINPUTS; loop++) {
          rfm = &subm->rf_inp[ loop ];										// Make a temporary pointer to this rf input, if only for readability of the source
          rfm->rf_input = ( rfm->Tile << 1 ) | ( ( *rfm->Pol == 'Y' ) ? 1 : 0 );                        	// Take the "Tile" number, multiply by 2 via lshift and iff the 'Pol' is Y, then add in a 1. That gives the content of the 'rf_input' field in the udp packets for this row

          delay_so_far_start_mm = 0L;										// No known delays (yet) for the start of the subobservation. So far we're calculating delays all in millimeters (of light travel time)
          delay_so_far_middle_mm = 0L;										// No known delays (yet) for the middle of the subobservation. So far we're calculating delays all in millimeters (of light travel time)
          delay_so_far_end_mm = 0L;										// No known delays (yet) for the end of the subobservation. So far we're calculating delays all in millimeters (of light travel time)

//---------- Do cable delays ----------

          if ( subm->CABLEDEL >= 1 ){										// CABLEDEL indicates: 0=Don't apply delays. 1=apply only the cable delays. 2=apply cable delays _and_ average beamformer dipole delays.
            delay_so_far_start_mm += rfm->Length_f;								// So add in the cable delays WIP!!! or is it subtract them?
            delay_so_far_middle_mm += rfm->Length_f;								// Cable delays apply equally at the start, middle and end of the subobservation
            delay_so_far_end_mm += rfm->Length_f;								// so add them equally to all three delays for start, middle and end
          }

//          if ( subm->CABLEDEL >= 2 ){										// CABLEDEL indicates: 0=Don't apply delays. 1=apply only the cable delays. 2=apply cable delays _and_ average beamformer dipole delays.
//	    what's the value in mm of the beamformer delays
//            delay_so_far_start_mm += bf_delay;
//            delay_so_far_middle_mm += bf_delay;								// Cable delays apply equally at the start, middle and end of the subobservation
//            delay_so_far_end_mm += bf_delay;									// so add them equally to all three delays for start, middle and end
//          }

//---------- Do Geometric delays ----------

          if ( subm->GEODEL >= 1 ){										// GEODEL field. (0=nothing, 1=zenith, 2=tile-pointing, 3=az/el table tracking)

            delay_so_far_start_mm -=  ( ( rfm->North * subm->altaz[ 0 ].CosAzCosAlt ) +				// add the component of delay caused by the 'north' location of its position
                                        ( rfm->East * subm->altaz[ 0 ].SinAzCosAlt ) +				// add the component of delay caused by the 'east' location of its position
                                        ( rfm->Height * subm->altaz[ 0 ].SinAlt ) );				// add the component of delay caused by the 'height' of its position

            delay_so_far_middle_mm -= ( ( rfm->North * subm->altaz[ 1 ].CosAzCosAlt ) +				// add the component of delay caused by the 'north' location of its position
                                        ( rfm->East * subm->altaz[ 1 ].SinAzCosAlt ) +				// add the component of delay caused by the 'east' location of its position
                                        ( rfm->Height * subm->altaz[ 1 ].SinAlt ) );				// add the component of delay caused by the 'height' of its position

            delay_so_far_end_mm -=    ( ( rfm->North * subm->altaz[ 2 ].CosAzCosAlt ) +				// add the component of delay caused by the 'north' location of its position
                                        ( rfm->East * subm->altaz[ 2 ].SinAzCosAlt ) +				// add the component of delay caused by the 'east' location of its position
                                        ( rfm->Height * subm->altaz[ 2 ].SinAlt ) );				// add the component of delay caused by the 'height' of its position
          }

//---------- Do Calibration delays ----------
//	WIP.  Calibration delays are just a dream right now


//---------- Convert 'start', 'middle' and 'end' delays from millimetres to samples --------

          long double start_sub_s  = delay_so_far_start_mm * mm2s_conv_factor;					// Convert start delay into samples at the coarse channel sample rate
          long double middle_sub_s = delay_so_far_middle_mm * mm2s_conv_factor;					// Convert middle delay into samples at the coarse channel sample rate
          long double end_sub_s    = delay_so_far_end_mm * mm2s_conv_factor;					// Convert end delay into samples at the coarse channel sample rate

//---------- Commit to a whole sample delay and calculate the residuals --------

          long double whole_sample_delay = roundl( middle_sub_s );						// Pick a whole sample delay.  Use the rounded version of the middle delay for now. NB: seems to need -lm for linking
                                                                                                                // This will be corrected for by shifting coarse channel samples forward and backward in time by whole samples

          start_sub_s  -= whole_sample_delay; 									// Remove the whole sample we're using for this subobs to leave the residual delay that will be done by phase turning
          middle_sub_s -= whole_sample_delay;                 							// This may still leave more than +/- a half a sample
          end_sub_s    -= whole_sample_delay;                 							// but it's not allowed to leave more than +/- two whole samples (See Ian's code or IanM for more detail)

//---------- Check it's within the range Ian's code can handle of +/- two whole samples

          if ( (start_sub_s > res_max) || (start_sub_s < res_min ) || (end_sub_s > res_max) || (end_sub_s < res_min) ) {
            printf( "residual delays out of bounds!\n" );
          }

//---------- Now treat the start, middle & end residuals as points on a parabola at x=0, x=800, x=1600 ----------
//      Calculate a, b & c of this parabola in the form: delay = ax^2 + bx + c where x is the (tenth of a) block number

          a = ( start_sub_s - middle_sub_s - middle_sub_s + end_sub_s ) * two_over_num_blocks_sqrd ;                                                          // a = (s+e-2*m)/(n*n/2)
          b = ( middle_sub_s + middle_sub_s + middle_sub_s + middle_sub_s - start_sub_s - start_sub_s - start_sub_s - end_sub_s ) * one_over_num_blocks;      // b = (4*m-3*s-e)/(n)
          c = start_sub_s ;                                                                                                                                   // c = s

//      residual delays can now be interpolated for any time using 'ax^2 + bx + c' where x is the time

//---------- We'll be calulating each value in turn so we're better off passing back in a form only needing 2 additions per data point.
//		The phase-wrap delay correction phase wants the delay at time points of x=.5, x=1.5, x=2.5, x=3.5 etc, so
//		we'll set an initial value of a×0.5^2 + b×0.5 + c to get the first point and our first step in will be:

          long double initial = a * 0.25L + b * 0.5L + c;							// ie a×0.5^2 + b×0.5 + c for our initial value of delay(0.5)
          long double delta = a + a + b;									// That's our first step.  ie delay(1.5) - delay(0.5) or if you like, (a*1.5^2+b*1.5+c) - (a*0.5^2+b*0.5+c)
          long double delta_delta = a + a;									// ie 2a because it's the 2nd derivative

//---------- int32 maths would be fine for this and very fast provided we scaled the values into range ----------
//      We want to calcluate in millisamples, and we want to use most of the bits in the int32, so multiply by '1000 x 2^20'

          initial *= conv2int32;
          delta *= conv2int32;
          delta_delta *= conv2int32;

//---------- Now convert these long double floats to ints ----------

          rfm->ws_delay = (int16_t) whole_sample_delay;				// whole_sample_delay has already been roundl(ed) somewhere above here
          rfm->initial_delay = (int32_t) lroundl(initial);                      // The effect of this lroundl will be lost in the noise since it's a factor of 2^20 away from anything visible but lets be consistent
          rfm->initial_delay += (1 << 19) + (1 << 15); 				// During the >>20 that will come later to get back to millisamples we want to 'round' not truncate so add half of the (1<<20)
                                                                                // ie (1<<19) also add a few percent (~3%) to the millisample count so that "exact" 0.5s don't round down after cummulative
                                                                                // addition rounding errors.
          rfm->delta_delay = (int32_t) lroundl(delta);
          rfm->delta_delta_delay = (int32_t) lroundl(delta_delta);

//---------- Print out a bunch of debug info ----------

          if (debug_mode) {										// Debug logging to screen
            printf( "%d,%d,%d,%d,%d,%d,%s,%s,%d,%d,%d,%Lf,%Lf,%Lf,%Lf,%d,%d,%d,%d,%Lf,%Lf,%Lf,%f,%s,%f:",
            subm->subobs,
            loop,
            rfm->rf_input,
            rfm->Input,
            rfm->Antenna,
            rfm->Tile,
            rfm->TileName,
            rfm->Pol,
            rfm->Rx,
            rfm->Slot,
            rfm->Flag,
//            rfm->Length,
            rfm->Length_f,
            ( delay_so_far_start_mm * mm2s_conv_factor ),
            ( delay_so_far_middle_mm * mm2s_conv_factor ),
            ( delay_so_far_end_mm * mm2s_conv_factor ),
            rfm->ws_delay,
            rfm->initial_delay,
            rfm->delta_delay,
            rfm->delta_delta_delay,
            rfm->North,
            rfm->East,
            rfm->Height,
            // rfm->Gains,
            rfm->BFTemps,
            // rfm->Delays,
//            rfm->VCSOrder,
            rfm->Flavors,
            rfm->Calib_Delay
            //rfm->Calib_Gains
            );

//          for (int loop2 = 0; loop2 < 24; loop2++) {
//            printf( "%d,", rfm->Gains[loop2] );
//          }

//          for (int loop2 = 0; loop2 < 16; loop2++) {
//            printf( "%d,", rfm->Delays[loop2] );
//          }

//          for (int loop2 = 0; loop2 < 24; loop2++) {
//            printf( "%f,", rfm->Calib_Gains[loop2] );
//          }

            printf( "\n" );
          }												// Only see this if we're in debug mode

        }

//---------- And we're basically done reading the metafits and preping for the sub file write which is only a few second away (done by another thread)

        clock_gettime( CLOCK_REALTIME, &ended_meta_write_time);

        subm->meta_msec_took = ( ( ended_meta_write_time.tv_sec - started_meta_write_time.tv_sec ) * 1000 ) + ( ( ended_meta_write_time.tv_nsec - started_meta_write_time.tv_nsec ) / 1000000 );    // msec since this sub started

        subm->meta_done = meta_result;                                      // Record that we've finished working on this one even if we gave up.  Will be a 4 or a 5 depending on whether it worked or not.

//            printf("meta: so=%d,bcsf=%lld,GPSTIME=%lld,EXPOSURE=%d,PROJECT=%s,MODE=%s,CHANNELS=%s,FINECHAN_hz=%d,INTTIME_msec=%d,NINPUTS=%d,UNIXTIME=%lld,len=%d,st=%d,wait=%d,took=%d\n",
//              subm->subobs, bcsf_obsid, subm->GPSTIME, subm->EXPOSURE, subm->PROJECT, subm->MODE, subm->CHANNELS, subm->FINECHAN_hz, subm->INTTIME_msec, subm->NINPUTS, subm->UNIXTIME, len, subm->meta_done, subm->meta_msec_wait, subm->meta_msec_took);

      }				// End of 'if there is a metafits to read' (actually an 'else' off 'is there nothing to do')
    }				// End of huge 'while !terminate' loop
}				// End of function

//---------------------------------------------------------------------------------------------------------------------------------------------------
// makesub - Every time an 8 second block of udp packets is available, try to generate a sub files for the correlator
//---------------------------------------------------------------------------------------------------------------------------------------------------

void *makesub()
{

    printf("Makesub started\n");
    fflush(stdout);

//--------------- Set CPU affinity ---------------

    printf("Set process makesub cpu affinity returned %d\n", set_cpu_affinity ( conf.cpu_mask_makesub ) );
    fflush(stdout);

//---------------- Initialize and declare variables ------------------------

    DIR *dir;
    struct dirent *dp;
    struct stat filestats;
    int len_filename;
    char this_file[300],best_file[300],dest_file[300];                  // The name of the file we're looking at, the name of the best file we've found so far, and the sub's final destination name

    time_t earliest_file;                                               // Holding space for the date we need to beat when looking for old .free files
    int free_files = 0;                                                 // Count of the number of ".free" files available to use for writing out .sub files
    int bad_free_files = 0;                                             // Count of the number of ".free" files that *cannot* be used for some reason (probably the wrong size)

    int active_rf_inputs;                                               // The number of rf_inputs that we want in the sub file and sent at least 1 udp packet

    char *ext_shm_buf;                                                  // Pointer to the header of where all the external data is after the mmap
    char *dest;                                                         // Working copy of the pointer into shm
    char *block1_add;							// The address where the 1st data block is.  That is *after* the 4k header *and* after the 0th block which is metadata

    int ext_shm_fd;                                                     // File descriptor for the shmem block

    size_t transfer_size;
    size_t desired_size;                                                // The total size (including header and zeroth block) that this sub file needs to be

    int loop;
    int subobs_ready2write;
    int left_this_line;
    int sub_result;                                                     // Temp storage for the result before we write it back to the array for others to see.  Once we do that, we can't change our mind.
    BOOL go4sub;

    mwa_udp_packet_t dummy_udp={0};                                     // Make a dummy udp packet full of zeros and NULLs.  We'll use this to stand in for every missing packet!
    void *dummy_volt_ptr = &dummy_udp.volts[0];                         // and we'll remember where we can find UDP_PAYLOAD_SIZE (4096LL) worth of zeros

    subobs_udp_meta_t *subm;                                            // pointer to the sub metadata array I'm working on
    tile_meta_t *rfm;							// Pointer to the tile metadata for the tile I'm working on
  

    
    int block;                                                          // loop variable
    int MandC_rf;                                                       // loop variable
    int ninputs_pad;							// The number of inputs in the sub file padded out with any needed dummy tiles (used to be a thing but isn't any more)
    int ninputs_xgpu;                                                   // The number of inputs in the sub file but padded out to whatever number xgpu needs to deal with them in (not actually padded until done by Ian's code later)

    MandC_meta_t my_MandC_meta[MAX_INPUTS];                             // Working copies of the changeable metafits/metabin data for this subobs
    MandC_meta_t *my_MandC;                                             // Make a temporary pointer to the M&C metadata for one rf input

    block_0_tile_metadata_t block_0_working;				// Construct a single tile's metadata struct to write to the 0th block.  We'll update and write this out as we step through tiles.
    int32_t w_initial_delay;						// tile's initial fractional delay (times 2^20) working copy
    int32_t w_delta_delay;						// tile's initial fractional delay step (like its 1st deriviative)

    int source_packet;                                                  // which packet (of 5002) contains the first byte of data we need
    int source_offset;                                                  // what is the offset in that packet of the first byte we need
    int source_remain;                                                  // How much data is left from that offset to the end of the udp packet

    char *source_ptr;
    int bytes2copy;

    char months[] = "Jan,Feb,Mar,Apr,May,Jun,Jul,Aug,Sep,Oct,Nov,Dec "; // Needed for date to ASCII conversion
    char utc_start[200];                                                // Needed for date to ASCII conversion
    int mnth;                                                           // The current month
    char srch[4] = {0,0,0,0};                                           // Really only need the last byte set to a null as a string terminator for 3 character months

    long int my_time;
    char* t;                                                            // General pointer to char but used for ctime output

    struct timespec started_sub_write_time;
    struct timespec ended_sub_write_time;

//---------------- Main loop to live in until shutdown -------------------

    printf("Makesub entering main loop\n");
    fflush(stdout);

    clock_gettime( CLOCK_REALTIME, &ended_sub_write_time);              // Fake the ending time for the last sub file ('cos like there wasn't one y'know but the logging will expect something)

    while (!terminate) {                                                // If we're not supposed to shut down, let's find something to do

//---------- look for a sub to write out ----------

      subobs_ready2write = -1;                                          // Start by assuming there's nothing to write out

      for ( loop = 0 ; loop < 4 ; loop++ ) {                            // Look through all four subobs meta arrays

        if ( sub[loop].state == 2 ) {                                   // If this sub is ready to write out

          if ( subobs_ready2write == -1 ) {                             // check if we've already found a different one to write out and if we haven't

            subobs_ready2write = loop;                                  // then mark this one as the best so far

          } else {                                                      // otherwise we need to work out

            if ( sub[subobs_ready2write].subobs > sub[loop].subobs ) {  // if that other one was for a later sub than this one
              subobs_ready2write = loop;                                // in which case, mark this one as the best so far
            }
          }
        }
      }                                                                 // We left this loop with an index to the best sub to write or a -1 if none available.

//---------- if we don't have one ----------

      if ( subobs_ready2write == -1 ) {                                 // if there is nothing to do
        monitor.sub_write_sleeps++;
        usleep(20000);                                                  // Chillax for a bit.
      } else {                                                          // or we have some work to do!  Like a whole sub file to write out!

//---------- but if we *do* have a sub file ready to write out ----------

        clock_gettime( CLOCK_REALTIME, &started_sub_write_time);        // Record the start time before we actually get started.  The clock starts ticking from here.

        subm = &sub[subobs_ready2write];                                // Temporary pointer to our sub's metadata array

        subm->msec_wait = ( ( started_sub_write_time.tv_sec - ended_sub_write_time.tv_sec ) * 1000 ) + ( ( started_sub_write_time.tv_nsec - ended_sub_write_time.tv_nsec ) / 1000000 ); // msec since the last sub ending

        subm->udp_at_start_write = UDP_added_to_buff;                   // What's the udp packet number we've received (EVEN IF WE HAVEN'T LOOKED AT IT!) at the time we start to process this sub for writing

        subm->state = 3;                                                // Record that we're working on this one!

        sub_result = 5;                                                 // Start by assuming we failed  (ie result==5).  If we get everything working later, we'll swap this for a 4.

//---------- Do some last-minute metafits work to prepare ----------

        go4sub = TRUE;                                                                                                  // Say it all looks okay so far

        ninputs_pad = subm->NINPUTS;											// We don't pad .sub files any more so these two variables are the same
        ninputs_xgpu = ((subm->NINPUTS + 31) & 0xffe0);                                                                 // Get this from 'ninputs' rounded up to multiples of 32

        transfer_size = ( ( SUB_LINE_SIZE * (BLOCKS_PER_SUB+1LL) ) * ninputs_pad );                                     // Should be 5275648000 for 256T in 160+1 blocks
        desired_size = transfer_size + SUBFILE_HEADER_SIZE;                                                             // Should be 5275652096 for 256T in 160+1 blocks plus 4K header (1288001 x 4K for dd to make)

        active_rf_inputs = 0;                                                                                           // The number of rf_inputs that we want in the sub file and sent at least 1 udp packet

        for ( loop = 0 ; loop < ninputs_pad ; loop++ ) {                                                                // populate the metadata array for all rf_inputs in this subobs incl padding ones
          my_MandC_meta[loop].rf_input = subm->rf_inp[loop].rf_input;
          my_MandC_meta[loop].start_byte = ( UDP_PAYLOAD_SIZE + ( subm->rf_inp[loop].ws_delay * 2  ) );			// NB: Each delay is a sample, ie two bytes, not one!!!
          my_MandC_meta[loop].seen_order = subm->rf2ndx[ my_MandC_meta[loop].rf_input ];                                // If they weren't seen, they will be 0 which maps to NULL pointers which will be replaced with padded zeros
          if ( my_MandC_meta[loop].seen_order != 0 ) active_rf_inputs++;                                                // seen_order starts at 1. If it's 0 that means we didn't even get 1 udp packet for this rf_input
        }

        if ( debug_mode ) {                                                                                             // If we're in debug mode
          sprintf( dest_file, "%s/%lld_%d_%d.free", conf.shared_mem_dir, subm->GPSTIME, subm->subobs, subm->COARSE_CHAN );      // Construct the full file name including path for a .free file
        } else {
          sprintf( dest_file, "%s/%lld_%d_%d.sub", conf.shared_mem_dir, subm->GPSTIME, subm->subobs, subm->COARSE_CHAN );       // Construct the full file name including path for a .sub (regular) file
        }

//---------- Make a 4K ASCII header for the sub file ----------

        memset( sub_header, 0, SUBFILE_HEADER_SIZE );                                                                   // Pre fill with null chars just to ensure we don't leave junk anywhere in the header

        INT64 obs_offset = subm->subobs - subm->GPSTIME;                                                                // How long since the observation started?

        my_time = subm->UNIXTIME - (8 * 60 * 60);                                                                       // Adjust for time zone AWST (Perth time to GMT)
        t = ctime( &my_time );

        memcpy ( srch,&t[4],3 );
        mnth = ( strstr ( months, srch ) - months )/4 + 1;

        sprintf( utc_start, "%.4s-%02d-%.2s-%.8s", &t[20], mnth, &t[8], &t[11] );                                       // Shift all the silly date stuff into a string

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
          "MWAX_U2S_VER " THISVER "-%d\n"
          ;

        sprintf( sub_header, head_mask, SUBFILE_HEADER_SIZE, subm->GPSTIME, subm->subobs, subm->MODE, utc_start, obs_offset,
              NTIMESAMPLES, subm->NINPUTS, ninputs_xgpu, subm->INTTIME_msec, (subm->FINECHAN_hz/ULTRAFINE_BW), transfer_size, subm->PROJECT, subm->EXPOSURE, subm->COARSE_CHAN,
              conf.coarse_chan, subm->UNIXTIME, subm->FINECHAN_hz, (COARSECHAN_BANDWIDTH/subm->FINECHAN_hz), COARSECHAN_BANDWIDTH, SAMPLES_PER_SEC, BUILD );

//---------- Look in the shared memory directory and find the oldest .free file of the correct size ----------

        if ( go4sub ) {                                                                                 // If everything is okay so far, enter the next block of code
          go4sub = FALSE;                                                                               // but go back to assuming a failure unless we succeed in the next bit

          dir = opendir( conf.shared_mem_dir );                                                         // Open up the shared memory directory

          if ( dir == NULL ) {                                                                          // If the directory doesn't exist we must be running on an incorrectly set up server
            printf( "Fatal error: Directory %s does not exist\n", conf.shared_mem_dir );
            fflush(stdout);
            terminate = TRUE;                                                                           // Tell every thread to close down
            pthread_exit(NULL);                                                                         // and close down ourselves.
          }

          earliest_file = 0xFFFFFFFFFFFF;                                                               // Pick some ridiculous date in the future.  This is the date we need to beat.
          free_files = 0;                                                                               // So far, we haven't found any available free files we can reuse
          bad_free_files = 0;                                                                           // nor any free files that we *can't* use (wrong size?)

          while ( (dp=readdir(dir)) != NULL) {                                                          // Read an entry and while there are still directory entries to look at
            if ( ( dp->d_type == DT_REG ) && ( dp->d_name[0] != '.' ) ) {                               // If it's a regular file (ie not a directory or a named pipe etc) and it's not hidden
              if ( ( (len_filename = strlen( dp->d_name )) >= 5) && ( strcmp( &dp->d_name[len_filename-5], ".free" ) == 0 ) ) {           // If the filename is at least 5 characters long and the last 5 characters are ".free"

                sprintf( this_file, "%s/%s", conf.shared_mem_dir, dp->d_name );                         // Construct the full file name including path
                if ( stat ( this_file, &filestats ) == 0 ) {                                            // Try to read the file statistics and if they are available
                  if ( filestats.st_size == desired_size ) {                                            // If the file is exactly the size we need

                    // printf( "File %s has size = %ld and a ctime of %ld\n", this_file, filestats.st_size, filestats.st_ctim.tv_sec );

                    free_files++;                                                                       // That's one more we can use!

                    if ( filestats.st_ctim.tv_sec < earliest_file ) {                                   // and this file has been there longer than the longest one we've found so far (ctime)
                      earliest_file = filestats.st_ctim.tv_sec;                                         // We have a new 'oldest' time to beat
                      strcpy( best_file, this_file );                                                   // and we'll store its name
                      go4sub = TRUE;                                                                    // We've found at least one file we can reuse.  It may not be the best, but we know we can do this now.
                    }
                  } else {
                    bad_free_files++;                                                                       // That's one more we can use!
                  }
                }
              }
            }
          }
          closedir(dir);
        }

//---------- Try to mmap that file (assuming we found one) so we can treat it like RAM (which it actually is) ----------

        if ( go4sub ) {                                                                                 // If everything is okay so far, enter the next block of code
          go4sub = FALSE;                                                                               // but go back to assuming a failure unless we succeed in the next bit

          // printf( "The winner is %s\n", best_file );

          if ( rename( best_file, conf.temp_file_name ) != -1 ) {                                       // If we can rename the file to our temporary name

            if ( (ext_shm_fd = shm_open( &conf.temp_file_name[8], O_RDWR, 0666)) != -1 ) {              // Try to open the shmem file (after removing "/dev/shm" ) and if it opens successfully

              if ( (ext_shm_buf = (char *) mmap (NULL, desired_size, PROT_READ | PROT_WRITE, MAP_SHARED , ext_shm_fd, 0)) != ((char *)(-1)) ) {      // and if we can mmap it successfully
                go4sub = TRUE;                                                                          // Then we are all ready to use the buffer so remember that
              } else {
                printf( "mmap failed\n" );
                fflush(stdout);
              }

              close( ext_shm_fd );                                                                      // If the shm_open worked we want to close the file whether or not the mmap worked

            } else {
              printf( "shm_open failed\n" ) ;
              fflush(stdout);
            }

          } else {
            printf( "Failed rename\n" );
            fflush(stdout);
          }
        }

//---------- Hopefully we have an mmap to a sub file in shared memory and we're ready to fill it with data ----------

        if ( go4sub ) {                                                         // If everything is good to go so far

          dest = ext_shm_buf;                                                   // we'll be trying to write to this block of RAM in strict address order, starting at the beginning (ie ext_shm_buf)

          dest = mempcpy( dest, sub_header, SUBFILE_HEADER_SIZE );              // Do the memory copy from the preprepared 4K subfile header to the beginning of the sub file

          block1_add = dest + ( ninputs_pad * SUB_LINE_SIZE );			// Work out where we need to be when we write out the 1st block (ie after the 0th block). This makes it easy for us to 'pad out' the 0th block to a full block size.

//---------- Write out the 0th block (the tile metadata block) ----------

          for ( MandC_rf = 0; MandC_rf < ninputs_pad; MandC_rf++ ) {            // The zeroth block is the size of the padded number of inputs times SUB_LINE_SIZE.  NB: We dodn't pad any more!

            rfm = &subm->rf_inp[ MandC_rf ];					// Get a temp pointer for this tile's metadata

            block_0_working.rf_input = rfm->rf_input;				// Copy the tile's ID and polarization into the structure we're about to write to the sub file's 0th block
            block_0_working.ws_delay = rfm->ws_delay;				// Copy the tile's whole sample delay value into the structure we're about to write to the sub file's 0th block

            w_initial_delay = rfm->initial_delay;				// Copy the tile's initial fractional delay (times 2^20) to a working copy we can update as we step through the delays
            w_delta_delay = rfm->delta_delay;					// Copy the tile's initial fractional delay step (like its 1st deriviative) to a working copy we can update as we step through the delays 

            block_0_working.initial_delay = w_initial_delay;			// Copy the tile's initial fractional delay (times 2^20) to the structure we're about to write to the 0th block
            block_0_working.delta_delay = w_delta_delay;			// Copy the tile's initial fractional delay step (like its 1st deriviative) to the structure we're about to write to the 0th block
            block_0_working.delta_delta_delay = rfm->delta_delta_delay;		// Copy the tile's fractional delay step's step (like its 2nd deriviative) to the structure we're about to write to the 0th block
            block_0_working.num_pointings = 1;					// Initially 1, but might grow to 10 or more if beamforming.  The first pointing is for delay tracking in the correlator.

            for ( int loop = 0; loop < POINTINGS_PER_SUB; loop++ ) {		// populate the fractional delay lookup table for this tile (one for each 5ms)
              block_0_working.frac_delay[ loop ] = (w_initial_delay >> 20);	// Rounding and everything was all included when we did the original maths in the other thread
              w_initial_delay += w_delta_delay;					// update our *TEMP* copies as we cycle through
              w_delta_delay += block_0_working.delta_delta_delay;		// and update out *TEMP* delta.  We need to do this using temp values, so we don't 'use up' the initial values we want to write out.
            }

            dest = mempcpy( dest, &block_0_working, sizeof(block_0_working) );	// write them out one at a time
          }

          // 'dest' now points to the address after our last memory write, BUT we need to pad out to a whole block size!
          // 'block1_add points to the address at the beginning of block 1.  The remainder of block 0 needs to be null filled

          memset( dest, 0, block1_add - dest );					// Write out a bunch of nulls to line us up with the first voltage block (ie block 1)
          dest = block1_add;							// And set our write pointer to the beginning of block 1

//---------- Write out the voltage data blocks ----------

          for ( block = 1; block <= BLOCKS_PER_SUB; block++ ) {                 // We have 160 (or whatever) blocks of real data to write out. We'll do them in time order (50ms each)

            for ( MandC_rf = 0; MandC_rf < ninputs_pad; MandC_rf++ ) {          // Now step through the M&C ordered rf inputs for the inputs the M&C says should be included in the sub file.  This is likely to be similar to the list we actually saw, but may not be identical!

              my_MandC = &my_MandC_meta[MandC_rf];                              // Make a temporary pointer to the M&C metadata for this rf input.  NB these are sorted in order they need to be written out as opposed to the order the packets arrived.

              left_this_line = SUB_LINE_SIZE;

              while ( left_this_line > 0 ) {                                    // Keep going until we've finished writing out a whole line of the sub file

                source_packet = ( my_MandC->start_byte / UDP_PAYLOAD_SIZE );    // Initially the udp payload size is a power of 2 so this could be done faster with a bit shift, but what would happen if the packet size changed?
                source_offset = ( my_MandC->start_byte % UDP_PAYLOAD_SIZE );    // Initially the udp payload size is a power of 2 so this could be done faster with a bit mask, but what would happen if the packet size changed?
                source_remain = ( UDP_PAYLOAD_SIZE - source_offset );           // How much of the packet is left in bytes?  It should be an even number and at least 2 or I have a bug in the logic or code

                source_ptr = subm->udp_volts[my_MandC->seen_order][source_packet];      // Pick up the pointer to the udp volt data we need (assuming we ever saw it arrive)
                if ( source_ptr == NULL ) {                                             // but if it never arrived
                  source_ptr = dummy_volt_ptr;                                          // point to our pre-prepared, zero filled, fake packet we use when the real one isn't available
                  subm->udp_dummy++;                                                    // The number of dummy packets we needed to insert to pad things out. Make a note for reporting and debug purposes
                }

                bytes2copy = ( (source_remain < left_this_line) ? source_remain : left_this_line );             // Get the minimum of the remaining length of the udp volt payload or the length of line left to populate

                dest = mempcpy( dest, source_ptr + source_offset, bytes2copy );         // Do the memory copy from the udp payload to the sub file and update the destination pointer

                left_this_line -= bytes2copy;                                           // Keep up to date with the remaining amount of work needed on this line
                my_MandC->start_byte += bytes2copy;                                     // and where we are up to in reading them

              }                                                                 // We've finished the udp packet

            }                                                                   // We've finished the signal chain for this block

          }                                                                     // We've finished the block

          // By here, the entire sub has been written out to shared memory and it's time to close it out and rename it so it becomes available for other programs

          if ( (dest - ext_shm_buf) != desired_size ) printf("Memory pointer error in writing sub file!\n");	// before we do that, let's just do a quick check to see we ended up exactly at the end, so we can confirm all our maths is right.

          munmap(ext_shm_buf, desired_size);                                    // Release the mmap for the whole sub file

          if ( rename( conf.temp_file_name, dest_file ) != -1 ) {               // Rename my temporary file to a final sub file name, thus releasing it to other programs
            sub_result = 4;                                                     // This was our last check.  If we got to here, the sub file worked, so prepare to write that to monitoring system
          } else {
            sub_result = 10;                                                    // This was our last check.  If we got to here, the sub file rename failed, so prepare to write that to monitoring system
            printf( "Final rename failed.\n" ) ;                                // but if that fails, I'm not really sure what to do.  Let's just note it and see if it ever happens
            fflush(stdout);
          }

        }                                                               // We've finished the sub file writing and closed and renamed the file.

//---------- We're finished or we've given up.  Either way record the new state and elapsed time ----------

        subm->udp_at_end_write = UDP_added_to_buff;                     // What's the udp packet number we've received (EVEN IF WE HAVEN'T LOOKED AT IT!) at the time we finish processing this sub for writing

        clock_gettime( CLOCK_REALTIME, &ended_sub_write_time);

        subm->msec_took = ( ( ended_sub_write_time.tv_sec - started_sub_write_time.tv_sec ) * 1000 ) + ( ( ended_sub_write_time.tv_nsec - started_sub_write_time.tv_nsec ) / 1000000 ); // msec since this sub started

/*
        if ( ( subm->udp_count == 1280512 ) && ( subm->udp_dummy == 1310720 ) ) {                                               //count=1280512,dummy=1310720 is a perfect storm.  No missing packets, but I couldn't find any packets at all.
          printf ( "my_MandC_meta array\n" );
          for ( loop = 0 ; loop < ninputs_pad ; loop++ ) {
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

        subm->state = sub_result;                                       // Record that we've finished working on this one even if we gave up.  Will be a 4 or a 5 depending on whether it worked or not.

        printf("now=%lld,so=%d,ob=%lld,%s,st=%d,free=%d:%d,wait=%d,took=%d,used=%lld,count=%d,dummy=%d,rf_inps=w%d:s%d:c%d\n",
            (INT64)(ended_sub_write_time.tv_sec - GPS_offset), subm->subobs, subm->GPSTIME, subm->MODE, subm->state, free_files, bad_free_files, subm->msec_wait, subm->msec_took, subm->udp_at_end_write - subm->first_udp, subm->udp_count, subm->udp_dummy, subm->NINPUTS, subm->rf_seen, active_rf_inputs );

        fflush(stdout);

      }

    }                                                                   // Jump up to the top and look again (as well as checking if we need to shut down)

//---------- We've been told to shut down ----------

    printf( "Exiting makesub\n" );

    pthread_exit(NULL);
}

void *heartbeat()
{
    unsigned char ttl = MONITOR_TTL;
    struct sockaddr_in addr;
    struct in_addr localInterface;

    int monitor_socket;

    // create what looks like an ordinary UDP socket
    if ( ( monitor_socket = socket( AF_INET, SOCK_DGRAM, 0) ) < 0) {
      perror("socket");
      terminate = TRUE;
      pthread_exit(NULL);
    }

    // set up destination address
    memset( &addr, 0, sizeof(addr) );
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = inet_addr( MONITOR_IP );
    addr.sin_port = htons( MONITOR_PORT );

    setsockopt( monitor_socket, IPPROTO_IP, IP_MULTICAST_TTL, &ttl, sizeof(ttl) );

    char loopch = 0;

    if (setsockopt( monitor_socket, IPPROTO_IP, IP_MULTICAST_LOOP, (char *) &loopch, sizeof(loopch) ) < 0) {
      perror("setting IP_MULTICAST_LOOP:");
      close( monitor_socket );
      terminate = TRUE;
      pthread_exit(NULL);
    }

    // Set local interface for outbound multicast datagrams. The IP address specified must be associated with a local, multicast-capable interface.
    localInterface.s_addr = inet_addr( conf.local_if );

    if (setsockopt( monitor_socket, IPPROTO_IP, IP_MULTICAST_IF, (char *) &localInterface, sizeof(localInterface) ) < 0) {
      perror("setting local interface");
      close( monitor_socket );
      terminate = TRUE;
      pthread_exit(NULL);
    }

    while(!terminate) {
      if (sendto( monitor_socket, &monitor, sizeof(monitor), 0, (struct sockaddr *) &addr, sizeof(addr) ) < 0) {
        printf( "\nFailed to send monitor packet" );
        fflush(stdout);
      }
      monitor.sub_write_sleeps = 0;
      usleep(1000000);
    }

    printf("\nStopping heartbeat");
    pthread_exit(NULL);
}

//---------------------------------------------------------------------------------------------------------------------------------------------------

void sigint_handler(int signo)
{
  printf("\n\nAsked to shut down via SIGINT\n");
  fflush(stdout);
  terminate = TRUE;                             // This is a volatile, file scope variable.  All threads should be watching for this.
}

//---------------------------------------------------------------------------------------------------------------------------------------------------

void sigterm_handler(int signo)
{
  printf("\n\nAsked to shut down via SIGTERM\n");
  fflush(stdout);
  terminate = TRUE;                             // This is a volatile, file scope variable.  All threads should be watching for this.
}

//---------------------------------------------------------------------------------------------------------------------------------------------------

void sigusr1_handler(int signo)
{
  printf("\n\nSomeone sent a sigusr1 signal to me\n");
  fflush(stdout);
}

//---------------------------------------------------------------------------------------------------------------------------------------------------

void usage(char *err)                           // Bad command line.  Report the supported usage.
{
    printf( "%s", err );
    printf("\n\n To run:        /home/mwa/udp2sub -f mwax.conf -i 1\n\n");

    printf("                    -f Configuration file to use for settings\n");
    printf("                    -i Instance number on server, if multiple copies per server in use\n");
    printf("                    -c Coarse channel override\n");
    printf("                    -d Debug mode.  Write to .free files\n");
    fflush(stdout);
}

// ------------------------ Start of world -------------------------

int main(int argc, char **argv)
{
    int prog_build = BUILD;                             // Build number.  Remember to update each build revision!

//---------------- Parse command line parameters ------------------------

    int instance = 0;                                           // Assume we're the first (or only) instance on this server
    int chan_override = 0;                                      // Assume we are going to use the default coarse channel for this instance
    char *conf_file = "/vulcan/mwax_config/mwax_u2s.cfg";       // Default configuration path

    while (argc > 1 && argv[1][0] == '-') {
      switch (argv[1][1]) {

        case 'd':
          debug_mode = TRUE;
          printf ( "Debug mode on\n" );
          fflush(stdout);
          break ;

        case 'i':
          ++argv ;
          --argc ;
          instance = atoi(argv[1]);
          break;

        case 'c':
          ++argv ;
          --argc ;
          chan_override = atoi(argv[1]);
          break;

        case 'f':
          ++argv ;
          --argc ;
          conf_file = argv[1];
          printf ( "Config file to read = %s\n", conf_file );
          fflush(stdout);
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

    if ( gethostname( hostname, sizeof hostname ) == -1 ) strcpy( hostname, "unknown" );        // Get the hostname or default to unknown if we fail
    printf("Running udp2sub on %s.  ver " THISVER " Build %d\n", hostname, prog_build);
    fflush(stdout);

    read_config( conf_file, hostname, instance, chan_override, &conf );         // Use the config file, the host name and the instance number (if there is one) to look up all our configuration and settings

    if ( conf.udp2sub_id == 0 ) {                                       // If the lookup returned an id of 0, we don't have enough information to continue
      printf("Hostname not found in configuration\n");
      fflush(stdout);
      exit(EXIT_FAILURE);                                               // Abort!
    }

//---------------- We have our config details now. Create/open a monitoring file in shared memory to tell M&C how we're doing ------------------------

//WIP!

//---------------- Trap signals from outside the program ------------------------

    signal(SIGINT, sigint_handler);                     // Tell the OS that we want to trap SIGINT calls
    signal(SIGTERM, sigterm_handler);                   // Tell the OS that we want to trap SIGTERM calls
    signal(SIGUSR1, sigusr1_handler);                   // Tell the OS that we want to trap SIGUSR1 calls

//---------------- Allocate the RAM we need for the incoming udp buffer and initialise it ------------------------

    UDP_num_slots = conf.UDP_num_slots;                                 // We moved this to a config variable, but most of the code still assumes the old name so make both names valid

    msgvecs = calloc( 2 * UDP_num_slots, sizeof(struct mmsghdr) );      // NB Make twice as big an array as the number of actual UDP packets we are going to buffer
    if ( msgvecs == NULL ) {                                            // If that failed
      printf("msgvecs calloc failed\n");                                // log a message
      fflush(stdout);
      exit(EXIT_FAILURE);                                               // and give up!
    }

    iovecs = calloc( UDP_num_slots, sizeof(struct iovec) );             // NB Make the *same* number of entries as the number of actual UDP packets we are going to buffer
    if ( iovecs == NULL ) {                                             // If that failed
      printf("iovecs calloc failed\n");                                 // log a message
      fflush(stdout);
      exit(EXIT_FAILURE);                                               // and give up!
    }

    UDPbuf = calloc( UDP_num_slots, sizeof( mwa_udp_packet_t ) );       // NB Make the *same* number of entries as the number of actual UDP packets we are going to buffer
    if ( UDPbuf == NULL ) {                                             // If that failed
      printf("UDPbuf calloc failed\n");                                 // log a message
      fflush(stdout);
      exit(EXIT_FAILURE);                                               // and give up!
    }

//---------- Now initialize the arrays

    for (int loop = 0; loop < UDP_num_slots; loop++) {

        iovecs[ loop ].iov_base = &UDPbuf[loop];
        iovecs[ loop ].iov_len = sizeof( mwa_udp_packet_t );

        msgvecs[ loop ].msg_hdr.msg_iov = &iovecs[loop];                // Populate the first copy
        msgvecs[ loop ].msg_hdr.msg_iovlen = 1;

        msgvecs[ loop + UDP_num_slots ].msg_hdr.msg_iov = &iovecs[loop];        // Now populate a second copy of msgvecs that point back to the first set
        msgvecs[ loop + UDP_num_slots ].msg_hdr.msg_iovlen = 1;                 // That way we don't need to worry (as much) about rolling over buffers during reads
    }

//---------------- Allocate the RAM we need for the subobs pointers and metadata and initialise it ------------------------

    sub = calloc( 4, sizeof( subobs_udp_meta_t ) );                     // Make 4 slots to store the metadata against the maximum 4 subobs that can be open at one time
    if ( sub == NULL ) {                                                // If that failed
      printf("sub calloc failed\n");                                    // log a message
      fflush(stdout);
      exit(EXIT_FAILURE);                                               // and give up!
    }

    blank_sub_line = calloc( SUB_LINE_SIZE, sizeof(char));              // Make ourselves a buffer of zeros that's the size of an empty line.  We'll use this for padding out sub file
    if ( blank_sub_line == NULL ) {                                     // If that failed
      printf("blank_sub_line calloc failed\n");                         // log a message
      fflush(stdout);
      exit(EXIT_FAILURE);                                               // and give up!
    }

    sub_header = calloc( SUBFILE_HEADER_SIZE, sizeof(char));            // Make ourselves a buffer that's initially of zeros that's the size of a sub file header.
    if ( sub_header == NULL ) {                                         // If that failed
      printf("sub_header calloc failed\n");                             // log a message
      fflush(stdout);
      exit(EXIT_FAILURE);                                               // and give up!
    }

//---------------- We're ready to fire up the four worker threads now ------------------------

    printf("Firing up pthreads\n");
    fflush(stdout);

    pthread_t UDP_recv_pt;
    pthread_create(&UDP_recv_pt,NULL,UDP_recv,NULL);                    // Fire up the process to receive the udp packets into the large buffers we just created

    pthread_t UDP_parse_pt;
    pthread_create(&UDP_parse_pt,NULL,UDP_parse,NULL);                  // Fire up the process to parse the udp packets and generate arrays of sorted pointers

    pthread_t makesub_pt;
    pthread_create(&makesub_pt,NULL,makesub,NULL);                      // Fire up the process to generate sub files from raw packets and pointers

    //pthread_t monitor_pt;
    //pthread_create(&monitor_pt,NULL,heartbeat,NULL);

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

    mwa_udp_packet_t *my_udp;                                           // Make a local pointer to the UDP packet we're going to be working on.
    uint32_t subobs_mask = 0xFFFFFFF8;                                  // Mask to apply with '&' to get sub obs from GPS second

    INT64 UDP_closelog = UDP_removed_from_buff - 20;                    // Go back 20 udp packets

    if ( debug_mode ) {                                                 // If we're in debug mode
      UDP_closelog = UDP_removed_from_buff - 100;                      // go back 100!
    }

    if ( UDP_closelog < 0 ) UDP_closelog = 0;                           // In case we haven't really started yet

    while ( UDP_closelog <= UDP_removed_from_buff ) {
      my_udp = &UDPbuf[ UDP_closelog % UDP_num_slots ];
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

    free( msgvecs );                                                    // The threads are dead, so nobody needs this memory now. Let it be free!
    free( iovecs );                                                     // Give back to OS (heap)
    free( UDPbuf );                                                     // This is the big one.  Probably many GB!

    free( sub );                                                        // Free the metadata array storage area

    printf("Exiting process\n");
    fflush(stdout);

    exit(EXIT_SUCCESS);
}

// End of code.  Comments, snip and examples only follow
