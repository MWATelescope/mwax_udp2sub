//===================================================================================================================================================
// Capture UDP packets into a temporary, user space buffer and then sort/copy/process them into some output array
//
// Author(s)  BWC Brian Crosse brian.crosse@curtin.edu.au
//            LAW Luke Williams luke.a.williams@curtin.edu.au
//            CJP Christopher Phillips christopher.j.phillips@curtin.edu.au
// Commenced 2017-05-25
//
#define BUILD 92
#define THISVER "2.14b"
//
// 2.14-092     2024-10-23 CJP  major overhaul of buffer state transitions alongside additional logging to catch/repair issues
//                              with udp2sub locking up after moderate packet loss incidents, and state system documentation.
//
// 2.13-091     2024-09-04 CJP  navigate metafits HDUs by name not index, separate out metafits parsing to dedicated function,
//                              and add more error checks and logging.  Add and apply standard clang-format settings.  First
//                              run at a CMake build.
//
// 2.12-090     2024-08-13 CJP  buffer state logging, and stopped reading CALIB_DELAY/CALIB_GAINS
//
// 2.11-089     2024-06-05 CJP  completed more header offsets, improved shutdown behaviour in the absence of packets.
//
// 2.10-088     2024-05-07 CJP  determine oversampling from mwax.cfg, pass through deripple, set packet_map offset in header.
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
//                              Read ALTAZ table from metafits (3rd HDU) for delay calculations
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

#define _GNU_SOURCE

#include <sys/types.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <stdbool.h>
#include <stdatomic.h>

#include <sys/socket.h>

#include <time.h>
#include <stdio.h>
#include <pthread.h>
#include <stdlib.h>
#include <signal.h>
#include <string.h>
#include <math.h>
#include <unistd.h>
#include <fcntl.h>
#include <dirent.h>
#include <float.h>

#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/shm.h>

#include <fitsio.h>
#include <stdarg.h>

#include "vec3.h"

//---------------- and some new friends -------------------

#define SUB_SLOTS 4
#define MAX_INPUTS (544LL)
#define UDP_PAYLOAD_SIZE (4096LL)

#define LIGHTSPEED (299792458000.0L)

#define SUBFILE_HEADER_SIZE (4096LL)
#define SAMPLES_PER_SEC (conf.oversampling ? 1638400LL : 1280000LL)
#define COARSECHAN_BANDWIDTH (1280000LL)
#define ULTRAFINE_BW (200ll)
#define BLOCKS_PER_SUB (160LL)
#define FFT_PER_BLOCK (10LL)

#define POINTINGS_PER_SUB (BLOCKS_PER_SUB * FFT_PER_BLOCK)
// NB: POINTINGS_PER_SUB gives 1600 so that every 5ms we write a new delay to the sub file.

#define SUB_LINE_SIZE ((SAMPLES_PER_SEC * 8LL * 2LL) / BLOCKS_PER_SUB)
// They might be 6400 samples each for critically sampled data or 8192 for 32/25 oversampled data. Ether way, the 5ms is constant while Ultrafine_bw is 200Hz

#define NTIMESAMPLES ((SAMPLES_PER_SEC * 8LL) / BLOCKS_PER_SUB)

// samples per second * eight seconds * (one byte each for real and imaginary components)
#define UDP_PER_RF_PER_SUB (((SAMPLES_PER_SEC * 8LL * 2LL) / UDP_PAYLOAD_SIZE) + 2)
#define SUBSECSPERSEC ((SAMPLES_PER_SEC * 2LL) / UDP_PAYLOAD_SIZE)  // 625/800, for critically sampled/oversampled
#define SUBSECSPERSUB (SUBSECSPERSEC * 8LL)

// #define RECVMMSG_MODE ( MSG_DONTWAIT )
#define RECVMMSG_MODE (MSG_WAITFORONE)
#define UDP_RECV_SHUTDOWN_TIMEOUT 2000000  // microseconds

#define HOSTNAME_LENGTH 21

#define MONITOR_IP "224.0.2.2"
#define MONITOR_PORT 8007
#define MONITOR_TTL 3

#define FITS_CHECK(OP)                                      \
  if (status) {                                             \
    fprintf(stderr, "Error in metafits access (%s): ", OP); \
    fits_report_error(stderr, status);                      \
    return false;                                           \
  }
#ifdef DEBUG
#define DEBUG_LOG(...) fprintf(stderr, __VA_ARGS__)
#else
#define DEBUG_LOG(...)
#endif

#define MWA_PACKET_TYPE_HEADER 0x00        // 0x00 == Binary Header
#define MWA_PACKET_TYPE_LEGACY 0x20        // 0x20 == Legacy Mode  2K samples of Voltage Data in complex 8 bit real + 8 bit imaginary format).
#define MWA_PACKET_TYPE_OVERSAMPLING 0x30  // 0x30 == Oversampling Mode 2K samples of Voltage Data in complex 8 bit real + 8 bit imaginary format)

// In legacy mode, there are 625 packets per second and 2048 samples per packet. This results in 1.28M samples per second.
// In oversampling mode, there are 800 packets per second and 2048 samples per packet. This results in 1.6384M samples per second.

//---------------- MWA external Structure definitions --------------------

#pragma pack(push, 1)  // We're writing this header into all our packets, so we want/need to force the compiler not to add its own idea of structure padding

typedef struct mwa_udp_packet {  // Structure format for the MWA data packets

  uint8_t packet_type;   // Packet type (cf MWA_PACKET_TYPE_* above)
  uint8_t freq_channel;  // The current coarse channel frequency number [0 to 255 inclusive].  This number is available from the header, but repeated here to simplify archiving and
                         // monitoring multiple frequencies.
  uint16_t rf_input;     // maps to tile/antenna and polarisation (LSB is 0 for X, 1 for Y)

  uint32_t GPS_time;  // [second] GPS time of packet (bottom 32 bits only).  Is overwriten with the GPS time of the start of this subobservation.

  uint16_t subsec_time;  // count from 0 to PACKETS_PER_SEC - 1 (from header)  Typically PACKETS_PER_SEC is 625 in MWA.  Is overwritten with packet count within this subobs.
  uint8_t spare1;        // spare padding byte.  Reserved for future use.
  uint8_t edt2udp_id;    // Which edt2udp instance generated this packet and therefore a lookup to who to ask for a retransmission (0 implies no retransmission possible)

  uint16_t edt2udp_token;  // value to pass back to edt2udp instance to help identify source packet.  A token's value is constant for a given source rf_input and freq_channel for
                           // entire sub-observation
  uint8_t spare2[2];       // Spare bytes for future use

  uint16_t volts[2048];  // 2048 complex voltage samples.  Each one is 8 bits real, followed by 8 bits imaginary but we'll treat them as a single 16 bit 'bit pattern'

} mwa_udp_packet_t;

typedef struct udp2sub_monitor {   // Health packet data
  uint16_t version;                // U2S build version
  uint16_t instance;               // Instance ID
  uint8_t coarse_chan;             // Coarse chan from 01 to 24.
  char hostname[HOSTNAME_LENGTH];  // Host name is looked up to against these strings (21 bytes)
  uint64_t udp_count;              // Cumulative total UDP packets collected from the NIC
  uint64_t udp_dummy;              // Cumulative total dummy packets inserted to pad out subobservations
  uint32_t discarded_subobs;       // Cumulative total subobservations discarded for being too old
} udp2sub_monitor_t;

// TODO:
// Last sub file created
// most advanced udp packet
// Lowest remaining buffer left
// Stats on lost packets
// mon->sub_write_sleeps++;                                        // record we went to sleep
// mon->sub_write_dummy++;                                         // record we needed to use the dummy packet to fill in for a missing udp packet

#pragma pack(pop)  // Set the structure packing back to 'normal' whatever that is

//---------------- internal structure definitions --------------------

typedef struct altaz_meta {  // Structure format for the metadata associated with the pointing at the beginning, middle and end of the sub-observation

  int64_t gpstime;
  float Alt;
  float Az;
  float Dist_km;

  long double SinAzCosAlt;  // will be multiplied by tile East
  long double CosAzCosAlt;  // will be multiplied by tile North
  long double SinAlt;       // will be multiplied by tile Height

} altaz_meta_t;

typedef struct tile_meta {  // Structure format for the metadata associated with one rf input for one subobs

  int Input;
  int Antenna;
  int Tile;
  char TileName[9];  // Leave room for a null on the end!
  char Pol[2];       // Leave room for a null on the end!
  int Rx;
  int Slot;
  int Flag;
  //    char Length[15];
  long double Length_f;  // Floating point format version of the weird ASCII 'EL_###' length string above.
  long double North;
  long double East;
  long double Height;
  int Gains[24];
  float BFTemps;
  int Delays[16];
  //    int VCSOrder;
  char Flavors[11];

  uint16_t rf_input;  // What's the tile & pol identifier we'll see in the udp packets for this input?

  int16_t ws_delay;  // The whole sample delay IN SAMPLES (NOT BYTES). Can be -ve. Each extra delay will move the starting position later in the sample sequence
  double initial_delay;
  double delta_delay;
  double delta_delta_delay;
  double start_total_delay;
  double middle_total_delay;
  double end_total_delay;

} tile_meta_t;

/*typedef struct block_0_tile_metadata {   // Structure format for the metadata line for each tile stored in the first block of the sub file.
    uint16_t rf_input;                     // What's the tile & pol identifier we'll see in the udp packets for this input?
    int16_t ws_delay;                      // The whole sample delay for this tile, for this subobservation?  Will often be negative!
    int32_t initial_delay;
    int32_t delta_delay;
    int32_t delta_delta_delay;
    int16_t num_pointings;                 // Initially 1, but might grow to 10 or more if beamforming.  The first pointing is for delay tracking in the correlator.
    int16_t frac_delay[POINTINGS_PER_SUB]; // 1601 Fractional delays in units of 1000th of a whole sample time.
                                           // Not supposed to be out of range of -2000 to 2000 millisamples (inclusive).

} block_0_tile_metadata_t;*/

// typedef struct delay_table_entry {      // Structure format for the metadata line for each tile stored in the first block of the sub file.
//     uint16_t rf_input;                  // What's the tile & pol identifier we'll see in the udp packets for this input?
//     int16_t ws_delay;                   // The whole sample delay for this tile, for this subobservation?  Will often be negative!
//     int32_t initial_delay;
//     int32_t delta_delay;
//     int32_t delta_delta_delay;
//
//     int16_t num_pointings;                 // Initially 1, but might grow to 10 or more if beamforming.  The 1st pointing is for delay tracking in the correlator.
//     int32_t frac_delay[POINTINGS_PER_SUB]; // 1601 Fractional delays in units of a millionth of a whole sample time.
//                                            // Not supposed to be out of range of -2000 to 2000 millisamples (inclusive).
// } delay_table_entry_t;
#pragma pack(push, 1)
// structure for each signal path
typedef struct delay_table_entry {
  uint16_t rf_input;
  int16_t ws_delay_applied;  // The whole-sample delay for this signal path, for the entire sub-observation.  Will often be negative.
  double start_total_delay;
  double middle_total_delay;
  double end_total_delay;
  double initial_delay;      // Initial residual delay at centre of first 5 ms timestep
  double delta_delay;        // Increment between timesteps
  double delta_delta_delay;  // Increment in the increment between timesteps
  int16_t num_pointings;     // Initially 1 but might grow to 10 or more if beamforming.  The first pointing is for delay tracking in the correlator.
  int16_t reserved;
  float frac_delay[POINTINGS_PER_SUB];  // 1600 fractional delays in fractions of a whole sample time.  Not supposed to be out of range of -1 to 1 samples (inclusive).
} delay_table_entry_t;
#pragma pack(pop)

typedef struct subobs_udp_meta {  // Structure format for the MWA subobservation metadata that tracks the sorted location of the udp packets

  volatile uint32_t subobs;  // The sub observation number.  ie the GPS time of the first second in this sub-observation

  int msec_wait;  // The number of milliseconds the sub write thread had waited doing nothing before starting to write out this sub.  Only valid for state = 3 or 4 or 5 or higher
  int msec_took;  // The number of milliseconds the sub write thread took to write out this sub.  Only valid for state = 4 or 5 or higher

  int64_t first_udp;
  int64_t last_udp;
  int64_t udp_at_start_write;
  int64_t udp_at_end_write;

  int udp_count;  // The number of udp packets collected from the NIC
  int udp_dummy;  // The number of dummy packets we needed to insert to pad things out

  int meta_msec_wait;  // The number of milliseconds it took to from reading the last metafits to finding a new one
  int meta_msec_took;  // The number of milliseconds it took to read the metafits

  int64_t GPSTIME;  // Following fields are straight from the metafits file (via the metabin) This is the GPSTIME *FOR THE OBS* not the subobs!!!
  int EXPOSURE;
  char FILENAME[300];
  int CABLEDEL;
  int GEODEL;
  int CALIBDEL;
  int DERIPPLE;
  char PROJECT[32];
  char MODE[32];

  int CHANNELS[24];
  float FINECHAN;
  float INTTIME;

  int NINPUTS;
  int64_t UNIXTIME;

  int COARSE_CHAN;
  int FINECHAN_hz;
  int INTTIME_msec;

  uint16_t rf_seen;        // The number of different rf_input sources seen so far this sub observation
  uint16_t rf2ndx[65536];  // A mapping from the rf_input value to what row in the pointer array its pointers are stored
  char ***udp_volts;       // array of arrays of pointers to every udp packet's payload that may be needed for this sub-observation.
                           // NB: THIS ARRAY IS IN THE ORDER INPUTS WERE SEEN STARTING AT 1!, NOT THE SUB FILE ORDER!
                           // also note, entry 0 should never be dereferenced.

  tile_meta_t rf_inp[MAX_INPUTS];  // Metadata about each rf input in an array indexed by the order the input needs to be in the output sub file,
                                   // NOT the order udp packets were seen in.

  altaz_meta_t altaz[3];  // The AltAz at the beginning, middle and end of the 8 second sub-observation

} subobs_udp_meta_t;

typedef struct MandC_meta {  // Structure format for the MWA subobservation metadata that tracks the sorted location of the udp packets.  Array with one entry per rf_input

  uint16_t rf_input;    // tile/antenna and polarisation (LSB is 0 for X, 1 for Y)
  int start_byte;       // What byte (not sample) to start at.  Remember we store a whole spare packet before the real data starts
  uint16_t seen_order;  // For this subobs (only). What order was this rf input seen in?  That tells us where we need to look in the udp_volts pointer array

} MandC_meta_t;

// used to track sizes of sections in block0 of subfiles.
typedef struct data_section {
  char *name;
  int offset;
  int length;
} data_section;
//---------------------------------------------------------------------------------------------------------------------------------------------------
// These variables are shared by all threads.  "file scope"
//---------------------------------------------------------------------------------------------------------------------------------------------------

volatile bool terminate         = false;  // Global request for everyone to close down
volatile bool UDP_recv_complete = false;  // UDP_recv has closed down successfully.

volatile int64_t UDP_added_to_buff     = 0;  // Total number of packets received and placed in the buffer.  If it overflows we are in trouble and need a restart.
volatile int64_t UDP_removed_from_buff = 0;  // Total number of packets pulled from buffer and space freed up.  If it overflows we are in trouble and need a restart.

int64_t UDP_num_slots;            // Must be at least 3000000 for mwax07 with 128T. 3000000 will barely make it!
uint32_t GPS_offset = 315964782;  // Logging only.  Needs to be updated on leap seconds

struct mmsghdr *msgvecs;
struct iovec *iovecs;
mwa_udp_packet_t *UDPbuf;
udp2sub_monitor_t monitor;

atomic_int slot_state[4] = {0};  // 0: free, 1: collecting packets, 2: ready to write, 3: write in progress, 4/5: write succeeded/failed, 6: marked for abandonment
atomic_int meta_state[4] = {0};  // 0: free, 1: metafits read requested, 2: metafits read in progress, 4/5: metafits read succeeded/failed

subobs_udp_meta_t *sub;  // Pointer to the four subobs metadata arrays
char *sub_header;        // Pointer to a buffer that's the size of a sub file header.

bool debug_mode         = false;  // Default to not being in debug mode
bool force_cable_delays = false;  // Always apply cable delays, regardless of metafits
bool force_geo_delays   = false;  // Always apply geometric delays, regardless of metafits

//---------------------------------------------------------------------------------------------------------------------------------------------------
// read_config - use our hostname and a command line parameter to find ourselves in the list of possible configurations
// Populate a single structure with the relevant information and return it
//---------------------------------------------------------------------------------------------------------------------------------------------------

// #pragma pack(push,1)                          // We're writing this header into all our packets, so we want/need to force the compiler not to add it's own idea of structure
// padding

typedef struct udp2sub_config {  // Structure for the configuration of each udp2sub instance.

  // fields for this instance read from a line in the mwax_u2s config file, usually found at /vulcan/mwax_config/mwax_u2s.cfg
  int udp2sub_id;     // Which correlator id number we are.  Probably from 01 to 26, at least initially.
  char hostname[64];  // Host name is looked up against these strings to select the correct line of configuration settings
  int host_instance;  // Is compared with a value that can be put on the command line for multiple copies per server

  int64_t UDP_num_slots;  // The number of UDP buffers to assign.  Must be ~>=3000000 for 128T array

  unsigned int cpu_mask_parent;     // Allowed cpus for the parent thread which reads metafits file
  unsigned int cpu_mask_UDP_recv;   // Allowed cpus for the thread that needs to pull data out of the NIC super fast
  unsigned int cpu_mask_UDP_parse;  // Allowed cpus for the thread that checks udp packets to create backwards pointer lists
  unsigned int cpu_mask_makesub;    // Allowed cpus for the thread that writes the sub files out to memory

  char shared_mem_dir[40];   // The name of the shared memory directory where we get .free files from and write out .sub files
  char temp_file_name[40];   // The name of the temporary file we use.  If multiple copies are running on each server, these may need to be different
  char stats_file_name[40];  // The name of the shared memory file we use to write statistics and debugging to.  Again if multiple copies per server, it may need different names
  char spare_str[40];        //
  char metafits_dir[60];     // The directory where metafits files are looked for.  NB Likely to be on an NFS mount such as /vulcan/metafits

  char local_if[20];      // Address of the local NIC interface we want to receive the multicast stream on.
  int coarse_chan;        // Which coarse chan from 01 to 24.
  char multicast_ip[30];  // The multicast address and port (below) we wish to join.
  int UDPport;            // Multicast port address
  char monitor_if[20];    // Local interface address for monitoring packets

  // fields common to all u2s instances read from mwax.cfg, usually found at /vulcan/mwax_config/mwax.cfg
  int tiles;
  int xgpu_tiles;
  int oversampling;
} udp2sub_config_t;

// #pragma pack(pop)                               // Set the structure packing back to 'normal' whatever that is

udp2sub_config_t conf;  // A place to store the configuration data for this instance of the program.  ie of the 60 copies running on 10 computers or whatever

bool isround(uint64_t x) {  // returns true for numbers whose decimal representation matches [0123468]0*
  while (x > 10) {
    if (x % 10 > 0) {
      return false;
    }
    x = x / 10;
  }
  return (x < 5 || !(x & 1));
}

void report_substatus(char *thread_name, char *status, ...);
void report_substatus(char *thread_name, char *status, ...) {
  static char *last_status     = NULL;
  static uint64_t repeat_count = 0;
  if (status != last_status) {
    last_status  = status;
    repeat_count = 0;
  }
  repeat_count++;

  if (isround(repeat_count)) {
    struct timespec this_time;
    clock_gettime(CLOCK_REALTIME, &this_time);
    printf("now=%ld.%03d %-15s: ", (int64_t)(this_time.tv_sec - GPS_offset), (int)(this_time.tv_nsec / 1000000), thread_name);
    printf("(slot.meta)_state = [%d.%d %d.%d %d.%d %d.%d] ", slot_state[0], meta_state[0], slot_state[1], meta_state[1], slot_state[2], meta_state[2], slot_state[3],
           meta_state[3]);
    va_list arguments;
    va_start(arguments, status);
    vprintf(status, arguments);
    va_end(arguments);
    if (repeat_count > 5) {
      printf("(%lu repeats)", repeat_count);
    }
    printf("\n");
    fflush(stdout);
  }
}

//---------------------------------------------------------------------------------------------------------------------------------------------------
// load_channel_map - Load config from a CSV file.
//---------------------------------------------------------------------------------------------------------------------------------------------------

int load_and_cr_terminate(const char *path, char **data) {
  FILE *file;
  size_t sz;
  file = fopen(path, "r");
  if (file == NULL) {
    fprintf(stderr, "Error loading configuration. Unable to open %s\n", path);
    return 1;
  }
  fseek(file, 0, SEEK_END);
  sz = ftell(file);
  rewind(file);
  if (sz == -1) {
    fprintf(stderr, "Error loading configuration. Failed to determine file size for %s\n", path);
    return 2;
  }
  (*data)     = malloc(sz + 1);
  (*data)[sz] = '\n';  // Simplifies parsing slightly
  if (fread((*data), 1, sz, file) != sz) {
    fprintf(stderr, "Error loading configuration. Read error loading %s\n", path);
    return 3;
  }
  fclose(file);
  return 0;
}

int load_config_file(char *path, udp2sub_config_t **config_records) {
  fprintf(stderr, "Reading configuration from %s\n", path);
  // Read the whole input file into a buffer.
  char *data;
  int err = load_and_cr_terminate(path, &data);
  if (err > 0) {
    return err;
  }

  char *datap = data;
  udp2sub_config_t *records;
  int max_rows = strlen(datap) / 28;        // Minimum row size is 28, pre-allocate enough
  int row_sz   = sizeof(udp2sub_config_t);  // room for the worst case and `realloc` later
  records      = calloc(max_rows, row_sz);  // when the size is known.

  int row = 0, col = 0;             // Current row and column in input table
  char *sep = ",";                  // Next expected separator
  char *end = NULL;                 // Mark where `strtol` stops parsing
  char *tok = strsep(&datap, sep);  // Pointer to start of next value in input
  while (row < max_rows) {
    while (col < 18) {
      switch (col) {
        case 0:
          records[row].udp2sub_id = strtol(tok, &end, 10);  // Parse the current token as a number,
          if (end == NULL || *end != '\0') goto done;       // consuming the whole token, or abort.
          break;
        case 1:
          strcpy(records[row].hostname, tok);
          break;
        case 2:
          records[row].host_instance = strtol(tok, &end, 10);
          if (end == NULL || *end != '\0') goto done;
          break;
        case 3:
          records[row].UDP_num_slots = strtol(tok, &end, 10);  // buffer size in bytes
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
          records[row].coarse_chan = strtol(tok, &end, 10);  /// channel index, is mapped to sky frequency number by the metafits
          if (end == NULL || *end != '\0') goto done;
          break;
        case 15:
          strcpy(records[row].multicast_ip, tok);
          break;
        case 16:
          records[row].UDPport = strtol(tok, &end, 10);
          if (end == NULL || *end != '\0') goto done;
          break;
        case 17:
          strcpy(records[row].monitor_if, tok);
          break;
      }

      if (col == 16)       // If we've parsed the second-to-last column,
        sep = "\n";        // the next separator to expect will be LF.
      else if (col == 17)  // If we've parsed the last column, the next
        sep = ",";         // separator will be a comma again.
      col++;

      tok = strsep(&datap, sep);   // Get the next token from the input and
      if (tok == NULL) goto done;  // abort the row if we don't find one.
    }
    if (col == 18)
      col = 0;  // Wrap to column 0 if we parsed a full row
    else
      break;  // or abort if we didn't.
    row++;
  }
done:
  *config_records = realloc(records, (row + 1) * sizeof(udp2sub_config_t));
  free(data);
  fprintf(stderr, "%d instance record(s) found.\n", row + 1);
  return row + 1;
}

int load_mwax_config(char *path, udp2sub_config_t *cfg) {
  fprintf(stderr, "Reading configuration from %s\n", path);

  char *data;
  int err = load_and_cr_terminate(path, &data);
  if (err > 0) {
    return err;
  }
  char *datap         = data;
  int section_is_mwax = 0;
  while (datap) {
    char *line = strsep(&datap, "\n");
    char *sep;
    if ((sep = strstr(line, "="))) {
      char *name  = line;
      char *value = sep + 1;
      *sep--      = 0;
      while (*sep == ' ' && sep > name) *sep-- = 0;  // trim trailing spaces from name

      if (section_is_mwax) {
        if (!strcmp(name, "tiles")) {
          cfg->tiles = atoi(value);
        }
        if (!strcmp(name, "xgpu_tiles")) {
          cfg->xgpu_tiles = atoi(value);
        }
        if (!strcmp(name, "oversampling")) {
          cfg->oversampling = atoi(value);
        }
      }
    } else if ((sep = strstr(line, "["))) {
      sep++;
      char *section = strsep(&sep, "]");
      if (sep) {
        // section found.
        section_is_mwax = !strcmp(section, "mwax");
      }
    }
  }
  free(data);
  return 0;
}

void read_config(char *file, char *shared_file, char *us, int inst, int coarse_chan, udp2sub_config_t *config) {
  int num_instances = 0;  // Number of instance records loaded
  int instance_ndx  = 0;  // Start out assuming we don't appear in the list

  udp2sub_config_t *available_config = NULL;
  num_instances                      = load_config_file(file, &available_config);

  if (available_config == NULL) {
    fprintf(stderr, "Failed to load instance configuration data.\n");
    exit(1);
  }

  for (int loop = 0; loop < num_instances; loop++) {  // Check through all possible configurations
    if ((strcmp(available_config[loop].hostname, us) == 0) && (available_config[loop].host_instance == inst)) {
      instance_ndx = loop;  // if the edt card config matches the command line and the hostname matches
      break;                // We don't need to keep looking
    }
  }

  *config = available_config[instance_ndx];  // Copy the relevant line into the structure we were passed a pointer to
  load_mwax_config(shared_file, config);

  if (coarse_chan > 0) {                                          // If there is a coarse channel override on the command line
    config->coarse_chan = coarse_chan;                            // Force the coarse chan from 01 to 24 with that value
    sprintf(config->multicast_ip, "239.255.90.%d", coarse_chan);  // Multicast ip is 239.255.90.xx
    config->UDPport = 59000 + coarse_chan;                        // Multicast port address is the forced coarse channel number plus an offset of 59000
  }

  monitor.coarse_chan = config->coarse_chan;
  memcpy(monitor.hostname, config->hostname, HOSTNAME_LENGTH);
  monitor.instance = config->udp2sub_id;
}

//===================================================================================================================================================

// ------------------------ Function to set CPU affinity so that we can control which socket & core we run on -------------------------

int set_cpu_affinity(unsigned int mask) {
  cpu_set_t my_cpu_set;   // Make a CPU affinity set for this thread
  CPU_ZERO(&my_cpu_set);  // Zero it out completely

  for (int loop = 0; loop < 32; loop++) {  // Support up to 32 cores for now.  We need to step through them from LSB to MSB

    if (((mask >> loop) & 0x01) == 0x01) {  // If that bit is set
      CPU_SET(loop, &my_cpu_set);           // then add it to the allowed list
    }
  }

  pthread_t my_thread;         // I need to look up my own TID / LWP
  my_thread = pthread_self();  // So what's my TID / LWP?

  if (conf.udp2sub_id > 26) {                                                    // Don't do any mwax servers for now
    return (pthread_setaffinity_np(my_thread, sizeof(cpu_set_t), &my_cpu_set));  // Make the call and return the result back to the caller
  } else {
    return (-1);
  }
}

//===================================================================================================================================================
// THREAD BLOCK.  The following functions are complete threads
//===================================================================================================================================================

//---------------------------------------------------------------------------------------------------------------------------------------------------
// UDP_recv - Pull UDP packets out of the kernel and place them in a large user-space circular buffer
//---------------------------------------------------------------------------------------------------------------------------------------------------

void *UDP_recv() {
  printf("UDP_recv started\n");
  fflush(stdout);

  //--------------- Set CPU affinity ---------------

  printf("Set process UDP_recv cpu affinity returned %d\n", set_cpu_affinity(conf.cpu_mask_UDP_recv));
  fflush(stdout);

  //---------------- Initialize and declare variables ------------------------

  int64_t UDP_slots_empty;  // How much room is left unused in the application's UDP receive buffer
  int64_t UDP_first_empty;  // Index to the first empty slot 0 to (UDP_num_slots-1)

  int64_t UDP_slots_empty_min = UDP_num_slots + 1;  // what's the smallest number of empty slots we've seen this batch?  (Set an initial value that will always be beaten)

  int64_t Num_loops_when_full = 0;  // How many times (since program start) have we checked if there was room in the buffer and there wasn't any left  :-(

  int retval;  // General return value variable.  Context dependant.

  //--------------------------------

  printf("Set up to receive from multicast %s:%d on interface %s\n", conf.multicast_ip, conf.UDPport, conf.local_if);
  fflush(stdout);

  struct sockaddr_in addr;  // Standard socket setup stuff needed even for multicast UDP
  memset(&addr, 0, sizeof(addr));

  int fd;
  struct ip_mreq mreq;

  if ((fd = socket(AF_INET, SOCK_DGRAM, 0)) == -1) {  // create what looks like an ordinary UDP socket
    perror("socket");
    terminate = true;
    pthread_exit(NULL);
  }

  addr.sin_family      = AF_INET;
  addr.sin_addr.s_addr = htonl(INADDR_ANY);
  addr.sin_port        = htons(conf.UDPport);

  if (setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &(int){1}, sizeof(int)) == -1) {
    perror("setsockopt SO_REUSEADDR");
    close(fd);
    terminate = true;
    pthread_exit(NULL);
  }

  if (bind(fd, (struct sockaddr *)&addr, sizeof(addr)) == -1) {  // bind to the receive address
    perror("bind");
    close(fd);
    terminate = true;
    pthread_exit(NULL);
  }

  mreq.imr_multiaddr.s_addr = inet_addr(conf.multicast_ip);  // use setsockopt() to request that the kernel join a multicast group
  mreq.imr_interface.s_addr = inet_addr(conf.local_if);

  if (setsockopt(fd, IPPROTO_IP, IP_ADD_MEMBERSHIP, &mreq, sizeof(mreq)) == -1) {
    perror("setsockopt IP_ADD_MEMBERSHIP");
    close(fd);
    terminate = true;
    pthread_exit(NULL);
  }

  //--------------------------------------------------------------------------------------------------------------------------------------------------------

  printf("Ready to start\n");
  fflush(stdout);

  UDP_slots_empty                     = UDP_num_slots;              // We haven't written to any yet, so all slots are available at the moment
  UDP_first_empty                     = 0;                          // The first of those is number zero
  struct mmsghdr *UDP_first_empty_ptr = &msgvecs[UDP_first_empty];  // Set our first empty pointer to the address of the 0th element in the array

  // Note that msgvecs contains 2*UDP_num_slots entries, with the second half a duplicate of the first,
  // so when we pass recvmmsg a pointer to somewhere in the first half it will wrap the destinations automagically.

  while (!terminate) {
    if (UDP_slots_empty > 0) {  // There's room for at least 1 UDP packet to arrive!  We should go ask the OS for it.

      // on some runs where we were collecting around 140k packets per second (129 tiles, critically sampled)
      //
      // 88% of the calls to recvmmsg (64% of the data), we got a single packet back
      // 99% of the calls to recvmmsg we got fewer than 16packets, 99.99% of the time fewer than 70.
      // Occasionally we got 300+
      //
      // 65.00% of the data returned by recvmmsg in single packets
      // 78.00% of the data returned by recvmmsg in groups of fewer than   3 packets
      // 90.00% of the data returned by recvmmsg in groups of fewer than   7 packets
      // 99.00% of the data returned by recvmmsg in groups of fewer than  44 packets
      // 99.90% of the data returned by recvmmsg in groups of fewer than  88 packets
      // 99.99% of the data returned by recvmmsg in groups of fewer than 128 packets

      if ((retval = recvmmsg(fd, UDP_first_empty_ptr, UDP_slots_empty, RECVMMSG_MODE, NULL)) == -1) continue;

      UDP_added_to_buff += retval;  // Add that to the number we've ever seen and placed in the buffer

    } else {
      Num_loops_when_full++;
      usleep(1000);  // we should chill for a moment rather than take 100% CPU waiting on someone else to consume packets
    }

    UDP_slots_empty = UDP_num_slots + UDP_removed_from_buff - UDP_added_to_buff;  // How many UDP slots are available for us to (ask to) read using the one recvmmsg() request?

    if (UDP_slots_empty < UDP_slots_empty_min)
      UDP_slots_empty_min = UDP_slots_empty;  // Do we have a new winner of the "What's the minimum number of empty slots available" prize?  Debug use only.  Not needed

    UDP_first_empty     = (UDP_added_to_buff % UDP_num_slots);  // The index from 0 to (UDP_num_slots-1) of the first available UDP packet slot in the buffer
    UDP_first_empty_ptr = &msgvecs[UDP_first_empty];
  }

  //  sleep(1);
  printf("looped on full %ld times.  min = %ld\n", Num_loops_when_full, UDP_slots_empty_min);
  fflush(stdout);

  mreq.imr_multiaddr.s_addr = inet_addr(conf.multicast_ip);
  mreq.imr_interface.s_addr = inet_addr(conf.local_if);

  if (setsockopt(fd, IPPROTO_IP, IP_DROP_MEMBERSHIP, &mreq, sizeof(mreq)) == -1) {  // Say we don't want multicast packets any more.
    perror("setsockopt IP_DROP_MEMBERSHIP");
    close(fd);
    pthread_exit(NULL);
  }

  close(fd);  // Close the file descriptor for the port now we're about the leave

  printf("Exiting UDP_recv\n");
  fflush(stdout);
  UDP_recv_complete = true;
  pthread_exit(NULL);
}

void clear_slot(int slot) {
  memset(sub[slot].udp_volts[1], 0, MAX_INPUTS * UDP_PER_RF_PER_SUB * sizeof(char *));

  char ***voltage_save = sub[slot].udp_volts;
  memset(&sub[slot], 0, sizeof(subobs_udp_meta_t));
  sub[slot].udp_volts = voltage_save;
}

//------------------------------------------------------------------------------------------------------------------------------------------------------
// UDP_parse - Check UDP packets as they arrive and update the array of pointers to them so we can later address them in sorted order
//------------------------------------------------------------------------------------------------------------------------------------------------------

void *UDP_parse() {
  printf("UDP_parse started\n");
  fflush(stdout);

  //--------------- Set CPU affinity ---------------

  printf("Set process UDP_parse cpu affinity returned %d\n", set_cpu_affinity(conf.cpu_mask_UDP_parse));
  fflush(stdout);

  //---------------- Initialize and declare variables ------------------------

  uint32_t last_good_packet_sub_time = 0;           // What sub obs was the last packet (for caching)
  uint32_t subobs_mask               = 0xFFFFFFF8;  // Mask to apply with '&' to get sub obs from GPS second

  // time is divided in to 8 second 'slots' (one per subobservation), referenced by their first second in GPS time
  // these variables track the range of slots we are currently accepting packets for
  uint32_t start_window = 0;  // the oldest subobservation we're accepting packets for.
  uint32_t end_window   = 0;  // the newest subobservation page of current window (equal to old "end_window"-7).  Recalc window when we receive a packet for a later page.

  int slot_index;  // index into which of the 4 subobs metadata blocks we want
  int rf_ndx;      // index into the position in the meta array we are using for this rf input (for this sub).  NB May be different for the same rf on a different sub.

  subobs_udp_meta_t *this_sub = NULL;  // Pointer to the relevant one of the four subobs metadata arrays

  int expected_packet_type = conf.oversampling ? MWA_PACKET_TYPE_OVERSAMPLING : MWA_PACKET_TYPE_LEGACY;

  //---------------- Main loop to process incoming udp packets -------------------

  mwa_udp_packet_t *last_translated = NULL;
  while (!terminate) {
    if (UDP_removed_from_buff < UDP_added_to_buff) {            // If there is at least one packet waiting to be processed
      mwa_udp_packet_t *my_udp;                                 // Make a local pointer to the UDP packet we're working on
      my_udp = &UDPbuf[UDP_removed_from_buff % UDP_num_slots];  // and point to it.

      //---------- At this point in the loop, we are about to process the next arrived UDP packet, and it is located at my_udp ----------

      if (my_udp->packet_type == expected_packet_type) {     // If it's a real packet off the network with some fields in network order, they need to be converted to host order and
                                                             // have a few tweaks made before we can use the packet
        my_udp->subsec_time   = ntohs(my_udp->subsec_time);  // Convert subsec_time to a usable uint16_t which at this stage is still within a single second
        my_udp->GPS_time      = ntohl(my_udp->GPS_time);     // Convert GPS_time (bottom 32 bits only) to a usable uint32_t
        my_udp->rf_input      = ntohs(my_udp->rf_input);     // Convert rf_input to a usable uint16_t
        my_udp->edt2udp_token = ntohs(my_udp->edt2udp_token);  // Convert edt2udp_token to a usable uint16_t
        last_translated       = my_udp;
        // the bottom three bits of the GPS_time is 'which second within the subobservation' this second is
        my_udp->subsec_time += ((my_udp->GPS_time & 0b111) * SUBSECSPERSEC) + 1;  // Change the subsec field to be the packet count within a whole subobs, not just this second. was
                                                                                  // 0 to 624.  now 1 to 5000 inclusive. (0x21 packets can be 0 to 5001!)

        my_udp->GPS_time &= subobs_mask;
        my_udp->packet_type = 0x21;  // Now change the packet type to a 0x21 to say it's similar to a 0x20 but in host byte order and with a subobs timestamp
      }

      uint32_t now = 0;
      {
        struct timespec this_time;
        clock_gettime(CLOCK_REALTIME, &this_time);
        now = (this_time.tv_sec - GPS_offset);
      }

      if ((my_udp->packet_type != 0x21) ||  // wrong packet type
          (my_udp->GPS_time < now - 32) ||  // packet almost certainly has a bad timestamp
          (my_udp->GPS_time > now + 9)      // Note that this validly be for the near future if we're writing a margin packet into next subobs
      ) {                                   // This packet is not a valid packet for us.

        if (my_udp != last_translated) {                         // we haven't translated to network order. Do this before we log the contents.
          my_udp->subsec_time   = ntohs(my_udp->subsec_time);    // Convert subsec_time to a usable uint16_t which at this stage is still within a single second
          my_udp->GPS_time      = ntohl(my_udp->GPS_time);       // Convert GPS_time (bottom 32 bits only) to a usable uint32_t
          my_udp->rf_input      = ntohs(my_udp->rf_input);       // Convert rf_input to a usable uint16_t
          my_udp->edt2udp_token = ntohs(my_udp->edt2udp_token);  // Convert edt2udp_token to a usable uint16_t
        }

        report_substatus("UDP_parse", "rejecting packet (packet_type=0x%02x, GPS_time=%d, (now=%d), rf_input=%d, edt2udp_token=0x%04x",  //
                         my_udp->packet_type, my_udp->GPS_time, now, my_udp->rf_input, my_udp->edt2udp_token);
        UDP_removed_from_buff++;  // Flag it as used and release the buffer slot.  We don't want to see it again.
        continue;                 // start the loop again
      }

      //---------- If this packet is for the same sub obs as the previous packet, we can assume a bunch of things haven't changed or rolled over, otherwise we have things to check
      if (my_udp->GPS_time != last_good_packet_sub_time) {  // If this is a different sub obs than the last packet we allowed through to be processed.
        //---------- Arrived too late to be usable?
        if (my_udp->GPS_time < start_window) {  // This packet has a time stamp before the earliest open sub-observation
          UDP_removed_from_buff++;              // Throw it away.  ie flag it as used and release the buffer slot.  We don't want to see it again.
          continue;                             // start the loop again
        }

        // TODO - continue updating window even if we don't get any packets for a few seconds.
        //---------- Further ahead in time than we're ready for?
        if (my_udp->GPS_time > end_window) {
          // This packet has a time stamp after the end of the latest open sub-observation.  We need to do some preparatory work before we can process this packet.

          // First we need to know if this is simply the next chronological subobs, or if we have skipped ahead.
          // NB that a closedown packet will look like we skipped ahead to 2106.
          // If we've skipped ahead then all current subobs need to be closed.
          uint32_t start_old = start_window;
          uint32_t end_old   = end_window;
          if (my_udp->GPS_time == end_window + 8) {  // just moving up one subobservation, keep this one and the previous one open.
            start_window = end_window;
            end_window   = my_udp->GPS_time;
            report_substatus("UDP_parse", "window adjusted from %d-%d to %d-%d (moving end up by one subobs).", start_old, end_old, start_window, end_window);
          } else {  // otherwise the packet stream is so far into the future that we need to close *all* open subobs.
            start_window = end_window = my_udp->GPS_time;
            report_substatus("UDP_parse", "window adjusted from %d-%d to %d-%d (single subobservation).", start_old, end_old, start_window, end_window);
          }

          for (int loop = 0; loop < SUB_SLOTS; loop++) {  // check all subobs meta slots. If they're too old we'll rule them off. NB this may not be checked in time order of subobs
            if ((sub[loop].subobs < start_window) && (slot_state[loop] == 1)) {
              // If this sub obs slot is currently in use by us (ie state==1) and has now reached its timeout ( < start_window )
              if (sub[loop].subobs == (start_window - 8)) {  // then if it's a very recent subobs (which is what we'd expect during normal operations)
                slot_state[loop] = 2;                        // set the state flag to tell another thread it's their job to write out this subobs and pass the data on down the line
                report_substatus("UDP_parse", "subobs %d slot %d. Requesting write (setting state to 2).", sub[loop].subobs, loop);
              } else {                       // or if it isn't recent then it's probably because the receivers (or medconv array) went away so we want to abandon it
                slot_state[loop] = 6;        // set the state flag to indicate that this subobs should be abandoned as too old to be useful
                monitor.discarded_subobs++;  // note that this is just a request - the slot isn't really free until we've also finished reading the metafits
                report_substatus("UDP_parse", "subobs %d slot %d. Abandoning (setting state to 6).", sub[loop].subobs, loop);
              }
            }
          }
        }

        // We now have a new subobs that we need to set up for.  Hopefully the slot we want to use is either empty or finished with and free for reuse.  If not we've overrun the
        // sub writing threads
        slot_index = (my_udp->GPS_time >> 3) & 0b11;  // The 3 LSB are 'what second in the subobs' we are.  We want the 2 bits immediately to the left of that.  They will give us
        // our index into the subobs metadata array
        if (slot_state[slot_index] == 0) {
          // If the state is a 0, then it's free for reuse,
          report_substatus("UDP_parse", "subobs %d slot %d. First new packet", my_udp->GPS_time, slot_index);

          sub[slot_index].subobs    = my_udp->GPS_time;       // We've already cleared the low three bits.
          sub[slot_index].first_udp = UDP_removed_from_buff;  // This was the first udp packet seen for this sub. (0 based)
          slot_state[slot_index]    = 1;                      // Let's remember we're using this slot now and tell other threads.
          meta_state[slot_index]    = 1;                      // request metafits read
          // NB: The subobs field must be populated *before* these become 1
        }

        //---------- This packet isn't similar enough to previous ones (ie from the same sub-obs) to assume things, so let's get new pointers
        if (slot_state[slot_index] == 1) {
          this_sub = &sub[slot_index];  // The 3 LSB are 'what second in the subobs' we are.  We want the 2 bits left of that.  They will give us an index into
          // the subobs metadata array which we use to get the pointer to the struct
          last_good_packet_sub_time = my_udp->GPS_time;  // Remember this so next packet we probably don't need to do these checks and lookups again

        } else {
          // TODO - report this condition in health packet.
          // Note that it will already show up as increased packet loss though, so priority on additional reporting is not high.
          report_substatus("UDP_parse", "subobs %d slot %d. Packet received but slot not available", my_udp->GPS_time, slot_index);
          this_sub = NULL;
          // the subobs metadata array which we use to get the pointer to the struct
          last_good_packet_sub_time = -1;
        }
      }

      if (this_sub) {
        //---------- We have a udp packet to add and a place for it to go.  Time to add its details to the sub obs metadata
        rf_ndx = this_sub->rf2ndx[my_udp->rf_input];               // Look up the position in the meta array we are using for this rf input (for this sub).
        if (rf_ndx == 0) {                                         // this is the first time we've seen one this sub from this rf input
          this_sub->rf_seen++;                                     // Increase the number of different rf inputs seen so far.  START AT 1, NOT 0!
          this_sub->rf2ndx[my_udp->rf_input] = this_sub->rf_seen;  // and assign that number for this rf input's metadata index
          rf_ndx                             = this_sub->rf_seen;  // and get the correct index for this packet too because we'll need that
        }  // TODO Should check for array overrun.  ie that rf_seen has grown past MAX_INPUTS (or is it plus or minus one?)

        sub[slot_index].udp_volts[rf_ndx][my_udp->subsec_time] = (char *)my_udp->volts;  // This is an important line so lets unpack what it does and why.
        // The 'this_sub' struct stores a 2D array of pointers (udp_volt) to all the udp payloads that apply to that sub obs.
        // The dimensions are rf_input (sorted by the order in which they were seen on the incoming packet stream) and the packet count (0 to 5001) inside the subobs.
        // By this stage, seconds and subsecs have been merged into a single number so subsec time is already in the range 0 to 5001

        this_sub->last_udp = UDP_removed_from_buff;  // The last udp packet seen so far for this sub. (0 based) Eventually it won't be updated and the final value will remain
        this_sub->udp_count++;                       // Add one to the number of udp packets seen this sub obs.
                                                     // We rarely get all ninputs*5000 packets for a given sub obs, so even if
                                                     // we didn't count duplicates we still couldn't use this to check if we've finished a sub.
        monitor.udp_count++;
      }

      //---------- We are done with this packet, EXCEPT if this was the very first for an rf_input for a subobs, or the very last, then we want to duplicate them in the adjacent
      // subobs.
      //---------- This is because one packet may contain data that goes in two subobs (or even separate obs!) due to delay tracking
      if (my_udp->subsec_time == 1) {                     // If this was the first packet (for an rf input) for a subobs
        my_udp->subsec_time = SUBSECSPERSUB + 1;          // then say it's at the end of a subobs
        my_udp->GPS_time -= 8;                            // and move it into the previous subobs
      } else if (my_udp->subsec_time == SUBSECSPERSUB) {  // If this was the last packet (for an rf input) for a subobs
        my_udp->subsec_time = 0;                          // then say it's at the start of a subobs
        my_udp->GPS_time += 8;                            // and move it into the next subobs
      } else {
        UDP_removed_from_buff++;  // We don't need to duplicate this packet, so incr the number of packets we've ever processed (which releases the packet from the buffer).
      }

      //---------- End of processing of this UDP packet ----------

    } else {          //
      usleep(10000);  // Chill for a bit
    }
  }

  //---------- We've been told to shut down ----------

  printf("Exiting UDP_parse\n");
  fflush(stdout);
  pthread_exit(NULL);
}

long double get_path_difference(long double north, long double east, long double height, long double alt, long double az) {
  vec3_t A        = (vec3_t){north, east, height};
  vec3_t B        = (vec3_t){0, 0, 0};
  vec3_t AB       = vec3_subtract(B, A);
  vec3_t ACn      = vec3_unit(deg2rad(alt), deg2rad(az));
  long double ACr = vec3_dot(AB, ACn);
  return ACr;
}

int fits_read_key_verbose(fitsfile *fptr, int datatype, const char *keyname, char *verbose_keyname, void *value, char *comm, int *status) {
  // TODO - ensure read_metafits() returns false if any of these fail.
  int res = fits_read_key(fptr, datatype, keyname, value, comm, status);
  if (*status) {
    printf("Failed to read %s\n", keyname);
    fflush(stdout);
  }
  return res;
}

bool read_metafits(const char *metafits_file, subobs_udp_meta_t *subm) {
  // preconditions:
  //     subm->subobs >= subm->GPSTIME (the latter as read from the metafits_file, theoretically should be same as the number in the filename)
  //     conf.coarse_chan > 0
  //     conf.coarse_chan <= 24
  // returns:
  //     false on failure
  //     true on success

  fitsfile *fptr;  // FITS file pointer, defined in fitsio.h
  int status = 0;  // CFITSIO status value MUST be initialized to zero!

  fits_open_file(&fptr, metafits_file, READONLY, &status);
  if (status) return false;

  fits_read_key_verbose(fptr, TLONGLONG, "GPSTIME", NULL, &(subm->GPSTIME), NULL, &status);                    // Read the GPSTIME of the metafits observation
                                                                                                               // (should be same as bcsf_obsid but read anyway)
  fits_read_key_verbose(fptr, TINT, "EXPOSURE", NULL, &(subm->EXPOSURE), NULL, &status);                       // Read the EXPOSURE time from the metafits
  fits_read_key_verbose(fptr, TSTRING, "FILENAME", "observation filename", &(subm->FILENAME), NULL, &status);  // WIP!!! SHould be changed to allow reading more than one line
  fits_read_key_verbose(fptr, TINT, "CABLEDEL", NULL, &(subm->CABLEDEL), NULL, &status);                       // Read the CABLEDEL field.
                                                                                                               // 0=Don't apply. 1=apply only the cable delays.
                                                                                                               // 2=apply cable delays _and_ average beamformer dipole delays.
  if (debug_mode || force_cable_delays) subm->CABLEDEL = 1;

  fits_read_key_verbose(fptr, TINT, "GEODEL", NULL, &(subm->GEODEL), NULL, &status);  // Read the GEODEL field. (0=nothing, 1=zenith, 2=tile-pointing, 3=az/el table tracking)
  if (debug_mode || force_geo_delays) subm->GEODEL = 3;

  fits_read_key_verbose(fptr, TINT, "CALIBDEL", NULL, &(subm->CALIBDEL), NULL, &status);           // Read the CALIBDEL field. (0=Don't apply calibration solutions. 1=Do apply)
  fits_read_key_verbose(fptr, TINT, "DERIPPLE", NULL, &(subm->DERIPPLE), NULL, &status);           // now a required field in metafits.. Later stages need to be more tolerant
  fits_read_key_verbose(fptr, TSTRING, "PROJECT", "project id", &(subm->PROJECT), NULL, &status);  // project id
  fits_read_key_verbose(fptr, TSTRING, "MODE", NULL, &(subm->MODE), NULL, &status);                // observing mode

  if ((subm->GPSTIME + (int64_t)subm->EXPOSURE - 1) < (int64_t)subm->subobs) {  // If the last observation has expired. (-1 because inclusive)
    strcpy(subm->MODE, "NO_CAPTURE");                                           // then change the mode to NO_CAPTURE
  }

  //---------- Parsing the sky frequency (coarse) channel is a whole job in itself! ----------
  {
    int temp_CHANNELS[24];
    char *token;
    char *saveptr;
    fits_read_key_longstr(fptr, "CHANNELS", &saveptr, NULL, &status);
    if (status) {
      printf("Failed to read Channels\n");
      fflush(stdout);
      return false;
    }

    int ch_index = 0;        // Start at channel number zero (of 0 to 23)
    char *ptr    = saveptr;  // Get a temp copy (but only of the pointer. NOT THE STRING!) that we can update as we step though the channels in the csv list

    while ((token = strsep(&ptr, ",")) && (ch_index < 24)) {  // Get a pointer to the next number and assuming there *is* one and we still want more
      temp_CHANNELS[ch_index++] = atoi(token);                // turn it into an int and remember it (although it isn't sorted yet)
    }
    free(saveptr);

    if (ch_index != 24) {
      printf("Did not find 24 channels in metafits file.\n");
      fflush(stdout);
      return false;
    }

    // From the RRI user manual:
    // "1. The DR coarse PFB outputs the 256 channels in a fashion that the first 128 channels appear in sequence
    // followed by the 129 channels and then 256 down to 130 appear. The setfreq is user specific command wherein
    // the user has to enter the preferred 24 channesl in sequence to be transported using the 3 fibers. [ line 14 Appendix- E]"
    // Clear as mud?  Yeah.  I thought so too.

    // So we want to look through for where a channel number is greater than, or equal to 129.  We'll assume they are already sorted by M&C
    int course_swap_index = 24;  // start by assuming there are no channels to swap

    // find the index where the channels are swapped i.e. where 129 exists
    for (int i = 0; i < 24; ++i) {
      if (temp_CHANNELS[i] >= 129) {
        course_swap_index = i;
        break;
      }
    }

    // Now reorder freq array based on the course channel boundary around 129
    for (int i = 0; i < 24; ++i) {
      if (i < course_swap_index) {
        subm->CHANNELS[i] = temp_CHANNELS[i];
      } else {
        subm->CHANNELS[23 - i + (course_swap_index)] = temp_CHANNELS[i];  // I was confident this line was correct back when 'recombine' was written!
      }
    }
  }

  subm->COARSE_CHAN = subm->CHANNELS[conf.coarse_chan - 1];  // conf.coarse_chan numbers are 1 to 24 inclusive, but the array index is 0 to 23 incl.

  if (subm->COARSE_CHAN == 0) printf("Failed to parse valid coarse channel\n");  // Check we found something plausible

  //---------- Hopefully we did that okay, although we better check during debugging that it handles the reversing above channel 128 correctly ----------

  fits_read_key_verbose(fptr, TFLOAT, "FINECHAN", NULL, &(subm->FINECHAN), NULL, &status);
  subm->FINECHAN_hz = (int)(subm->FINECHAN * 1000.0);  // We'd prefer the fine channel width in Hz rather than kHz.

  fits_read_key_verbose(fptr, TFLOAT, "INTTIME", "Integration Time", &(subm->INTTIME), NULL, &status);
  subm->INTTIME_msec = (int)(subm->INTTIME * 1000.0);  // We'd prefer the integration time in msecs rather than seconds.

  fits_read_key_verbose(fptr, TINT, "NINPUTS", NULL, &(subm->NINPUTS), NULL, &status);
  if (subm->NINPUTS > MAX_INPUTS) subm->NINPUTS = MAX_INPUTS;  // Don't allow more inputs than MAX_INPUTS (probably die reading the tile list anyway)

  if (subm->NINPUTS == 0) printf("subfile specifies no inputs!?\n");  // Check we found something plausible

  fits_read_key_verbose(fptr, TLONGLONG, "UNIXTIME", NULL, &(subm->UNIXTIME), NULL, &status);
  FITS_CHECK("read_key UNIXTIME");
  //---------- We now have everything we need from the 1st HDU ----------

  int colnum;
  int anynulls;
  long nrows;
  long ntimes;

  long frow, felem;

  int cfitsio_ints[MAX_INPUTS];      // Temp storage for integers read from the metafits file (in metafits order) before copying to final structure (in sub file order)
  int64_t cfitsio_J[3];              // Temp storage for long "J" type integers read from the metafits file (used in pointing HDU)
  float cfitsio_floats[MAX_INPUTS];  // Temp storage for floats read from the metafits file (in metafits order) before copying to final structure (in sub file order)

  char cfitsio_strings[MAX_INPUTS][15];  // Temp storage for strings read from the metafits file (in metafits order) before copying to final structure (in sub file order)
  char *cfitsio_str_ptr[MAX_INPUTS];     // We also need an array of pointers to the stings
  for (int loop = 0; loop < MAX_INPUTS; loop++) {
    cfitsio_str_ptr[loop] = &cfitsio_strings[loop][0];  // That we need to fill with the addresses of the first character in each string in the list of inputs
  }

  int metafits2sub_order[MAX_INPUTS];  // index is the position in the metafits file starting at 0.  Value is the order in the sub file starting at 0.

  fits_movnam_hdu(fptr, BINARY_TBL, "TILEDATA", 0, &status);
  FITS_CHECK("Moving to TILEDATA HDU");

  fits_get_num_rows(fptr, &nrows, &status);
  FITS_CHECK("get_num_rows 2nd HDU");
  if (nrows != subm->NINPUTS) {
    printf("NINPUTS (%d) doesn't match number of rows in tile data table (%ld)\n", subm->NINPUTS, nrows);
    return false;
  }

  frow  = 1;
  felem = 1;
  //        nullval = -99.;

  fits_get_colnum(fptr, CASEINSEN, "Antenna", &colnum, &status);
  fits_read_col(fptr, TINT, colnum, frow, felem, nrows, 0, cfitsio_ints, &anynulls, &status);
  FITS_CHECK("reading Antenna column");

  fits_get_colnum(fptr, CASEINSEN, "Pol", &colnum, &status);
  fits_read_col(fptr, TSTRING, colnum, frow, felem, nrows, 0, &cfitsio_str_ptr, &anynulls, &status);
  FITS_CHECK("reading Pol column");

  for (int loop = 0; loop < nrows; loop++) {
    metafits2sub_order[loop] = (cfitsio_ints[loop] << 1) |                 // Take the "Antenna" number, multiply by 2 via lshift
                               ((*cfitsio_str_ptr[loop] == 'Y') ? 1 : 0);  // and iff the 'Pol' is Y, then add in a 1. That's how you know where in the sub file it goes.
  }

  // Now we know how to map the the order from the metafits file to the sub file (and internal structure), it's time to start reading in the fields one at a time

  //---------- write the 'Antenna' and 'Pol' fields -------- NB: These data are sitting in the temporary arrays already, so we don't need to reread them.

  for (int loop = 0; loop < nrows; loop++) {
    subm->rf_inp[metafits2sub_order[loop]].Antenna =
        cfitsio_ints[loop];  // Copy each integer from the array we got from the metafits (via cfitsio) into one element of the rf_inp array structure
    strcpy(subm->rf_inp[metafits2sub_order[loop]].Pol,
           cfitsio_str_ptr[loop]);  // Copy each string from the array we got from the metafits (via cfitsio) into one element of the rf_inp array structure
  }

  //---------- Read and write the 'Input' field --------

  fits_get_colnum(fptr, CASEINSEN, "Input", &colnum, &status);
  fits_read_col(fptr, TINT, colnum, frow, felem, nrows, 0, cfitsio_ints, &anynulls, &status);
  FITS_CHECK("reading Input column");
  for (int loop = 0; loop < nrows; loop++) subm->rf_inp[metafits2sub_order[loop]].Input = cfitsio_ints[loop];
  // Copy each integer from the array we got from the metafits (via cfitsio) into one element of the rf_inp array structure

  //---------- Read and write the 'Tile' field --------

  fits_get_colnum(fptr, CASEINSEN, "Tile", &colnum, &status);
  fits_read_col(fptr, TINT, colnum, frow, felem, nrows, 0, cfitsio_ints, &anynulls, &status);
  FITS_CHECK("reading Tile column");
  for (int loop = 0; loop < nrows; loop++) subm->rf_inp[metafits2sub_order[loop]].Tile = cfitsio_ints[loop];
  // Copy each integer from the array we got from the metafits (via cfitsio) into one element of the rf_inp array structure

  //---------- Read and write the 'TileName' field --------

  fits_get_colnum(fptr, CASEINSEN, "TileName", &colnum, &status);
  fits_read_col(fptr, TSTRING, colnum, frow, felem, nrows, 0, &cfitsio_str_ptr, &anynulls, &status);
  FITS_CHECK("reading TileName column");
  for (int loop = 0; loop < nrows; loop++) strcpy(subm->rf_inp[metafits2sub_order[loop]].TileName, cfitsio_str_ptr[loop]);

  //---------- Read and write the 'Rx' field --------

  fits_get_colnum(fptr, CASEINSEN, "Rx", &colnum, &status);
  fits_read_col(fptr, TINT, colnum, frow, felem, nrows, 0, cfitsio_ints, &anynulls, &status);
  FITS_CHECK("reading Rx column");
  for (int loop = 0; loop < nrows; loop++) subm->rf_inp[metafits2sub_order[loop]].Rx = cfitsio_ints[loop];
  // Copy each integer from the array we got from the metafits (via cfitsio) into one element of the rf_inp array structure

  //---------- Read and write the 'Slot' field --------

  fits_get_colnum(fptr, CASEINSEN, "Slot", &colnum, &status);
  fits_read_col(fptr, TINT, colnum, frow, felem, nrows, 0, cfitsio_ints, &anynulls, &status);
  FITS_CHECK("reading Slot column");
  for (int loop = 0; loop < nrows; loop++) subm->rf_inp[metafits2sub_order[loop]].Slot = cfitsio_ints[loop];
  // Copy each integer from the array we got from the metafits (via cfitsio) into one element of the rf_inp array structure

  //---------- Read and write the 'Flag' field --------

  fits_get_colnum(fptr, CASEINSEN, "Flag", &colnum, &status);
  fits_read_col(fptr, TINT, colnum, frow, felem, nrows, 0, cfitsio_ints, &anynulls, &status);
  FITS_CHECK("reading Flag column");
  for (int loop = 0; loop < nrows; loop++) subm->rf_inp[metafits2sub_order[loop]].Flag = cfitsio_ints[loop];
  // Copy each integer from the array we got from the metafits (via cfitsio) into one element of the rf_inp array structure

  //---------- Read and write the 'Length' field --------

  fits_get_colnum(fptr, CASEINSEN, "Length", &colnum, &status);
  fits_read_col(fptr, TSTRING, colnum, frow, felem, nrows, 0, &cfitsio_str_ptr, &anynulls, &status);
  FITS_CHECK("reading Length column");
  //      for (int loop = 0; loop < nrows; loop++) strcpy( subm->rf_inp[ metafits2sub_order[loop] ].Length, cfitsio_str_ptr[loop] );
  for (int loop = 0; loop < nrows; loop++) subm->rf_inp[metafits2sub_order[loop]].Length_f = roundl(strtold(cfitsio_str_ptr[loop] + 3, NULL) * 1000.0);
  // Not what it might first appear. Convert the weird ASCII 'EL_123' format 'Length' string into a usable float, The +3 is 'step in 3 characters'

  //---------- Read and write the 'North' field --------

  fits_get_colnum(fptr, CASEINSEN, "North", &colnum, &status);
  fits_read_col(fptr, TFLOAT, colnum, frow, felem, nrows, 0, cfitsio_floats, &anynulls, &status);
  FITS_CHECK("reading North column");
  for (int loop = 0; loop < nrows; loop++) subm->rf_inp[metafits2sub_order[loop]].North = roundl(cfitsio_floats[loop] * 1000.0);  // Convert to long double in mm and round

  //---------- Read and write the 'East' field --------

  fits_get_colnum(fptr, CASEINSEN, "East", &colnum, &status);
  fits_read_col(fptr, TFLOAT, colnum, frow, felem, nrows, 0, cfitsio_floats, &anynulls, &status);
  FITS_CHECK("reading East column");
  for (int loop = 0; loop < nrows; loop++) subm->rf_inp[metafits2sub_order[loop]].East = roundl(cfitsio_floats[loop] * 1000.0);  // Convert to long double in mm and round

  //---------- Read and write the 'Height' field --------

  fits_get_colnum(fptr, CASEINSEN, "Height", &colnum, &status);
  fits_read_col(fptr, TFLOAT, colnum, frow, felem, nrows, 0, cfitsio_floats, &anynulls, &status);
  FITS_CHECK("reading Height column");
  for (int loop = 0; loop < nrows; loop++) subm->rf_inp[metafits2sub_order[loop]].Height = roundl(cfitsio_floats[loop] * 1000.0);  // Convert to long double in mm and round

  //---------- Read and write the 'Gains' field --------

  fits_get_colnum(fptr, CASEINSEN, "Gains", &colnum, &status);
  // Gains is a little different because it is an array of ints. We're going to read each row (ie rf input) with a separate cfitsio call. Maybe there's a better way to do
  // this, but I don't know it!
  for (int loop = 0; loop < nrows; loop++) {
    fits_read_col(fptr, TINT, colnum, loop + 1, felem, 24, 0, subm->rf_inp[metafits2sub_order[loop]].Gains, &anynulls, &status);
  }
  FITS_CHECK("reading Gains");

  //---------- Read and write the 'BFTemps' field --------

  fits_get_colnum(fptr, CASEINSEN, "BFTemps", &colnum, &status);
  fits_read_col(fptr, TFLOAT, colnum, frow, felem, nrows, 0, cfitsio_floats, &anynulls, &status);
  for (int loop = 0; loop < nrows; loop++) subm->rf_inp[metafits2sub_order[loop]].BFTemps = cfitsio_floats[loop];
  // Copy each float from the array we got from the metafits (via cfitsio) into one element of the rf_inp array structure
  FITS_CHECK("reading BFTemps");

  //---------- Read and write the 'Delays' field --------

  fits_get_colnum(fptr, CASEINSEN, "Delays", &colnum, &status);
  // Like 'Gains', this is a little different because it is an array of ints. See comments against 'Gains' for more detail
  for (int loop = 0; loop < nrows; loop++) {
    fits_read_col(fptr, TINT, colnum, loop + 1, felem, 16, 0, subm->rf_inp[metafits2sub_order[loop]].Delays, &anynulls, &status);
  }
  FITS_CHECK("reading Delays");

  //---------- Read and write the 'VCSOrder' field --------
  //
  //        fits_get_colnum( fptr, CASEINSEN, "VCSOrder", &colnum, &status );
  //        fits_read_col( fptr, TFLOAT, colnum, frow, felem, nrows, 0, cfitsio_floats, &anynulls, &status );
  //        for (int loop = 0; loop < nrows; loop++) subm->rf_inp[ metafits2sub_order[loop] ].VCSOrder = cfitsio_floats[loop];            // Copy each float from the
  //        array we got from the metafits (via cfitsio) into one element of the rf_inp array structure
  //
  //---------- Read and write the 'Flavors' field --------

  fits_get_colnum(fptr, CASEINSEN, "Flavors", &colnum, &status);
  fits_read_col(fptr, TSTRING, colnum, frow, felem, nrows, 0, &cfitsio_str_ptr, &anynulls, &status);
  for (int loop = 0; loop < nrows; loop++) strcpy(subm->rf_inp[metafits2sub_order[loop]].Flavors, cfitsio_str_ptr[loop]);
  FITS_CHECK("reading Flavors");

  // Now we have read everything available from the TILEDATA HDU
  // but we want to do some conversions and calculations per tile.
  // Those can be performed by the caller.

  // We need to read in the AltAz information from the ALTAZ HDU for the beginning, middle and end of this subobservation

  fits_movnam_hdu(fptr, BINARY_TBL, "ALTAZ", 0, &status);
  FITS_CHECK("Moving to ALTAZ HDU");

  fits_get_num_rows(fptr, &ntimes, &status);  // How many rows (times) are written to the metafits?
  DEBUG_LOG("ntimes=%ld\n", ntimes);

  // They *should* start at GPSTIME and have an entry every 4 seconds for EXPOSURE seconds, inclusive of the beginning and end.  ie 3 times for 8 seconds exposure @0sec,
  // @4sec & @8sec So the number of them should be '(subm->EXPOSURE >> 2) + 1' The 3 we want for the beginning, middle and end of this subobs are '((subm->subobs -
  // subm->GPSTIME)>>2)+1', '((subm->subobs - subm->GPSTIME)>>2)+2' & '((subm->subobs - subm->GPSTIME)>>2)+3' a lot of the time, we'll be past the end of the exposure
  // time, so if we are, we'll need to fill the values in with something like a zenith pointing.

  if ((subm->GEODEL == 1) ||                                     // If we have been specifically asked for zenith pointings *or*
      ((((subm->subobs - subm->GPSTIME) >> 2) + 3) > ntimes)) {  // if we want times which are past the end of the list available in the metafits
    DEBUG_LOG("Not going to do delay tracking!! GEODEL=%d subobs=%d GPSTIME=%lld ntimes=%ld\n", subm->GEODEL, subm->subobs, subm->GPSTIME, ntimes);
    for (int loop = 0; loop < 3; loop++) {                    // then we need to put some default values in (ie between observations)
      subm->altaz[loop].gpstime = (subm->subobs + loop * 4);  // populate the true gps times for the beginning, middle and end of this *sub*observation
      subm->altaz[loop].Alt     = 90.0;                       // Point straight up (in degrees above horizon)
      subm->altaz[loop].Az      = 0.0;                        // facing North (in compass degrees)
      subm->altaz[loop].Dist_km = 0.0;                        // No 'near field' supported between observations so just use 0.

      subm->altaz[loop].SinAzCosAlt = 0L;  // will be multiplied by tile East
      subm->altaz[loop].CosAzCosAlt = 0L;  // will be multiplied by tile North
      subm->altaz[loop].SinAlt      = 1L;  // will be multiplied by tile Height
    }

  } else {
    // We know from the condition test above that we have 3 valid pointings available to read from the metafits 3rd HDU

    frow = ((subm->subobs - subm->GPSTIME) >> 2) + 1;  // We want to start at the first pointing for this *subobs* not the obs, so we need to step into the list

    //---------- Read and write the 'gpstime' field --------

    fits_get_colnum(fptr, CASEINSEN, "gpstime", &colnum, &status);
    fits_read_col(fptr, TLONGLONG, colnum, frow, felem, 3, 0, cfitsio_J, &anynulls,
                  &status);  // Read start, middle and end time values, beginning at *this* subobs in the observation
    for (int loop = 0; loop < 3; loop++)
      subm->altaz[loop].gpstime = cfitsio_J[loop];  // Copy each 'J' integer from the array we got from the metafits (via cfitsio) into one element of the pointing array structure
    FITS_CHECK("reading gpstime column");

    //---------- Read and write the 'Alt' field --------

    fits_get_colnum(fptr, CASEINSEN, "Alt", &colnum, &status);
    fits_read_col(fptr, TFLOAT, colnum, frow, felem, 3, 0, cfitsio_floats, &anynulls,
                  &status);  // Read start, middle and end time values, beginning at *this* subobs in the observation
    for (int loop = 0; loop < 3; loop++)
      subm->altaz[loop].Alt = cfitsio_floats[loop];  // Copy each float from the array we got from the metafits (via cfitsio) into one element of the rf_inp array structure
    FITS_CHECK("reading Alt column");

    //---------- Read and write the 'Az' field --------

    fits_get_colnum(fptr, CASEINSEN, "Az", &colnum, &status);
    fits_read_col(fptr, TFLOAT, colnum, frow, felem, 3, 0, cfitsio_floats, &anynulls,
                  &status);  // Read start, middle and end time values, beginning at *this* subobs in the observation
    for (int loop = 0; loop < 3; loop++)
      subm->altaz[loop].Az = cfitsio_floats[loop];  // Copy each float from the array we got from the metafits (via cfitsio) into one element of the rf_inp array structure
    FITS_CHECK("reading Az column");

    //---------- Read and write the 'Dist_km' field --------

    fits_get_colnum(fptr, CASEINSEN, "Dist_km", &colnum, &status);
    fits_read_col(fptr, TFLOAT, colnum, frow, felem, 3, 0, cfitsio_floats, &anynulls,
                  &status);  // Read start, middle and end time values, beginning at *this* subobs in the observation
    for (int loop = 0; loop < 3; loop++)
      subm->altaz[loop].Dist_km = cfitsio_floats[loop];  // Copy each float from the array we got from the metafits (via cfitsio) into one element of the rf_inp array structure
    FITS_CHECK("reading Dist_km column");

    //---------- Now calculate the East/North/Height conversion factors for the three times --------

    long double d2r = M_PIl / 180.0L;          // long double conversion factor from degrees to radians
    long double SinAlt, CosAlt, SinAz, CosAz;  // temporary variables for storing the trig results we need to do delay tracking
    for (int loop = 0; loop < 3; loop++) {
      sincosl(d2r * (long double)subm->altaz[loop].Alt, &SinAlt, &CosAlt);  // Calculate the Sin and Cos of Alt in one operation.
      sincosl(d2r * (long double)subm->altaz[loop].Az, &SinAz, &CosAz);     // Calculate the Sin and Cos of Az in one operation.
      //
      subm->altaz[loop].SinAzCosAlt = SinAz * CosAlt;  // this conversion factor will be multiplied by tile East
      subm->altaz[loop].CosAzCosAlt = CosAz * CosAlt;  // this conversion factor will be multiplied by tile North
      subm->altaz[loop].SinAlt      = SinAlt;          // this conversion factor will be multiplied by tile Height
    }
  }
  //---------- We now have everything we need from the fits file ----------

  if (status == END_OF_FILE) status = 0;  // Reset after normal error
  fits_close_file(fptr, &status);
  if (status) {
    fprintf(stderr, "Error when closing metafits file: ");
    fits_report_error(stderr, status);  // print any error message
  }
  return true;
}

void test_read_metafits(int tdi) {
  subobs_udp_meta_t sub;
  int instance           = 0;                                   // Assume we're the first (or only) instance on this server
  int chan_override      = 0;                                   // Assume we are going to use the default coarse channel for this instance
  char *conf_file        = "/vulcan/mwax_config/mwax_u2s.cfg";  // Default configuration path
  char *shared_conf_file = "/vulcan/mwax_config/mwax.cfg";      // Default shared configuration path
  char hostname[300];                                           // Long enough to fit a 255 host name.  Probably it will only be short, but -hey- it's just a few bytes
  if (gethostname(hostname, sizeof hostname) == -1) strcpy(hostname, "unknown");  // Get the hostname or default to unknown if we fail
  printf("Running udp2sub on %s.  ver " THISVER " Build %d\n", hostname, BUILD);

  read_config(conf_file, shared_conf_file, hostname, instance, chan_override, &conf);
  printf("conf.coarse_chan = %d\n", conf.coarse_chan);

  struct {
    char *fnam;
    uint32_t subobs;
  } test_data[] = {
      {"/home/mwa/incident/1408332144_metafits.fits", 1408332145},
      {"/home/mwa/incident/1408313944_metafits.fits", 1408313945},
      {"/home/mwa/incident/1408693944_metafits.fits", 1408693945},  // current, ok

  };
  sub.subobs = test_data[tdi].subobs;
  bool ok    = read_metafits(test_data[tdi].fnam, &sub);
  printf("read_metafits(\"%s\", &sub) returned %s\n\n", test_data[tdi].fnam, ok ? "true" : "false");
}

//---------------------------------------------------------------------------------------------------------------------------------------------------
// Add metafits info - Every time a new 8 second block of udp packets starts, try to find the metafits data applicable and write it into the subm structure
//---------------------------------------------------------------------------------------------------------------------------------------------------

void add_meta_fits() {
  DIR *dir;
  struct dirent *dp;
  int len_filename;

  char metafits_file[300];  // The metafits file name

  int64_t bcsf_obsid;  // 'best candidate so far' for the metafits file
  int64_t mf_obsid;

  int loop;
  int slot_index;  // slot to read metafits for.
  bool go4meta;

  subobs_udp_meta_t *subm;  // pointer to the sub metadata array I'm working on

  struct timespec started_meta_write_time;
  struct timespec ended_meta_write_time;

  long double mm2s_conv_factor = (long double)SAMPLES_PER_SEC / (long double)LIGHTSPEED;  // Frequently used conversion factor of delay in units of millimetres to samples.

  static long double two_over_num_blocks_sqrd =
      (((long double)2L) / ((long double)(BLOCKS_PER_SUB * FFT_PER_BLOCK) * (long double)(BLOCKS_PER_SUB * FFT_PER_BLOCK)));  // Frequently used during fitting parabola
  static long double one_over_num_blocks = (((long double)1L) / (long double)(BLOCKS_PER_SUB * FFT_PER_BLOCK));               // Frequently used during fitting parabola

  static long double res_max = (long double)2L;   // Frequently used in range checking. Largest allowed residual.
  static long double res_min = (long double)-2L;  // Frequently used in range checking. Smallest allowed residual.

  long double a, b, c;  // coefficients of the delay fitting parabola

  int ticks_waited = 0;

  //---------------- Main loop to live in until shutdown -------------------

  fprintf(stderr, "add_meta_fits started\n");
  fflush(stderr);

  clock_gettime(CLOCK_REALTIME, &ended_meta_write_time);  // Fake the ending time for the last metafits file
                                                          // ('cos like there wasn't one y'know but the logging will expect something)

  while (!terminate) {  // If we're not supposed to shut down, let's find something to do

    //---------- look for a sub which needs metafits info ----------

    slot_index = -1;  // Start by assuming there's nothing to do

    for (loop = 0; loop < SUB_SLOTS; loop++) {  // Look through all four subobs meta arrays

      if ((slot_state[loop] == 6) && (meta_state[loop] < 2)) {   // If we are in the process of abandoning this slot, but haven't started reading metadata yet
        meta_state[loop] = 6;                                    // metadata read cancelled, the clearing thread doesn't need to wait for metadata read completion.
      } else if (meta_state[loop] == 1) {                        // If this sub is ready to have M&C metadata added
        if (slot_index == -1) {                                  // check if we've already found a different one to do and if we haven't
          slot_index = loop;                                     // then mark this one as the best so far
        } else if (sub[slot_index].subobs > sub[loop].subobs) {  // if that other one was for a later sub than this one
          slot_index = loop;                                     // then mark this one as the most urgent
        }
      }
    }  // We left this loop with an index to the best sub to do or a -1 if none available.

    //---------- if we don't have one ----------

    if (slot_index == -1) {  // if there is nothing to do
      usleep(100000);        // Chill for a longish time.  NO point in checking more often than a couple of times a second.
      ticks_waited += 1;
      if (ticks_waited % (10 * 6) == 0) {  // every few seconds
        report_substatus("add_meta_fits", "waiting");
      }
    } else {  // or we have some work to do!  Like a whole metafits file to process!
      ticks_waited = 0;
      report_substatus("add_meta_fits", "subobs %d slot %d. About to read metafits.", sub[slot_index].subobs, slot_index);

      //---------- but if we *do* have metafits info needing to be read ----------

      //      This metafits file we are about to read is the metafits that matches the subobs that we began to see packets for 8 seconds ago.  The subobs still has another 8
      //      seconds before we close it off, during which late packets and retries can be requested and received (if we ever finish that).  It's had plenty of time to be written
      //      and even updated with tile flags from pointing errors by the M&C

      clock_gettime(CLOCK_REALTIME, &started_meta_write_time);  // Record the start time before we actually get started.  The clock starts ticking from here.

      subm = &sub[slot_index];  // Temporary pointer to our sub's metadata array

      subm->meta_msec_wait = ((started_meta_write_time.tv_sec - ended_meta_write_time.tv_sec) * 1000) +
                             ((started_meta_write_time.tv_nsec - ended_meta_write_time.tv_nsec) / 1000000);  // msec since the last metafits processed

      meta_state[slot_index] = 2;  // Record that we're working on this one!

      go4meta = true;  // All good so far

      //---------- Look in the metafits directory and find the most applicable metafits file ----------

      if (go4meta) {      // If everything is okay so far, enter the next block of code
        go4meta = false;  // but go back to assuming a failure unless we succeed in the next bit

        bcsf_obsid = 0;  // 'best candidate so far' is very bad indeed (ie non-existent)

        dir = opendir(conf.metafits_dir);  // Open up the directory where metafits live

        if (dir == NULL) {  // If the directory doesn't exist we must be running on an incorrectly set up server
          printf("Fatal error: Directory %s does not exist\n", conf.metafits_dir);
          fflush(stdout);
          terminate = true;    // Tell every thread to close down
          pthread_exit(NULL);  // and close down ourselves.
        }

        // note - there could be a few thousand entries here.
        while ((dp = readdir(dir)) != NULL) {                      // Read an entry and while there are still directory entries to look at
          if ((dp->d_type == DT_REG) && (dp->d_name[0] != '.')) {  // If it's a regular file (ie not a directory or a named pipe etc) and it's not hidden
            if (((len_filename = strlen(dp->d_name)) >= 15) && (strcmp(&dp->d_name[len_filename - 14], "_metafits.fits") == 0)) {
              // If the filename is at least 15 characters long and the last 14 characters are "_metafits.fits"
              mf_obsid = strtoll(dp->d_name, 0, 0);  // Get the obsid from the name
              if (mf_obsid <= subm->subobs) {        // if the obsid is less than or the same as our subobs id, then it's a candidate for the one we need
                if (mf_obsid > bcsf_obsid) {         // If this metafits is later than the 'best candidate so far'?
                  bcsf_obsid = mf_obsid;             // that would make it our new best candidate so far
                  go4meta    = true;                 // We've found at least one file worth looking at.
                }
              }
            }
          }
        }
        closedir(dir);
      }

      //---------- Open the 'best candidate so far' metafits file ----------

      if (go4meta) {                                                                    // If everything is okay so far, enter the next block of code
        sprintf(metafits_file, "%s/%ld_metafits.fits", conf.metafits_dir, bcsf_obsid);  // Construct the full file name including path
        go4meta = read_metafits(metafits_file, subm);
        report_substatus("add_meta_fits", "attempt to read %s %s.", metafits_file, go4meta ? "succeeded" : "failed");
      }  // End of 'go for meta' metafile reading

      if (go4meta) {
        //---------- Let's take all that metafits info, do some maths and other processing and get it ready to use, for when we need to actually write out the sub file

        for (int loop = 0; loop < subm->NINPUTS; loop++) {
          tile_meta_t *rfm = &subm->rf_inp[loop];                           // Make a temporary pointer to this rf input, if only for readability of the source
          rfm->rf_input = (rfm->Tile << 1) | ((*rfm->Pol == 'Y') ? 1 : 0);  // Take the "Tile" number, multiply by 2 via lshift and iff the 'Pol' is Y, then add in a 1. That gives
          // the content of the 'rf_input' field in the udp packets for this row

          long double delay_so_far_start_mm  = 0;  // accumulator for delay to apply IN MILLIMETRES calculated so far at start of 8 sec subobservation
          long double delay_so_far_middle_mm = 0;  // accumulator for delay to apply IN MILLIMETRES calculated so far at middle of 8 sec subobservation
          long double delay_so_far_end_mm    = 0;  // accumulator for delay to apply IN MILLIMETRES calculated so far at end of 8 sec subobservation

          //---------- Do cable delays ----------

          if (subm->CABLEDEL >= 1) {  // CABLEDEL indicates: 0=Don't apply delays. 1=apply only the cable delays. 2=apply cable delays _and_ average beamformer dipole delays.
            delay_so_far_start_mm += rfm->Length_f;   // So add in the cable delays WIP!!! or is it subtract them?
            delay_so_far_middle_mm += rfm->Length_f;  // Cable delays apply equally at the start, middle and end of the subobservation
            delay_so_far_end_mm += rfm->Length_f;     // so add them equally to all three delays for start, middle and end
          }

          //          if ( subm->CABLEDEL >= 2 ){  // CABLEDEL indicates: 0=Don't apply delays. 1=apply only
          //          the cable delays. 2=apply cable delays _and_ average beamformer dipole delays.
          //          what's the value in mm of the beamformer delays
          //            delay_so_far_start_mm += bf_delay;
          //            delay_so_far_middle_mm += bf_delay;                    // Cable delays apply equally at the start, middle and end
          //            of the subobservation delay_so_far_end_mm += bf_delay; // so add them equally to all three
          //            delays for start, middle and end
          //          }

          //---------- Do Geometric delays ----------

          if (subm->GEODEL >= 1) {  // GEODEL field. (0=nothing, 1=zenith, 2=tile-pointing, 3=az/el table tracking)
            delay_so_far_start_mm += get_path_difference(rfm->North, rfm->East, rfm->Height, subm->altaz[0].Alt, subm->altaz[0].Az);
            delay_so_far_middle_mm += get_path_difference(rfm->North, rfm->East, rfm->Height, subm->altaz[1].Alt, subm->altaz[1].Az);
            delay_so_far_end_mm += get_path_difference(rfm->North, rfm->East, rfm->Height, subm->altaz[2].Alt, subm->altaz[2].Az);
          }
          DEBUG_LOG("north: %Lf, east: %Lf, height: %Lf, ", rfm->North, rfm->East, rfm->Height);
          //---------- Do Calibration delays ----------
          //   WIP.  Calibration delays are just a dream right now

          //---------- Convert 'start', 'middle' and 'end' delays from millimetres to samples --------

          long double start_sub_s  = delay_so_far_start_mm * mm2s_conv_factor;   // Convert start delay into samples at the coarse channel sample rate
          long double middle_sub_s = delay_so_far_middle_mm * mm2s_conv_factor;  // Convert middle delay into samples at the coarse channel sample rate
          long double end_sub_s    = delay_so_far_end_mm * mm2s_conv_factor;     // Convert end delay into samples at the coarse channel sample rate
          DEBUG_LOG("start: %Lf, middle: %Lf, end: %Lf, ", start_sub_s, middle_sub_s, end_sub_s);
          //---------- Commit to a whole sample delay and calculate the residuals --------

          long double whole_sample_delay =
              roundl(middle_sub_s);  // Pick a whole sample delay.  Use the rounded version of the middle delay for now. NB: seems to need -lm for linking
          // This will be corrected for by shifting coarse channel samples forward and backward in time by whole samples

          start_sub_s -= whole_sample_delay;   // Remove the whole sample we're using for this subobs to leave the residual delay that will be done by phase turning
          middle_sub_s -= whole_sample_delay;  // This may still leave more than +/- a half a sample
          end_sub_s -= whole_sample_delay;     // but it's not allowed to leave more than +/- two whole samples (See Ian's code or IanM for more detail)

          //---------- Check it's within the range Ian's code can handle of +/- two whole samples

          if ((start_sub_s > res_max) || (start_sub_s < res_min) || (end_sub_s > res_max) || (end_sub_s < res_min)) {
            printf("residual delays out of bounds!\n");
          }

          //---------- Now treat the start, middle & end  residuals as points on a parabola at x=0, x=800, x=1600 ----------
          //      Calculate a, b & c of this parabola in the form: delay = ax^2 + bx + c where x is the (tenth of a) block number

          a = (start_sub_s - middle_sub_s - middle_sub_s + end_sub_s) * two_over_num_blocks_sqrd;                                                       // a = (s+e-2*m)/(n*n/2)
          b = (middle_sub_s + middle_sub_s + middle_sub_s + middle_sub_s - start_sub_s - start_sub_s - start_sub_s - end_sub_s) * one_over_num_blocks;  // b = (4*m-3*s-e)/(n)
          c = start_sub_s;                                                                                                                              // c = s
          DEBUG_LOG("a: %Lf, b: %Lf, c: %Lf, ", a, b, c);
          //      residual delays can now be interpolated for any time using 'ax^2 + bx + c' where x is the time

          //---------- We'll be calulating each value in turn so we're better off passing back in a form only needing 2 additions per data point.
          //   The phase-wrap delay correction phase wants the delay at time points of x=.5, x=1.5, x=2.5, x=3.5 etc, so
          //   we'll set an initial value of a×0.5^2 + b×0.5 + c to get the first point and our first step in will be:
          rfm->initial_delay      = a * 0.25L + b * 0.5L + c;     // ie a×0.5^2 + b×0.5 + c for our initial value of delay(0.5)
          rfm->delta_delay        = a + a + b;                    // That's our first step.  ie delay(1.5) - delay(0.5) or if you like, (a*1.5^2+b*1.5+c) - (a*0.5^2+b*0.5+c)
          rfm->delta_delta_delay  = a + a;                        // ie 2a because it's the 2nd derivative
          rfm->ws_delay           = (int16_t)whole_sample_delay;  // whole_sample_delay has already been roundl(ed) somewhere above here
          rfm->start_total_delay  = start_sub_s;
          rfm->middle_total_delay = middle_sub_s;
          rfm->end_total_delay    = end_sub_s;
          DEBUG_LOG("ws: %d, initial: %d, delta: %d, delta_delta: %d\n", rfm->ws_delay, rfm->initial_delay, rfm->delta_delay, rfm->delta_delta_delay);

          //---------- Print out a bunch of debug info ----------

          if (debug_mode) {  // Debug logging to screen
            printf("%d,%d,%d,%d,%d,%d,%s,%s,%d,%d,%d,%Lf,%Lf,%Lf,%Lf,%d,%f,%f,%f,%Lf,%Lf,%Lf,%f,%s:", subm->subobs, loop, rfm->rf_input, rfm->Input, rfm->Antenna, rfm->Tile,
                   rfm->TileName, rfm->Pol, rfm->Rx, rfm->Slot, rfm->Flag,
                   //            rfm->Length,
                   rfm->Length_f, (delay_so_far_start_mm * mm2s_conv_factor), (delay_so_far_middle_mm * mm2s_conv_factor), (delay_so_far_end_mm * mm2s_conv_factor), rfm->ws_delay,
                   rfm->initial_delay, rfm->delta_delay, rfm->delta_delta_delay, rfm->North, rfm->East, rfm->Height,
                   // rfm->Gains,
                   rfm->BFTemps,
                   // rfm->Delays,
                   //            rfm->VCSOrder,
                   rfm->Flavors);

            printf("\n");
          }  // Only see this if we're in debug mode
        }
      }

      //---------- And we're basically done reading the metafits and preping for the sub file write which is only a few second away (done by another thread)

      clock_gettime(CLOCK_REALTIME, &ended_meta_write_time);

      subm->meta_msec_took = ((ended_meta_write_time.tv_sec - started_meta_write_time.tv_sec) * 1000) +
                             ((ended_meta_write_time.tv_nsec - started_meta_write_time.tv_nsec) / 1000000);  // msec since this sub started

      meta_state[slot_index] = go4meta ? 4 : 5;  // Record that we've finished working on this one even if we gave up.  Will be a 4 or a 5 depending on whether it worked or not.

      if (go4meta) {
        report_substatus("add_meta_fits", "subobs %d slot %d. Read metafits successfully.", subm->subobs, slot_index);
      } else {
        report_substatus("add_meta_fits", "subobs %d slot %d. Failed to read metafits.", subm->subobs, slot_index);
      }
    }  // End of 'if there is a metafits to read' (actually an 'else' off 'is there nothing to do')
  }  // End of huge 'while !terminate' loop
}  // End of function

void *add_meta_fits_thread() {
  add_meta_fits();
  pthread_exit(NULL);
}

//---------------------------------------------------------------------------------------------------------------------------------------------------
// makesub - Every time an 8 second block of udp packets is available, try to generate a sub files for the correlator
//---------------------------------------------------------------------------------------------------------------------------------------------------

void build_subfile_header(const subobs_udp_meta_t *subm, size_t transfer_size, int ninputs_xgpu, const data_section *data_sections, int n_data_sections);

void *makesub() {
  printf("Makesub started\n");
  fflush(stdout);

  //--------------- Set CPU affinity ---------------
  printf("Set process makesub cpu affinity returned %d\n", set_cpu_affinity(conf.cpu_mask_makesub));
  fflush(stdout);

  //---------------- Initialize and declare variables ------------------------

  DIR *dir;
  struct dirent *dp;
  struct stat filestats;
  int len_filename;
  char this_file[300], best_file[300], dest_file[300];  // The name of the file we're looking at, the name of the best file we've found so far, and the sub's final destination name

  time_t earliest_file;    // Holding space for the date we need to beat when looking for old .free files
  int free_files     = 0;  // Count of the number of ".free" files available to use for writing out .sub files
  int bad_free_files = 0;  // Count of the number of ".free" files that *cannot* be used for some reason (probably the wrong size)

  int active_rf_inputs;  // The number of rf_inputs that we want in the sub file and sent at least 1 udp packet

  char *ext_shm_buf;  // Pointer to the header of where all the external data is after the mmap
  char *dest;         // Working copy of the pointer into shm

  int ext_shm_fd;  // File descriptor for the shmem block

  size_t transfer_size;
  size_t desired_size;  // The total size (including header and zeroth block) that this sub file needs to be

  int loop;
  int slot_index;  // index of slot to write out
  int left_this_line;
  int sub_result;  // Temp storage for the result before we write it back to the array for others to see.  Once we do that, we can't change our mind.
  bool go4sub;

  mwa_udp_packet_t dummy_udp = {0};                  // Make a dummy udp packet full of zeros and NULLs.  We'll use this to stand in for every missing packet!
  void *dummy_volt_ptr       = &dummy_udp.volts[0];  // and we'll remember where we can find UDP_PAYLOAD_SIZE (4096LL) worth of zeros

  subobs_udp_meta_t *subm;  // pointer to the sub metadata array I'm working on
  tile_meta_t *rfm;         // Pointer to the tile metadata for the tile I'm working on

  int block;         // loop variable
  int MandC_rf;      // loop variable
  int ninputs_pad;   // The number of inputs in the sub file padded out with any needed dummy tiles (used to be a thing but isn't any more)
  int ninputs_xgpu;  // The number of inputs in the sub file but padded out to whatever number xgpu needs to deal with them in (not actually padded until done by Ian's code later)

  MandC_meta_t my_MandC_meta[MAX_INPUTS];  // Working copies of the changeable metafits/metabin data for this subobs
  MandC_meta_t *my_MandC;                  // Make a temporary pointer to the M&C metadata for one rf input

  delay_table_entry_t block_0_working;  // Construct a single tile's metadata struct to write to the 0th block.  We'll update and write this out as we step through tiles.
  double w_initial_delay;               // tile's initial fractional delay (times 2^20) working copy
  double w_delta_delay;                 // tile's initial fractional delay step (like its 1st derivative)

  int source_packet;  // which packet (of 5002) contains the first byte of data we need
  int source_offset;  // what is the offset in that packet of the first byte we need
  int source_remain;  // How much data is left from that offset to the end of the udp packet
  int ticks_waited;   // count howmany usleeps we've been wating for a subobservation to write.

  char *sp;

  struct timespec started_sub_write_time;
  struct timespec ended_sub_write_time;

  //---------------- Main loop to live in until shutdown -------------------

  printf("Makesub entering main loop\n");
  fflush(stdout);

  clock_gettime(CLOCK_REALTIME, &ended_sub_write_time);  // Fake the ending time for the last sub file ('cos like there wasn't one y'know but the logging will expect something)

  ticks_waited = 0;
  while (!terminate) {  // If we're not supposed to shut down, let's find something to do

    //---------- look for a sub to write out ----------

    slot_index = -1;  // Start by assuming there's nothing to write out

    for (loop = 0; loop < 4; loop++) {                       // Look through all four subobs meta arrays
      if (slot_state[loop] == 2 && meta_state[loop] == 4) {  // If this sub is ready to write out
        if (slot_index == -1) {                              // check if we've already found a different one to write out and if we haven't
          slot_index = loop;                                 // then mark this one as the best so far
        } else {                                             // otherwise we need to work out
          if (sub[slot_index].subobs > sub[loop].subobs) {   // if that other one was for a later sub than this one
            slot_index = loop;                               // in which case, mark this one as the best so far
          }
        }
      } else if ((slot_state[loop] == 2 && meta_state[loop] == 5) ||  // failed to read metadata
                 (slot_state[loop] == 4 || slot_state[loop] == 5) ||  // finished attempting makesub
                 (slot_state[loop] == 6 && meta_state[loop] >= 4)     // slot abandoned for taking too long; we wait for metafits read attempt completion before clearing.
      ) {
        report_substatus("makesub", "subobs %d slot %d. Clearing slot, as we've finished with it", sub[loop].subobs, loop);
        clear_slot(loop);
        meta_state[loop] = 0;
        slot_state[loop] = 0;
      }
    }  // We left this loop with an index to the best sub to write or a -1 if none available.

    if (slot_index == -1) {  // if there is nothing to do
      usleep(20000);         // Chillax for a bit.
      ticks_waited += 1;
      if (ticks_waited % (50 * 5) == 0) {  // every few secpnds
        report_substatus("makesub", "waiting");
      }

    } else {  // or we have some work to do!  Like a whole sub file to write out!
      ticks_waited = 0;

      report_substatus("makesub", "subobs %d slot %d. Starting writing.", sub[slot_index].subobs, slot_index);

      clock_gettime(CLOCK_REALTIME, &started_sub_write_time);  // Record the start time before we actually get started.  The clock starts ticking from here.
      subm = &sub[slot_index];                                 // Temporary pointer to our sub's metadata array

      subm->msec_wait = ((started_sub_write_time.tv_sec - ended_sub_write_time.tv_sec) * 1000) +
                        ((started_sub_write_time.tv_nsec - ended_sub_write_time.tv_nsec) / 1000000);  // msec since the last sub ending
      subm->udp_at_start_write =
          UDP_added_to_buff;       // What's the udp packet number we've received (EVEN IF WE HAVEN'T LOOKED AT IT!) at the time we start to process this sub for writing
      slot_state[slot_index] = 3;  // Record that we're working on this one!
      sub_result             = 5;  // Start by assuming we failed  (ie result==5).  If we get everything working later, we'll swap this for a 4.

      //---------- Do some last-minute metafits work to prepare ----------

      go4sub = true;  // Say it all looks okay so far

      ninputs_pad  = subm->NINPUTS;                    // We don't pad .sub files any more so these two variables are the same
      ninputs_xgpu = ((subm->NINPUTS + 31) & 0xffe0);  // Get this from 'ninputs' rounded up to multiples of 32

      transfer_size = SUB_LINE_SIZE * ninputs_pad * (BLOCKS_PER_SUB + 1);  // Should be 5275648000 for 256T in 160+1 blocks
      desired_size  = transfer_size + SUBFILE_HEADER_SIZE;                 // Should be 5275652096 for 256T in 160+1 blocks plus 4K header (1288001 x 4K for dd to make)

      active_rf_inputs = 0;  // The number of rf_inputs that we want in the sub file and sent at least 1 udp packet

      for (loop = 0; loop < ninputs_pad; loop++) {  // populate the metadata array for all rf_inputs in this subobs incl padding ones
        my_MandC_meta[loop].rf_input   = subm->rf_inp[loop].rf_input;
        my_MandC_meta[loop].start_byte = (UDP_PAYLOAD_SIZE + (subm->rf_inp[loop].ws_delay * 2));  // NB: Each delay is a sample, ie two bytes, not one!!!
        my_MandC_meta[loop].seen_order =
            subm->rf2ndx[my_MandC_meta[loop].rf_input];               // If they weren't seen, they will be 0 which maps to NULL pointers which will be replaced with padded zeros
        if (my_MandC_meta[loop].seen_order != 0) active_rf_inputs++;  // seen_order starts at 1. If it's 0 that means we didn't even get 1 udp packet for this rf_input
      }

      if (debug_mode) {  // If we're in debug mode
        sprintf(dest_file, "%s/%ld_%d_%d.free", conf.shared_mem_dir, subm->GPSTIME, subm->subobs,
                subm->COARSE_CHAN);  // Construct the full file name including path for a .free file
      } else {
        sprintf(dest_file, "%s/%ld_%d_%d.sub", conf.shared_mem_dir, subm->GPSTIME, subm->subobs,
                subm->COARSE_CHAN);  // Construct the full file name including path for a .sub (regular) file
      }

      //---------- Look in the shared memory directory and find the oldest .free file of the correct size ----------

      if (go4sub) {      // If everything is okay so far, enter the next block of code
        go4sub = false;  // but go back to assuming a failure unless we succeed in the next bit

        dir = opendir(conf.shared_mem_dir);  // Open up the shared memory directory

        if (dir == NULL) {  // If the directory doesn't exist we must be running on an incorrectly set up server
          printf("Fatal error: Directory %s does not exist\n", conf.shared_mem_dir);
          fflush(stdout);
          terminate = true;    // Tell every thread to close down
          pthread_exit(NULL);  // and close down ourselves.
        }

        earliest_file  = 0xFFFFFFFFFFFF;  // Pick some ridiculous date in the future.  This is the date we need to beat.
        free_files     = 0;               // So far, we haven't found any available free files we can reuse
        bad_free_files = 0;               // nor any free files that we *can't* use (wrong size?)

        size_t last_seen_size = 0;
        while ((dp = readdir(dir)) != NULL) {                      // Read an entry and while there are still directory entries to look at
          if ((dp->d_type == DT_REG) && (dp->d_name[0] != '.')) {  // If it's a regular file (ie not a directory or a named pipe etc) and it's not hidden
            if (((len_filename = strlen(dp->d_name)) >= 5) &&
                (strcmp(&dp->d_name[len_filename - 5], ".free") == 0)) {  // If the filename is at least 5 characters long and the last 5 characters are ".free"

              sprintf(this_file, "%s/%s", conf.shared_mem_dir, dp->d_name);  // Construct the full file name including path
              if (stat(this_file, &filestats) == 0) {                        // Try to read the file statistics and if they are available
                last_seen_size = filestats.st_size;
                if (filestats.st_size == desired_size) {  // If the file is exactly the size we need

                  // printf( "File %s has size = %ld and a ctime of %ld\n", this_file, filestats.st_size, filestats.st_ctim.tv_sec );

                  free_files++;  // That's one more we can use!

                  if (filestats.st_ctim.tv_sec < earliest_file) {  // and this file has been there longer than the longest one we've found so far (ctime)
                    earliest_file = filestats.st_ctim.tv_sec;      // We have a new 'oldest' time to beat
                    strcpy(best_file, this_file);                  // and we'll store its name
                    go4sub = true;                                 // We've found at least one file we can reuse.  It may not be the best, but we know we can do this now.
                  }
                } else {
                  bad_free_files++;  // That's one more we can use!
                }
              }
            }
          }
        }
        closedir(dir);
        if (!go4sub) {
          report_substatus("makesub", "subobs %d slot %d. Can't find any free files of size %d to use for subfile. (last size seen = %d)", sub[slot_index].subobs, slot_index,
                           desired_size, last_seen_size);
        }
      }

      //---------- Try to mmap that file (assuming we found one) so we can treat it like RAM (which it actually is) ----------

      if (go4sub) {      // If everything is okay so far, enter the next block of code
        go4sub = false;  // but go back to assuming a failure unless we succeed in the next bit

        // printf( "The winner is %s\n", best_file );

        if (rename(best_file, conf.temp_file_name) != -1) {  // If we can rename the file to our temporary name

          if ((ext_shm_fd = shm_open(&conf.temp_file_name[8], O_RDWR, 0666)) != -1) {  // Try to open the shmem file (after removing "/dev/shm" ) and if it opens successfully

            if ((ext_shm_buf = (char *)mmap(NULL, desired_size, PROT_READ | PROT_WRITE, MAP_SHARED, ext_shm_fd, 0)) != ((char *)(-1))) {  // and if we can mmap it successfully
              go4sub = true;  // Then we are all ready to use the buffer so remember that
            } else {
              printf("mmap failed\n");
              fflush(stdout);
            }

            close(ext_shm_fd);  // If the shm_open worked we want to close the file whether or not the mmap worked

          } else {
            printf("shm_open failed\n");
            fflush(stdout);
          }

        } else {
          printf("Failed rename\n");
          fflush(stdout);
        }
      }

      //---------- Hopefully we have an mmap to a sub file in shared memory and we're ready to fill it with data ----------

      if (go4sub) {  // If everything is good to go so far

        // first we write block0, noting the offsets to the data sections as we go along,
        // then we go back and write the 4k header.

        dest             = ext_shm_buf + SUBFILE_HEADER_SIZE;
        char *block0_add = dest;  // 0th block is after the 4k header,and just contains metadata;

        // Work out block1_add now to make it easy for us to 'pad out' the 0th block to a full block size.
        char *block1_add = dest + (ninputs_pad * SUB_LINE_SIZE);  // address of the 1st data block, after the 0th block.

        //---------- Write out the 0th block (the tile metadata block) ----------

        char *delay_table_start = dest;

        for (MandC_rf = 0; MandC_rf < ninputs_pad; MandC_rf++) {  // The zeroth block is the size of the padded number of inputs times SUB_LINE_SIZE.  NB: We dodn't pad any more!

          rfm = &subm->rf_inp[MandC_rf];  // Get a temp pointer for this tile's metadata

          block_0_working.rf_input         = rfm->rf_input;  // Copy the tile's ID and polarization into the structure we're about to write to the sub file's 0th block
          block_0_working.ws_delay_applied = rfm->ws_delay;  // Copy the tile's whole sample delay value into the structure we're about to write to the sub file's 0th block

          w_initial_delay = rfm->initial_delay;  // Copy the tile's initial fractional delay (times 2^20) to a working copy we can update as we step through the delays
          w_delta_delay   = rfm->delta_delay;    // Copy the tile's initial fractional delay step (~ 1st derivative) to a working copy we can update as we step through the delays

          block_0_working.initial_delay = w_initial_delay;  // Copy the tile's initial fractional delay (times 2^20) to the structure we're about to write to the 0th block
          block_0_working.delta_delay   = w_delta_delay;  // Copy the tile's initial fractional delay step (~ 1st derivative) to the structure we're about to write to the 0th block
          block_0_working.delta_delta_delay  = rfm->delta_delta_delay;  // Copy the tile's fractional delay step's step  to the structure we're about to write to the 0th block
          block_0_working.start_total_delay  = rfm->start_total_delay;
          block_0_working.middle_total_delay = rfm->middle_total_delay;
          block_0_working.end_total_delay    = rfm->end_total_delay;
          block_0_working.num_pointings      = 1;  // Initially 1, but might grow to 10 or more if beamforming.  The first pointing is for delay tracking in the correlator.

          for (int loop = 0; loop < POINTINGS_PER_SUB; loop++) {  // populate the fractional delay lookup table for this tile (one for each 5ms)
            block_0_working.frac_delay[loop] = w_initial_delay;   // Rounding and everything was all included when we did the original maths in the other thread
            w_initial_delay += w_delta_delay;                     // update our *TEMP* copies as we cycle through
            w_delta_delay += block_0_working.delta_delta_delay;   // and update out *TEMP* delta.  We need to do this using temp values, so we don't 'use up'
                                                                  // the initial values we want to write out.
          }

          dest = mempcpy(dest, &block_0_working, sizeof(block_0_working));  // write them out one at a time
        }

        char *delay_table_end  = dest;
        int delay_table_offset = delay_table_start - block0_add;
        int delay_table_length = delay_table_end - delay_table_start;

        //---------- Write out the dummy map ----------
        // Input x Packet Number bitmap of dummy packets used. All 1s = no dummy packets.

        char *packet_map_start = dest;

        uint32_t packet_map_stride = (UDP_PER_RF_PER_SUB - 2 + 7) / 8;  // round up the row size to ensure all the bits will still fit if UDP_PER_RF_PER_SUB%8 stops being 0

        for (MandC_rf = 0; MandC_rf < ninputs_pad; MandC_rf++) {
          rfm            = &subm->rf_inp[MandC_rf];  // Tile metadata
          uint16_t row   = subm->rf2ndx[rfm->rf_input];
          char **packets = sub[slot_index].udp_volts[row];
          for (int t = 0; t < UDP_PER_RF_PER_SUB - 2; t += 8) {
            uint8_t bitmap = (packets[t + 1] != NULL) << 7 | (packets[t + 2] != NULL) << 6 | (packets[t + 3] != NULL) << 5 | (packets[t + 4] != NULL) << 4 |
                             (packets[t + 5] != NULL) << 3 | (packets[t + 6] != NULL) << 2 | (packets[t + 7] != NULL) << 1 | (packets[t + 8] != NULL);
            dest[t / 8] = (char)bitmap;
          }
          dest += packet_map_stride;
        }

        char *packet_map_end  = dest;
        int packet_map_offset = packet_map_start - block0_add;
        int packet_map_length = packet_map_end - packet_map_start;

        //---------- Write out the volt data for the "margin packets" of every RF source to block 0. ----------
        /* We need two packet's worth at each end to be able to undo delays, because if we shift out samples
         * at the start of a block, we won't be able to later find them in the very first packet, they were
         * only ever stored in the second - but we still need that first packet in case we later want to shift
         * in samples, going the other way.
         */
        char *block0_margin_start = dest;

        for (MandC_rf = 0; MandC_rf < ninputs_pad; MandC_rf++) {
          my_MandC = &my_MandC_meta[MandC_rf];

          char **packets = sub[slot_index].udp_volts[my_MandC->seen_order];

          sp   = packets[0];
          dest = mempcpy(dest, sp ? sp : dummy_volt_ptr, UDP_PAYLOAD_SIZE);

          sp   = packets[1];
          dest = mempcpy(dest, sp ? sp : dummy_volt_ptr, UDP_PAYLOAD_SIZE);

          sp   = packets[UDP_PER_RF_PER_SUB - 2];
          dest = mempcpy(dest, sp ? sp : dummy_volt_ptr, UDP_PAYLOAD_SIZE);

          sp   = packets[UDP_PER_RF_PER_SUB - 1];
          dest = mempcpy(dest, sp ? sp : dummy_volt_ptr, UDP_PAYLOAD_SIZE);
        }

        char *block0_margin_end = dest;
        int margin_data_offset  = block0_margin_start - block0_add;
        int margin_data_length  = block0_margin_end - block0_margin_start;

        //
        //---------- null fill the remainder of block0
        memset(dest, 0, block1_add - dest);

        //---------- Make a 4K ASCII header for the sub file ----------
        {
          data_section data_sections[] = {
              {"PACKET_MAP", packet_map_offset, packet_map_length},
              {"DELAY_TABLE", delay_table_offset, delay_table_length},
              {"MARGIN_DATA", margin_data_offset, margin_data_length},
          };
          int lds = sizeof(data_sections) / sizeof(data_sections[0]);
          build_subfile_header(subm, transfer_size, ninputs_xgpu, data_sections, lds);
          memcpy(ext_shm_buf, sub_header, SUBFILE_HEADER_SIZE);  // Do the memory copy from the preprepared 4K subfile header to the beginning of the sub file
        }

        //---------- Write out the voltage data blocks ----------
        dest = block1_add;  // Set our write pointer to the beginning of block 1

        for (block = 1; block <= BLOCKS_PER_SUB; block++) {         // We have 160 (or whatever) blocks of real data to write out. We'll do them in time order (50ms each)
          for (MandC_rf = 0; MandC_rf < ninputs_pad; MandC_rf++) {  // Now step through the M&C ordered rf inputs for the inputs the M&C says should be included in the sub file.
                                                                    // This is likely to be similar to the list we actually saw, but may not be identical!
            my_MandC = &my_MandC_meta[MandC_rf];  // Make a temporary pointer to the M&C metadata for this rf input.  NB these are sorted in order they need to be written out as
                                                  // opposed to the order the packets arrived.

            left_this_line = SUB_LINE_SIZE;

            while (left_this_line > 0) {  // Keep going until we've finished writing out a whole line of the sub file
              int bytes2copy;

              source_packet = my_MandC->start_byte / UDP_PAYLOAD_SIZE;
              source_offset = my_MandC->start_byte % UDP_PAYLOAD_SIZE;
              source_remain = UDP_PAYLOAD_SIZE - source_offset;  // How much of the packet is left in bytes?
                                                                 // It should be an even number and at least 2 or I have a bug in the logic or code

              sp = sub[slot_index].udp_volts[my_MandC->seen_order][source_packet];  // Pick up the pointer to the udp volt data we need (assuming we ever saw it arrive)
              if (sp == NULL) {                                                     // but if it never arrived
                sp = dummy_volt_ptr;                                                // point to our pre-prepared, zero filled, fake packet we use when the real one isn't available
                subm->udp_dummy++;  // The number of dummy packets we needed to insert to pad things out. Make a note for reporting and debug purposes
                monitor.udp_dummy++;
              }

              bytes2copy =
                  ((source_remain < left_this_line) ? source_remain
                                                    : left_this_line);  // Get the minimum of the remaining length of the udp volt payload or the length of line left to populate

              dest = mempcpy(dest, sp + source_offset, bytes2copy);  // Do the memory copy from the udp payload to the sub file and update the destination pointer

              left_this_line -= bytes2copy;        // Keep up to date with the remaining amount of work needed on this line
              my_MandC->start_byte += bytes2copy;  // and where we are up to in reading them

            }  // We've finished the udp packet
          }  // We've finished the signal chain for this block
        }  // We've finished the block

        // By here, the entire sub has been written out to shared memory and it's time to close it out and rename it so it becomes available for other programs

        if ((dest - ext_shm_buf) != desired_size)
          printf("Memory pointer error in writing sub file!\n");  // before we do that, let's just do a quick check to see we ended up exactly at the end, so we can confirm all our
                                                                  // maths is right.

        munmap(ext_shm_buf, desired_size);  // Release the mmap for the whole sub file

        if (rename(conf.temp_file_name, dest_file) != -1) {  // Rename my temporary file to a final sub file name, thus releasing it to other programs
          sub_result = 4;                                    // This was our last check.  If we got to here, the sub file worked, so prepare to write that to monitoring system
        } else {
          printf("Final rename failed.\n");  // This was our last check.  If we got to here, the sub file rename failed, so prepare to write that to monitoring system
          fflush(stdout);                    // but if that fails, I'm not really sure what to do.  Let's just note it and see if it ever happens
        }

      }  // We've finished the sub file writing and closed and renamed the file.

      //---------- We're finished or we've given up.  Either way record the new state and elapsed time ----------

      subm->udp_at_end_write = UDP_added_to_buff;  // What's the udp packet number we've received (EVEN IF WE HAVEN'T LOOKED AT IT!)
                                                   // at the time we finish processing this sub for writing

      clock_gettime(CLOCK_REALTIME, &ended_sub_write_time);

      subm->msec_took = ((ended_sub_write_time.tv_sec - started_sub_write_time.tv_sec) * 1000) +
                        ((ended_sub_write_time.tv_nsec - started_sub_write_time.tv_nsec) / 1000000);  // msec since this sub started

      slot_state[slot_index] = sub_result;  // Record that we've finished working on this one even if we gave up.  Will be a 4 or a 5 depending on whether it worked or not.

      report_substatus("makesub", "subobs %d slot %d. Finished writing.", sub[slot_index].subobs, slot_index);
      printf("now=%ld,so=%d,ob=%ld,%s,st=%d,free=%d:%d,wait=%d,took=%d,used=%ld,count=%d,dummy=%d,rf_inps=w%d:s%d:c%d\n", (int64_t)(ended_sub_write_time.tv_sec - GPS_offset),
             subm->subobs, subm->GPSTIME, subm->MODE, slot_state[slot_index], free_files, bad_free_files, subm->msec_wait, subm->msec_took,
             subm->udp_at_end_write - subm->first_udp, subm->udp_count, subm->udp_dummy, subm->NINPUTS, subm->rf_seen, active_rf_inputs);

      fflush(stdout);
    }
  }  // Jump up to the top and look again (as well as checking if we need to shut down)

  //---------- We've been told to shut down ----------
  printf("Exiting makesub\n");
  pthread_exit(NULL);
}

void build_subfile_header(const subobs_udp_meta_t *subm, size_t transfer_size, int ninputs_xgpu, const data_section *data_sections, int n_data_sections) {
  int64_t obs_offset = subm->subobs - subm->GPSTIME;  // How long since the observation started?
  char utc_start[30];
  time_t observation_start = subm->UNIXTIME;
  strftime(utc_start, sizeof(utc_start), "%Y-%m-%d-%H:%M:%S", gmtime(&observation_start));

  memset(sub_header, 0, SUBFILE_HEADER_SIZE);  // Pre fill with null chars just to ensure we don't leave junk anywhere in the header

  char *bp = sub_header;
  char *ep = sub_header + SUBFILE_HEADER_SIZE;
  bp += snprintf(bp, ep - bp, "HDR_SIZE %lld\n", SUBFILE_HEADER_SIZE);
  bp += snprintf(bp, ep - bp, "POPULATED 1\n");
  bp += snprintf(bp, ep - bp, "OBS_ID %ld\n", subm->GPSTIME);
  bp += snprintf(bp, ep - bp, "SUBOBS_ID %d\n", subm->subobs);
  bp += snprintf(bp, ep - bp, "MODE %s\n", subm->MODE);
  bp += snprintf(bp, ep - bp, "UTC_START %s\n", utc_start);
  bp += snprintf(bp, ep - bp, "OBS_OFFSET %ld\n", obs_offset);
  bp += snprintf(bp, ep - bp, "NBIT 8\n");
  bp += snprintf(bp, ep - bp, "NPOL 2\n");
  bp += snprintf(bp, ep - bp, "NTIMESAMPLES %lld\n", NTIMESAMPLES);
  bp += snprintf(bp, ep - bp, "NINPUTS %d\n", subm->NINPUTS);
  bp += snprintf(bp, ep - bp, "NINPUTS_XGPU %d\n", ninputs_xgpu);
  bp += snprintf(bp, ep - bp, "APPLY_PATH_WEIGHTS 0\n");
  bp += snprintf(bp, ep - bp, "APPLY_PATH_DELAYS %d\n", subm->CABLEDEL || subm->GEODEL);
  bp += snprintf(bp, ep - bp, "APPLY_PATH_PHASE_OFFSETS %d\n", subm->CABLEDEL || subm->GEODEL);
  bp += snprintf(bp, ep - bp, "INT_TIME_MSEC %d\n", subm->INTTIME_msec);
  bp += snprintf(bp, ep - bp, "APPLY_COARSE_DERIPPLE %d\n", subm->DERIPPLE);
  bp += snprintf(bp, ep - bp, "FSCRUNCH_FACTOR %lld\n", (subm->FINECHAN_hz / ULTRAFINE_BW));
  bp += snprintf(bp, ep - bp, "APPLY_VIS_WEIGHTS 0\n");
  bp += snprintf(bp, ep - bp, "TRANSFER_SIZE %ld\n", transfer_size);
  bp += snprintf(bp, ep - bp, "PROJ_ID %s\n", subm->PROJECT);
  bp += snprintf(bp, ep - bp, "EXPOSURE_SECS %d\n", subm->EXPOSURE);
  bp += snprintf(bp, ep - bp, "COARSE_CHANNEL %d\n", subm->COARSE_CHAN);
  bp += snprintf(bp, ep - bp, "CORR_COARSE_CHANNEL %d\n", conf.coarse_chan);
  bp += snprintf(bp, ep - bp, "SECS_PER_SUBOBS 8\n");
  bp += snprintf(bp, ep - bp, "UNIXTIME %ld\n", subm->UNIXTIME);
  bp += snprintf(bp, ep - bp, "UNIXTIME_MSEC 0\n");
  bp += snprintf(bp, ep - bp, "FINE_CHAN_WIDTH_HZ %d\n", subm->FINECHAN_hz);
  bp += snprintf(bp, ep - bp, "NFINE_CHAN %lld\n", (COARSECHAN_BANDWIDTH / subm->FINECHAN_hz));
  bp += snprintf(bp, ep - bp, "BANDWIDTH_HZ %lld\n", COARSECHAN_BANDWIDTH);
  bp += snprintf(bp, ep - bp, "SAMPLE_RATE %lld\n", SAMPLES_PER_SEC);
  bp += snprintf(bp, ep - bp, "MC_IP 0.0.0.0\n");
  bp += snprintf(bp, ep - bp, "MC_PORT 0\n");
  bp += snprintf(bp, ep - bp, "MC_SRC_IP 0.0.0.0\n");
  bp += snprintf(bp, ep - bp, "MWAX_U2S_VER " THISVER "-%d\n", BUILD);
  for (int i = 0; i < n_data_sections; i++) bp += snprintf(bp, ep - bp, "IDX_%s %d+%d\n", data_sections[i].name, data_sections[i].offset, data_sections[i].length);
  bp += snprintf(bp, ep - bp, "MWAX_SUB_VER 2\n");
}

void *heartbeat() {
  unsigned char ttl = MONITOR_TTL;
  struct sockaddr_in addr;
  struct in_addr localInterface;

  int monitor_socket;

  // create what looks like an ordinary UDP socket
  if ((monitor_socket = socket(AF_INET, SOCK_DGRAM, 0)) < 0) {
    perror("socket");
    terminate = true;
    pthread_exit(NULL);
  }

  // set up destination address
  memset(&addr, 0, sizeof(addr));
  addr.sin_family      = AF_INET;
  addr.sin_addr.s_addr = inet_addr(MONITOR_IP);
  addr.sin_port        = htons(MONITOR_PORT);

  setsockopt(monitor_socket, IPPROTO_IP, IP_MULTICAST_TTL, &ttl, sizeof(ttl));

  char loopch = 0;

  if (setsockopt(monitor_socket, IPPROTO_IP, IP_MULTICAST_LOOP, (char *)&loopch, sizeof(loopch)) < 0) {
    perror("setting IP_MULTICAST_LOOP:");
    close(monitor_socket);
    terminate = true;
    pthread_exit(NULL);
  }

  // Set local interface for outbound multicast datagrams. The IP address specified must be associated with a local, multicast-capable interface.
  localInterface.s_addr = inet_addr(conf.monitor_if);

  if (setsockopt(monitor_socket, IPPROTO_IP, IP_MULTICAST_IF, (char *)&localInterface, sizeof(localInterface)) < 0) {
    perror("setting local interface");
    close(monitor_socket);
    terminate = true;
    pthread_exit(NULL);
  }

  monitor.version = BUILD;

  while (!terminate) {
    if (sendto(monitor_socket, &monitor, sizeof(monitor), 0, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
      printf("\nFailed to send monitor packet");
      fflush(stdout);
    }
    usleep(1000000);
  }

  printf("\nStopping heartbeat");
  pthread_exit(NULL);
}

//---------------------------------------------------------------------------------------------------------------------------------------------------

void sigint_handler(int signo) {
  printf("\n\nAsked to shut down via SIGINT\n");
  fflush(stdout);
  terminate = true;  // This is a volatile, file scope variable.  All threads should be watching for this.
}

//---------------------------------------------------------------------------------------------------------------------------------------------------

void sigterm_handler(int signo) {
  printf("\n\nAsked to shut down via SIGTERM\n");
  fflush(stdout);
  terminate = true;  // This is a volatile, file scope variable.  All threads should be watching for this.
}

//---------------------------------------------------------------------------------------------------------------------------------------------------

void sigusr1_handler(int signo) {
  printf("\n\nSomeone sent a sigusr1 signal to me\n");
  fflush(stdout);
}

//---------------------------------------------------------------------------------------------------------------------------------------------------

void usage(char *err)  // Bad command line.  Report the supported usage.
{
  printf("%s", err);
  printf("\n\n To run:        /home/mwa/udp2sub -f mwax_u2s.conf -F mwax.conf -i 1\n\n");

  printf("                    -f Configuration file to use for u2s settings\n");
  printf("                    -F Configuration file to use for shared settings\n");
  printf("                    -i Instance number on server, if multiple copies per server in use\n");
  printf("                    -c Coarse channel override\n");
  printf("                    -d Debug mode.  Write to .free files\n");
  fflush(stdout);
}

/** Generate delay tracking data for validation purposes.
 *
 * This function sets up the global state to appear as though a single subobservation for which we
 * have a metafits file is ready to have its metadata populated, without running any of the worker
 * threads. We then start the add_meta_fits() thread and wait for it to load the supplied metafits
 * file and calculate the delay tracking values that would be applied if this were a real
 * subobservation. When this has completed we terminate the thread and write the delay table to
 * standard output.
 */
int delaygen(uint32_t obs_id, uint32_t subobs_idx) {
  fprintf(stderr, "entering delay generator mode\n");

  subobs_udp_meta_t *subm = &sub[0];
  uint32_t subobs_start   = obs_id + subobs_idx * 8;
  slot_state[0]           = 1;
  meta_state[0]           = 1;
  sub[0].subobs           = subobs_start & ~7;

  fprintf(stderr, "obs ID: %d\n", obs_id);
  fprintf(stderr, "subobs ID: %d\n", sub[0].subobs);

  struct timespec time_start;
  struct timespec time_now;
  bool reader_done = false;
  clock_gettime(CLOCK_REALTIME, &time_start);
  clock_gettime(CLOCK_REALTIME, &time_now);
  pthread_t reader_thread;
  pthread_create(&reader_thread, NULL, add_meta_fits_thread, NULL);
  while (!reader_done && time_now.tv_sec - time_start.tv_sec < 5) {
    clock_gettime(CLOCK_REALTIME, &time_now);
    if (meta_state[0] == 4) {
      fprintf(stderr, "metadata loaded successfully.\n");
      reader_done = true;
    } else if (meta_state[0] == 5) {
      fprintf(stderr, "error loading metadata.\n");
      reader_done = true;
    }
  }
  terminate = true;
  pthread_join(reader_thread, NULL);
  fprintf(stderr, "reader thread joined.\n");
  if (!reader_done) {
    fprintf(stderr, "timed out waiting for metadata.\n");
  }

  DEBUG_LOG("NINPUTS %d\n", sub[0].NINPUTS);
  // altaz_meta_t *altaz = subm->altaz;
  for (int i = 0; i < 3; i++) {
    DEBUG_LOG("AltAz[%d] = {Alt: %f, Az: %f, Dist_km: %f, SinAlt: %Lf, SinAzCosAlt: %Lf, CosAzCosAlt: %Lf, gpstime: %lld}\n", i, altaz[i].Alt, altaz[i].Az, altaz[i].Dist_km,
              altaz[i].SinAlt, altaz[i].SinAzCosAlt, altaz[i].CosAzCosAlt, altaz[i].gpstime);
  }
  fflush(stderr);

  tile_meta_t *rfm;
  delay_table_entry_t dt_entry_working;
  int MandC_rf;
  int ninputs_pad = sub[0].NINPUTS;
  double w_initial_delay;
  double w_delta_delay;

  /// Copied and pasted from `makesub`:
  for (MandC_rf = 0; MandC_rf < ninputs_pad; MandC_rf++) {  // The zeroth block is the size of the padded number of inputs times SUB_LINE_SIZE.  NB: We dodn't pad any more!

    rfm = &subm->rf_inp[MandC_rf];  // Get a pointer for this tile's metadata

    dt_entry_working.rf_input         = rfm->rf_input;       // Copy the tile's ID and polarization into the structure we're about to write to the sub file's 0th block
    dt_entry_working.ws_delay_applied = rfm->ws_delay;       // Copy the tile's whole sample delay value into the structure we're about to write to the sub file's 0th block
    w_initial_delay                   = rfm->initial_delay;  // Copy the tile's initial fractional delay (times 2^20) to a working copy we can update as we step through the delays
    w_delta_delay = rfm->delta_delay;  // Copy the tile's initial fractional delay step (like its 1st derivative) to a working copy we can update as we step through the delays

    dt_entry_working.initial_delay = w_initial_delay;  // Copy the tile's initial fractional delay (times 2^20) to the structure we're about to write to the 0th block
    dt_entry_working.delta_delay = w_delta_delay;  // Copy the tile's initial fractional delay step (like its 1st derivative) to the structure we're about to write to the 0th block
    dt_entry_working.delta_delta_delay =
        rfm->delta_delta_delay;          // Copy the tile's fractional delay step's step (like its 2nd derivative) to the structure we're about to write to the 0th block
    dt_entry_working.num_pointings = 1;  // Initially 1, but might grow to 10 or more if beamforming.  The first pointing is for delay tracking in the correlator.

    for (int loop = 0; loop < POINTINGS_PER_SUB; loop++) {  // populate the fractional delay lookup table for this tile (one for each 5ms)
      dt_entry_working.frac_delay[loop] = w_initial_delay;  // Rounding and everything was all included when we did the original maths in the other thread
      w_initial_delay += w_delta_delay;                     // update our *TEMP* copies as we cycle through
      w_delta_delay +=
          dt_entry_working.delta_delta_delay;  // and update out *TEMP* delta.  We need to do this using temp values, so we don't 'use up' the initial values we want to write out.
    }

    printf("%d,%d,%.*f,%.*f,%.*f,%.*f,%.*f,%.*f,%d,%d,",

           rfm->rf_input, rfm->ws_delay, DECIMAL_DIG, rfm->start_total_delay, DECIMAL_DIG, rfm->middle_total_delay, DECIMAL_DIG, rfm->end_total_delay, DECIMAL_DIG,
           rfm->initial_delay, DECIMAL_DIG, rfm->delta_delay, DECIMAL_DIG, rfm->delta_delta_delay, 1, 0);
    for (int loop = 0; loop < POINTINGS_PER_SUB - 1; loop++) {
      printf("%.*f,", DECIMAL_DIG, dt_entry_working.frac_delay[loop]);
    }
    printf("%.*f\n", DECIMAL_DIG, dt_entry_working.frac_delay[POINTINGS_PER_SUB - 1]);
    memset(&dt_entry_working, 0, sizeof(delay_table_entry_t));
  }

  // for(int i=0; i<sub[0].NINPUTS; i++) {
  //  rfm = &sub[0].rf_inp[i];

  // }
  // printf("%d", rfm->
  fflush(stdout);

  fprintf(stderr, "\ndone.\n");
  return 0;
}

void *calloc_or_die(size_t nmemb, size_t size, char *name) {
  void *res = calloc(nmemb, size);
  if (!res) {
    printf("%s calloc failed\n", name);
    fflush(stdout);
    exit(EXIT_FAILURE);
  }
  return res;
}

// ------------------------ Start of world -------------------------

int main(int argc, char **argv) {
  int prog_build = BUILD;  // Build number.  Remember to update each build revision!

  //---------------- Parse command line parameters ------------------------

  int instance           = 0;                                   // Assume we're the first (or only) instance on this server
  int chan_override      = 0;                                   // Assume we are going to use the default coarse channel for this instance
  char *conf_file        = "/vulcan/mwax_config/mwax_u2s.cfg";  // Default configuration path
  char *shared_conf_file = "/vulcan/mwax_config/mwax.cfg";      // Default shared configuration path

  uint32_t delaygen_obs_id     = 0;  // Observation ID for delay generator
  uint32_t delaygen_subobs_idx = 0;  // The n-th subobservation
  bool delaygen_enable         = false;

  while (argc > 1 && argv[1][0] == '-') {
    switch (argv[1][1]) {
      case 'd':
        debug_mode = true;
        printf("Debug mode on\n");
        fflush(stdout);
        break;

      case 'i':
        ++argv;
        --argc;
        instance = atoi(argv[1]);
        break;

      case 'c':
        ++argv;
        --argc;
        chan_override = atoi(argv[1]);
        break;

      case 'f':
        ++argv;
        --argc;
        conf_file = argv[1];
        fprintf(stderr, "Config file to read = %s\n", conf_file);
        fflush(stderr);
        break;

      case 'F':
        ++argv;
        --argc;
        shared_conf_file = argv[1];
        fprintf(stderr, "Shared config file to read = %s\n", shared_conf_file);
        fflush(stderr);
        break;

      case 'C':
        force_cable_delays = true;
        fprintf(stderr, "Force enabled cable delays.\n");
        fflush(stderr);
        break;

      case 'G':
        force_geo_delays = true;
        fprintf(stderr, "Force enabled geometric delays.\n");
        fflush(stderr);
        break;

      case 'g':
        delaygen_enable = true;
        fprintf(stderr, "Delay generator mode enabled. u2s will quit after generating delay data.\n");
        fflush(stderr);
        break;

      case 'o':
        ++argv;
        --argc;
        delaygen_obs_id = atoi(argv[1]);
        fprintf(stderr, "Using observation %d for delay generator.\n", delaygen_obs_id);
        fflush(stderr);
        break;

      case 's':
        ++argv;
        --argc;
        delaygen_subobs_idx = atoi(argv[1]);
        fprintf(stderr, "Using subobservation index %d for delay generator.\n", delaygen_subobs_idx);
        fflush(stderr);
        break;

      default:
        usage("unknown option");
        exit(EXIT_FAILURE);
    }
    --argc;
    ++argv;
  }

  if (argc > 1) {  // There is/are at least one command line option we don't understand
    usage("");     // Print the available options
    exit(EXIT_FAILURE);
  }

  //---------------- Look up our configuration options ------------------------

  char hostname[300];  // Long enough to fit a 255 host name.  Probably it will only be short, but -hey- it's just a few bytes

  if (gethostname(hostname, sizeof hostname) == -1) strcpy(hostname, "unknown");  // Get the hostname or default to unknown if we fail
  fprintf(stderr, "Running udp2sub on %s.  ver " THISVER " Build %d\n", hostname, prog_build);
  fflush(stdout);

  // Use the config file, the host name and the instance number (if there is one) to look up all our configuration and settings
  read_config(conf_file, shared_conf_file, hostname, instance, chan_override, &conf);

  printf("after read_config, UDP_PER_RF_PER_SUB = %lld\n", UDP_PER_RF_PER_SUB);
  if (conf.udp2sub_id == 0) {  // If the lookup returned an id of 0, we don't have enough information to continue
    fprintf(stderr, "Hostname not found in configuration\n");
    fflush(stdout);
    exit(EXIT_FAILURE);  // Abort!
  }

  //---------------- We have our config details now. Create/open a monitoring file in shared memory to tell M&C how we're doing ------------------------

  // WIP!

  //---------------- Trap signals from outside the program ------------------------

  signal(SIGINT, sigint_handler);    // Tell the OS that we want to trap SIGINT calls
  signal(SIGTERM, sigterm_handler);  // Tell the OS that we want to trap SIGTERM calls
  signal(SIGUSR1, sigusr1_handler);  // Tell the OS that we want to trap SIGUSR1 calls

  //---------------- Allocate the RAM we need for the incoming udp buffer and initialise it ------------------------

  UDP_num_slots = conf.UDP_num_slots;  // We moved this to a config variable, but most of the code still assumes the old name so make both names valid

  // In delay geneator mode, we don't really need to buffer any packets, but we'll let the structures be populated
  if (delaygen_enable == true) {
    UDP_num_slots = 10;
  }

  msgvecs = calloc_or_die(2 * UDP_num_slots, sizeof(struct mmsghdr), "msgvecs");  // NB Make twice as big an array as the number of actual UDP packets we are going to buffer
  iovecs  = calloc_or_die(UDP_num_slots, sizeof(struct iovec), "iovecs");         // NB Make the *same* number of entries as the number of actual UDP packets we are going to buffer
  UDPbuf  = calloc_or_die(UDP_num_slots, sizeof(mwa_udp_packet_t), "UDPbuf");     // NB Make the *same* number of entries as the number of actual UDP packets we are going to buffer

  //---------- Now initialize the arrays

  for (int loop = 0; loop < UDP_num_slots; loop++) {
    iovecs[loop].iov_base = &UDPbuf[loop];
    iovecs[loop].iov_len  = sizeof(mwa_udp_packet_t);

    msgvecs[loop].msg_hdr.msg_iov    = &iovecs[loop];  // Populate the first copy
    msgvecs[loop].msg_hdr.msg_iovlen = 1;

    msgvecs[loop + UDP_num_slots].msg_hdr.msg_iov    = &iovecs[loop];  // Now populate a second copy of msgvecs that point back to the first set
    msgvecs[loop + UDP_num_slots].msg_hdr.msg_iovlen = 1;              // That way we don't need to worry (as much) about rolling over buffers during reads
  }

  //---------------- Allocate the RAM we need for the subobs pointers and metadata and initialise it ------------------------

  sub = calloc_or_die(SUB_SLOTS, sizeof(subobs_udp_meta_t), "sub");  // Make 4 slots to store the metadata against the maximum 4 subobs that can be open at one time

  for (int slot = 0; slot < SUB_SLOTS; slot++) {
    sub[slot].udp_volts    = calloc_or_die(MAX_INPUTS + 1, sizeof(char **), "packet payload pointer pointer array");
    char **cursor          = calloc_or_die(UDP_PER_RF_PER_SUB * MAX_INPUTS, sizeof(char *), "packet payload pointer array");
    sub[slot].udp_volts[0] = NULL;  // this should never be dereferenced.
    for (int input = 1; input < MAX_INPUTS + 1; input++) {
      sub[slot].udp_volts[input] = cursor;
      cursor += UDP_PER_RF_PER_SUB;
    }
  }

  sub_header = calloc_or_die(SUBFILE_HEADER_SIZE, sizeof(char), "sub_header");  // Make ourselves a buffer that's initially of zeros that's the size of a sub file header.

  // Enter delay generator if enabled, then quit
  if (delaygen_enable == true) {
    return delaygen(delaygen_obs_id, delaygen_subobs_idx);
  }

  //---------------- We're ready to fire up the four worker threads now ------------------------

  fprintf(stderr, "Firing up pthreads\n");
  fflush(stdout);

  pthread_t UDP_recv_pt;
  pthread_create(&UDP_recv_pt, NULL, UDP_recv, NULL);  // Fire up the process to receive the udp packets into the large buffers we just created

  pthread_t UDP_parse_pt;
  pthread_create(&UDP_parse_pt, NULL, UDP_parse, NULL);  // Fire up the process to parse the udp packets and generate arrays of sorted pointers

  pthread_t makesub_pt;
  pthread_create(&makesub_pt, NULL, makesub, NULL);  // Fire up the process to generate sub files from raw packets and pointers

  pthread_t monitor_pt;
  pthread_create(&monitor_pt, NULL, heartbeat, NULL);

  //---------------- The threads are off and running.  Now we just wait for a message to terminate, like a signal or a fatal error ------------------------

  printf("Master thread switching to metafits reading.\n");
  fflush(stdout);

  //--------------- Set CPU affinity ---------------

  printf("Set process parent cpu affinity returned %d\n", set_cpu_affinity(conf.cpu_mask_parent));
  fflush(stdout);

  add_meta_fits();

  //    while(!terminate) sleep(1);                 // The master thread currently does nothing! Zip! Nada!  What a waste eh?

  //---------------- Clean up the mess we made and go home ------------------------

  printf("Master thread waiting for child threads to end.\n");
  fflush(stdout);

  if (!UDP_recv_complete) {             // we might be stalled waiting on nonexistent packets
    usleep(UDP_RECV_SHUTDOWN_TIMEOUT);  // so wait a moment and check again.
  }
  if (!UDP_recv_complete) {
    printf("UDP recv failed to shutdown (possibly blocked waiting for packets). Cancelling the thread.\n");
    fflush(stdout);
    pthread_cancel(UDP_recv_pt);  // kill the thread, it's failing to shut down gracefully.
  }
  pthread_join(UDP_recv_pt, NULL);
  printf("UDP_recv joined.\n");
  fflush(stdout);

  pthread_join(UDP_parse_pt, NULL);
  printf("UDP_parse joined.\n");
  fflush(stdout);

  pthread_join(makesub_pt, NULL);
  printf("makesub joined.\n");
  fflush(stdout);

  //---------- The other threads are all closed down now. Perfect opportunity to have a look at the memory status (while nobody is changing it in the background ----------

  subobs_udp_meta_t *subm;  // pointer to the sub metadata array I'm looking at

  for (int loop = 0; loop < SUB_SLOTS; loop++) {  // Look through all four subobs meta arrays
    subm = &sub[loop];                            // Temporary pointer to our sub's metadata array

    printf("slot=%d,so=%d,st=%d,wait=%d,took=%d,first=%ld,last=%ld,startw=%ld,endw=%ld,count=%d,seen=%d\n", loop, subm->subobs, slot_state[loop], subm->msec_wait, subm->msec_took,
           subm->first_udp, subm->last_udp, subm->udp_at_start_write, subm->udp_at_end_write, subm->udp_count, subm->rf_seen);
  }

  printf("\n");  // Blank line

  mwa_udp_packet_t *my_udp;  // Make a local pointer to the UDP packet we're going to be working on.

  int64_t UDP_closelog = UDP_removed_from_buff - 20;  // Go back 20 udp packets

  if (debug_mode) {                              // If we're in debug mode
    UDP_closelog = UDP_removed_from_buff - 100;  // go back 100!
  }

  if (UDP_closelog < 0) UDP_closelog = 0;  // In case we haven't really started yet

  while (UDP_closelog <= UDP_removed_from_buff) {
    my_udp = &UDPbuf[UDP_closelog % UDP_num_slots];
    printf("num=%ld,slot=%d,freq=%d,rf=%d,time=%d:%d,e2u=%d:%d\n", UDP_closelog, ((my_udp->GPS_time >> 3) & 0b11), my_udp->freq_channel, my_udp->rf_input, my_udp->GPS_time,
           my_udp->subsec_time, my_udp->edt2udp_id, my_udp->edt2udp_token);

    UDP_closelog++;
  }

  // WIP Print out the next 20 udp buffers starting from UDP_removed_from_buffer (assuming they exist).
  // Before printing them do the ntoh conversion for all relevant fields
  // This will cause UDP_removed_from_buffer itself to be printed twice, once with and once without conversion

  //---------- Free up everything from the heap ----------

  free(msgvecs);  // The threads are dead, so nobody needs this memory now. Let it be free!
  free(iovecs);   // Give back to OS (heap)
  free(UDPbuf);   // This is the big one.  Probably many GB!

  free(sub);  // Free the metadata array storage area

  printf("Exiting process\n");
  fflush(stdout);

  exit(EXIT_SUCCESS);
}

// End of code.  Comments, snip and examples only follow
