#ifndef __MWAX_UDP2SUB_H__
#define __MWAX_UDP2SUB_H__

#include <stdbool.h>
#include <stdint.h>

// Static configuration
#define LOG_OUTPUT stderr      // Where to send log messages
#define HOSTNAME_LENGTH 21     // Max length of hostname strings
#define MONITOR_IP "224.0.2.2" // Heartbeat destination address
#define MONITOR_PORT 8007      // Heartbeat destination port
#define MONITOR_TTL 3          // Heartbeat TTL

#define MAX_INPUTS (544LL)
#define UDP_PAYLOAD_SIZE (4096LL)
#define LIGHTSPEED (299792458000.0L)
#define SUBFILE_HEADER_SIZE (4096LL)
#define CS_SAMPLE_RATE (1280000LL)
#define OS_SAMPLE_RATE (1638400LL)
#define COARSECHAN_BANDWIDTH (1280000LL)
#define ULTRAFINE_BW (200ll)
#define BLOCKS_PER_SUB (160LL)
#define FFT_PER_BLOCK (10LL)
#define POINTINGS_PER_SUB (BLOCKS_PER_SUB * FFT_PER_BLOCK) // NB: POINTINGS_PER_SUB gives 1600 so that every 5ms we write a new delay to the sub file.
#define CS_SUB_LINE_SIZE ((CS_SAMPLE_RATE * 8LL * 2LL) / BLOCKS_PER_SUB)
#define OS_SUB_LINE_SIZE ((OS_SAMPLE_RATE * 8LL * 2LL) / BLOCKS_PER_SUB) // They might be 6400 samples each for critically sampled data or 8192 for 32/25 oversampled data. Ether way, the 5ms is constant while Ultrafine_bw is 200Hz
#define CS_NTIMESAMPLES ((CS_SAMPLE_RATE * 8LL) / BLOCKS_PER_SUB)
#define OS_NTIMESAMPLES ((OS_SAMPLE_RATE * 8LL) / BLOCKS_PER_SUB)
#define CS_UDP_PER_RF_PER_SUB ( ((CS_SAMPLE_RATE * 8LL * 2LL) / UDP_PAYLOAD_SIZE) + 2 )
#define OS_UDP_PER_RF_PER_SUB ( ((OS_SAMPLE_RATE * 8LL * 2LL) / UDP_PAYLOAD_SIZE) + 2 )
#define CS_SUBSECSPERSEC ( (CS_SAMPLE_RATE * 2LL)/ UDP_PAYLOAD_SIZE )
#define OS_SUBSECSPERSEC ( (OS_SAMPLE_RATE * 2LL)/ UDP_PAYLOAD_SIZE )
#define CS_SUBSECSPERSUB ( CS_SUBSECSPERSEC * 8LL )
#define OS_SUBSECSPERSUB ( OS_SUBSECSPERSEC * 8LL )

// UDP packet structure
#pragma pack(push,1)                   // We're writing this header into all our packets, so we want/need to force the compiler not to add it's own idea of structure padding
typedef struct pkt_data {              // Structure format for the MWA data packets
  uint8_t packet_type;                 // Packet type (0x00 == Binary Header, 0x20 == 2K samples of Voltage Data in complex 8 bit real + 8 bit imaginary format)
  uint8_t freq_channel;                // The current coarse channel frequency number [0 to 255 inclusive].  This number is available from the header, but repeated here to simplify archiving and monitoring multiple frequencies.
  uint16_t rf_input;                   // maps to tile/antenna and polarisation (LSB is 0 for X, 1 for Y)
  uint32_t GPS_time;                   // [second] GPS time of packet (bottom 32 bits only)
  uint16_t subsec_time;                // count from 0 to PACKETS_PER_SEC - 1 (from header)  Typically PACKETS_PER_SEC is 625 in MWA
  uint8_t spare1;                      // spare padding byte.  Reserved for future use.
  uint8_t edt2udp_id;                  // Which edt2udp instance generated this packet and therefore a lookup to who to ask for a retransmission (0 implies no retransmission possible)
  uint16_t edt2udp_token;              // value to pass back to edt2udp instance to help identify source packet.  A token's value is constant for a given source rf_input and freq_channel for entire sub-observation
  uint8_t spare2[2];                   // Spare bytes for future use
  uint16_t volts[2048];                // 2048 complex voltage samples.  Each one is 8 bits real, followed by 8 bits imaginary but we'll treat them as a single 16 bit 'bit pattern'
} pkt_data_t;

// Health packet structure
typedef struct pkt_health {              // Health packet data
  uint16_t version;                    // U2S build version
  uint16_t instance;                   // Instance ID
  uint8_t  coarse_chan;                // Coarse chan from 01 to 24.
  char     hostname[HOSTNAME_LENGTH];  // Host name is looked up to against these strings (21 bytes)
  uint64_t udp_count;                  // Cumulative total UDP packets collected from the NIC
  uint64_t udp_dummy;                  // Cumulative total dummy packets inserted to pad out subobservations
  uint32_t discarded_subobs;           // Cumulative total subobservations discarded for being too old
} pkt_health_t;
#pragma pack(pop)                               // Set the structure packing back to 'normal' whatever that is



/** Per-instance runtime configuration. */
typedef struct instance_config {               // Structure for the configuration of each udp2sub instance.
  int udp2sub_id;                              // Which correlator id number we are.  Probably from 01 to 26, at least initially.
  char hostname[HOSTNAME_LENGTH];              // Host name is looked up against these strings to select the correct line of configuration settings
  int host_instance;                           // Is compared with a value that can be put on the command line for multiple copies per server
  uint64_t UDP_num_slots;                      // The number of UDP buffers to assign.  Must be ~>=3000000 for 128T array
  uint32_t cpu_mask_parent;                    // Allowed cpus for the parent thread which reads metafits file
  uint32_t cpu_mask_UDP_recv;                  // Allowed cpus for the thread that needs to pull data out of the NIC super fast
  uint32_t cpu_mask_UDP_parse;                 // Allowed cpus for the thread that checks udp packets to create backwards pointer lists
  uint32_t cpu_mask_makesub;                   // Allowed cpus for the thread that writes the sub files out to memory
  char shared_mem_dir[40];                     // The name of the shared memory directory where we get .free files from and write out .sub files
  char temp_file_name[40];                     // The name of the temporary file we use.  If multiple copies are running on each server, these may need to be different
  char stats_file_name[40];                    // The name of the shared memory file we use to write statistics and debugging to.  Again if multiple copies per server, it may need different names
  char spare_str[40];                          //
  char metafits_dir[60];                       // The directory where metafits files are looked for.  NB Likely to be on an NFS mount such as /vulcan/metafits
  char local_if[20];                           // Address of the local NIC interface we want to receive the multicast stream on.
  int coarse_chan;                             // Which coarse chan from 01 to 24.
  char multicast_ip[30];                       // The multicast address and port (below) we wish to join.
  uint16_t multicast_port;                     // Multicast port address
  char monitor_if[20];                         // Local interface address for monitoring packets
} instance_config_t;

/** Application state */
typedef struct app_state {
  volatile uint64_t UDP_added_to_buff = 0;                // Total number of packets received and placed in the buffer.  If it overflows we are in trouble and need a restart.
  volatile uint64_t UDP_removed_from_buff = 0;            // Total number of packets pulled from buffer and space freed up.  If it overflows we are in trouble and need a restart.
  uint64_t UDP_num_slots;                                 // Must be at least 3000000 for mwax07 with 128T. 3000000 will barely make it!
  bool terminate;                                         // Set to true to terminate the program
  uint32_t GPS_offset;                                    // Logging only.  Needs to be updated on leap seconds
  subobs_udp_meta_t *sub;                                 // Pointer to the four subobs metadata arrays
  // Data buffers
  struct mmsghdr *msgvecs;
  struct iovec *iovecs;
  pkt_health_t monitor;     // Health packet buffer
  pkt_data_t *UDPbuf;       // Data packet storage buffer
  char *blank_sub_line;     // Zero-filled data block line.
  char *sub_header;         // Pointer to a buffer that's the size of a sub file header.
  // System configuration
  char *hostname;
  int instance_id;
  char *config_path;
  int prog_build;          // Build number
  bool debug_mode;         // Enable extra logging
  instance_config_t cfg;
  delaygen_config_t delaygen_cfg;
  // Configuration overrides
  bool force_cable_delays;   // Always apply cable delays
  bool force_geo_delays;     // Always apply geometric delays
  bool force_channel_id;     // Use the specified channel selection
  bool force_instance_id;    // Use the specified instance id selection
  int chan_id_override;      // Channel override value
  int instance_id_override;  // Instance override value
} app_state_t;

/** Delay generator configuration. */
typedef struct delaygen_config {
  bool enabled;                               // Is the delay generator enabled?
  uint64_t obsid;                             // The observation ID to generate delays for.
  uint32_t subobs_idx;                        // The subobservation index to generate delays for.
} delaygen_config_t;

typedef struct altaz_meta {				// Structure format for the metadata associated with the pointing at the beginning, middle and end of the sub-observation
    uint64_t gpstime;
    float Alt;
    float Az;
    float Dist_km;
    long double SinAzCosAlt;				// will be multiplied by tile East
    long double CosAzCosAlt;				// will be multiplied by tile North
    long double SinAlt;					// will be multiplied by tile Height
} altaz_meta_t;

/** Tile metadata */
typedef struct tile_meta {                              // Structure format for the metadata associated with one rf input for one subobs
    int Input;
    int Antenna;
    int Tile;
    char TileName[9];                                   // Leave room for a null on the end!
    char Pol[2];                                        // Leave room for a null on the end!
    int Rx;
    int Slot;
    int Flag;
    long double Length_f;				// Floating point format version of the weird ASCII 'EL_###' length string above.
    long double North;
    long double East;
    long double Height;
    int Gains[24];
    float BFTemps;
    int Delays[16];
    char Flavors[11];
    float Calib_Delay;
    float Calib_Gains[24];
    uint16_t rf_input;                                  // What's the tile & pol identifier we'll see in the udp packets for this input?
    int16_t ws_delay;					// The whole sample delay IN SAMPLES (NOT BYTES). Can be -ve. Each extra delay will move the starting position later in the sample sequence
    double initial_delay;
    double delta_delay;
    double delta_delta_delay;
    double start_total_delay;
    double middle_total_delay;
    double end_total_delay;
} tile_meta_t;

// Subobservation metadata
typedef struct subobs_udp_meta {                        // Structure format for the MWA subobservation metadata that tracks the sorted location of the udp packets
    volatile uint32_t subobs;                           // The sub observation number.  ie the GPS time of the first second in this sub-observation
    volatile int state;                                 // 0==Empty, 1==Adding udp packets, 2==closed off ready for sub write, 3==sub write in progress, 4==sub write complete and ready for reuse, >=5 is failed
    int msec_wait;                                      // The number of milliseconds the sub write thread had waited doing nothing before starting to write out this sub.  Only valid for state = 3 or 4 or 5 or higher
    int msec_took;                                      // The number of milliseconds the sub write thread took to write out this sub.  Only valid for state = 4 or 5 or higher
    int64_t first_udp;
    int64_t last_udp;
    int64_t udp_at_start_write;
    int64_t udp_at_end_write;
    int udp_count;                                      // The number of udp packets collected from the NIC
    int udp_dummy;                                      // The number of dummy packets we needed to insert to pad things out
    volatile int meta_done;                             // Has metafits data been appended to this sub observation yet?  0 == No.
    int meta_msec_wait;                                 // The number of milliseconds it took to from reading the last metafits to finding a new one
    int meta_msec_took;                                 // The number of milliseconds it took to read the metafits
    int64_t GPSTIME;                                      // Following fields are straight from the metafits file (via the metabin) This is the GPSTIME *FOR THE OBS* not the subobs!!!
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
    int64_t UNIXTIME;
    int COARSE_CHAN;
    int FINECHAN_hz;
    int INTTIME_msec;
    uint16_t rf_seen;                                     // The number of different rf_input sources seen so far this sub observation
    uint16_t rf2ndx[65536];                               // A mapping from the rf_input value to what row in the pointer array its pointers are stored
    //char **udp_volts[MAX_INPUTS+1][UDP_PER_RF_PER_SUB];    // array of pointers to every udp packet's payload that may be needed for this sub-observation.  NB: THIS ARRAY IS IN THE ORDER INPUTS WERE SEEN STARTING AT 1!, NOT THE SUB FILE ORDER!
    char **udp_volts[MAX_INPUTS+1];                       // array of pointers to every udp packet's payload that may be needed for this sub-observation.  NB: THIS ARRAY IS IN THE ORDER INPUTS WERE SEEN STARTING AT 1!, NOT THE SUB FILE ORDER!
    tile_meta_t rf_inp[MAX_INPUTS];                       // Metadata about each rf input in an array indexed by the order the input needs to be in the output sub file, NOT the order udp packets were seen in.
    altaz_meta_t altaz[3];		                        		// The AltAz at the beginning, middle and end of the 8 second sub-observation

    // All of these former build-time constants are now part of the subobservation metadata
    uint64_t sample_rate;
    uint64_t sub_line_size;
    uint64_t ntimesamples;
    uint64_t udp_per_rf_per_sub;
    uint64_t subsecpersec;
    uint64_t subsecpersub;
} subobs_udp_meta_t;

#pragma pack(push,1)

// structure for each signal path
typedef struct delay_table_entry {
    uint16_t rf_input;
    int16_t ws_delay_applied;      // The whole-sample delay for this signal path, for the entire sub-observation.  Will often be negative.
    double start_total_delay;
    double middle_total_delay;
    double end_total_delay;
    double initial_delay;          // Initial residual delay at centre of first 5 ms timestep
    double delta_delay;            // Increment between timesteps
    double delta_delta_delay;      // Increment in the increment between timesteps
    int16_t num_pointings;         // Initially 1 but might grow to 10 or more if beamforming.  The first pointing is for delay tracking in the correlator.
    int16_t reserved;
    float frac_delay[POINTINGS_PER_SUB];        // 1600 fractional delays in fractions of a whole sample time.  Not supposed to be out of range of -1 to 1 samples (inclusive).
} delay_table_entry_t;
#pragma pack(pop)


typedef struct MandC_meta {                             // Structure format for the MWA subobservation metadata that tracks the sorted location of the udp packets.  Array with one entry per rf_input

    uint16_t rf_input;                                  // tile/antenna and polarisation (LSB is 0 for X, 1 for Y)
    int start_byte;                                     // What byte (not sample) to start at.  Remember we store a whole spare packet before the real data starts
    uint16_t seen_order;                                // For this subobs (only). What order was this rf input seen in?  That tells us where we need to look in the udp_volts pointer array

} MandC_meta_t;

// Log levels
typedef enum {
  LOG_ERROR = 0,
  LOG_WARNING,
  LOG_INFO,
  LOG_DEBUG
} log_level_t;

// Packet types
#define PACKET_TYPE_CRITICAL    0x20
#define PACKET_TYPE_OVERSAMPLED 0x30

// Other constants
#define CLOSEDOWN (0xFFFFFFFF)

// Common functions
void log_message(const char *tag, log_level_t level, const char *fmt, ...);
int set_cpu_affinity(const char *name, uint32_t mask);
int load_config_file(char *path, instance_config_t **config_records);
long double get_path_difference(long double north, long double east, long double height, long double alt, long double az);

// Thread functions
void *task_receive_buffer(void *args);
void *task_parse_udp(void *args);
void *task_add_metafits(void *args);
void *task_make_sub(void *args);
void *task_heartbeat(void *args);

// Logging helper macros.
// Place a `const char *TAG = "module";` in the source file to use these.
#define log_error(...)   log_message(TAG, LOG_ERROR,   __VA_ARGS__)
#define log_warning(...) log_message(TAG, LOG_WARNING, __VA_ARGS__)
#define log_info(...)    log_message(TAG, LOG_INFO,    __VA_ARGS__)
#define log_debug(...)   log_message(TAG, LOG_DEBUG,   __VA_ARGS__)


#endif // __MWAX_UDP2SUB_H__