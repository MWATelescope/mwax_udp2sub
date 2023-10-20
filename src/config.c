#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "mwax_udp2sub.h"

const char *TAG = "config";

/** Load instance configuration database from a CSV file.
 * 
 * @param path [in] Path to the CSV file to load.
 * @param config_records [out] Pointer for the loaded configuration records.
 */
static int load_config_db(char *path, instance_config_t **config_records) {
  log_info("Reading configuration from %s", path);

  // Read the whole input file into a buffer.
  char *data, *datap;
  FILE *file;
  size_t sz;
  file = fopen(path, "r");
    if(file == NULL) {
    log_error("Error loading configuration. Unable to open %s", path);
    return 1;
  }
  fseek(file, 0, SEEK_END);
  sz = ftell(file);
  rewind(file);
  if(sz == -1) {
    log_error("Error loading configuration. Failed to determine file size for %s", path);
    return 2;
  }
  data = datap = malloc(sz + 1);
  data[sz] = '\n'; // Simplifies parsing slightly
  if(fread(data, 1, sz, file) != sz) {
    log_error("Error loading configuration. Read error loading %s", path);
    return 3;
  }
  fclose(file);

  instance_config_t *records;
  int max_rows = sz / 28;                   // Minimum row size is 28, pre-allocate enough
  int row_sz = sizeof(instance_config_t);   // room for the worst case and `realloc` later
  records = calloc(max_rows, row_sz);      // when the size is known.

  int row = 0, col = 0;              // Current row and column in input table
  char *sep = ",";                   // Next expected separator
  char *end = NULL;                  // Mark where `strtol` stops parsing
  char *tok = strsep(&datap, sep);   // Pointer to start of next value in input
  while (row < max_rows) {
    while (col < 18) {
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
        records[row].multicast_port = strtol(tok, &end, 10);
        if (end == NULL || *end != '\0') goto done;
        break;
      case 17:
        strcpy(records[row].monitor_if, tok);
        break;
      }

      if (col == 16)      // If we've parsed the second-to-last column,
        sep = "\n";       // the next separator to expect will be LF.
      else if (col == 17) // If we've parsed the last column, the next
        sep = ",";        // separator will be a comma again.
      col++;

      tok = strsep(&datap, sep); // Get the next token from the input and
      if (tok == NULL) goto done; // abort the row if we don't find one.
    }
    if (col == 18) col = 0; // Wrap to column 0 if we parsed a full row
    else break;             // or abort if we didn't.
    row++;
  }
done:
  *config_records = realloc(records, (row + 1) * sizeof(instance_config_t));
  free(data);
  fprintf(stderr, "%d instance record(s) found.\n", row + 1);
  return row + 1;
}


/** Load configuration for the running instance.
 * 
 * This function should be called after processing the command line arguments
 * and any other options to take precedence over the default behaviour. These
 * are taken from the app state, which is updated if a matching configuration
 * record is found.
 */
void config_load(app_state_t *state) {
  instance_config_t *records = NULL;
  char *path = state->config_path;
  char *host = state->hostname;
  int16_t instance_idx = -1; // Initially assume there is no matching record
  uint16_t count = load_config_db(path, &records);

  if(records == NULL) {
    log_error("Failed to load configuration database from %s: no records found.", path);
    exit(-1);
  }

  for(int i=0; i < count; i++) {
    instance_config_t *record = &records[i];
    if((strcmp(record->hostname, host) == 0) && (record->host_instance == state->instance_id)) {
      instance_idx = i;
      break;
    }
  }

  state->cfg = records[instance_idx]; // Copy the config record into the app state;

  if(state->force_channel_id) {
    uint16_t chan = state->chan_id_override;
    state->cfg.coarse_chan = chan;
    sprintf(state->cfg.multicast_ip, "239.255.90.%d", chan); // Multicast ip is 239.255.90.xx
    state->cfg.multicast_port = 59000 + chan;                // Multicast port address is the forced coarse channel number plus an offset of 59000
  }
  pkt_health_t *health = &state->monitor;
  health->coarse_chan = state->cfg.coarse_chan;
  memcpy(health->hostname, host, HOSTNAME_LENGTH);
  health->instance = state->cfg.host_instance;
}