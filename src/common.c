/** Common functions used in mwax_udp2sub.
 * These functions are used by multiple modules in mwax_udp2sub.
 */

#define _GNU_SOURCE

#include <stdint.h>
#include <stdarg.h>
#include <stdio.h>
#include <string.h>
#include <time.h>
#include <pthread.h>

#include "vec3.h"
#include "mwax_udp2sub.h"

const char *TAG = "common";

// Forward declartions
static char *log_level_to_string(log_level_t level);

/** Log a message to the configured output.
 * The message will be prefixed with the tag and log level, and a newline
 * will be appended.
 */
void log_message(const char *tag, log_level_t level, const char *fmt, ...) {
  time_t rawtime;
  time(&rawtime);
  struct tm *time_info = localtime(&rawtime);
  char time_buffer[80];
  strftime(time_buffer, 80,"%c", time_info);

  va_list args;
  va_start(args, fmt);
  fprintf(LOG_OUTPUT, "%s [%s] %s: ", time_buffer, tag, log_level_to_string(level));
  vfprintf(LOG_OUTPUT, fmt, args);
  fprintf(LOG_OUTPUT, "\n");
  fflush(LOG_OUTPUT);
  va_end(args);
}

/** Set the CPU affinity for a thread. */
int set_cpu_affinity(const char *name, uint32_t mask) {
  int result;
  cpu_set_t cpu_set;                        // Make a CPU affinity set for this thread
  CPU_ZERO(&cpu_set);                       // Zero it out completely
  for(uint8_t loop = 0; loop < 32; loop++)  // Support up to 32 cores for now.  We need to step through them from LSB to MSB
    if(((mask >> loop) & 0x01) == 0x01)     // If that bit is set
      CPU_SET(loop, &cpu_set);              // then add it to the allowed list
  pthread_t thread = pthread_self();        // Look up my own TID / LWP
  result = pthread_setaffinity_np(thread, sizeof(cpu_set_t), &cpu_set);
  if(result >= 0) {
    log_info("Set CPU affinity for thread '%s' to 0x%08x", name, mask);
  } else {
    log_error("Failed to set CPU affinity for thread '%s' with error code %d (%s)", name, result, strerror(result));
  }  
  return result;
}

/** Convert a log level to a string. */
static char *log_level_to_string(log_level_t level) {
  switch(level) {
    case LOG_ERROR:   return "ERROR";
    case LOG_WARNING: return "WARNING";
    case LOG_INFO:    return "INFO";
    case LOG_DEBUG:   return "DEBUG";
    default:          return "UNKNOWN";
  }
}

long double get_path_difference(long double north, long double east, long double height, long double alt, long double az) {
  vec3_t A = (vec3_t) { north, east, height };
  vec3_t B = (vec3_t) { 0, 0, 0 };
  vec3_t AB = vec3_subtract(B, A);
  vec3_t ACn = vec3_unit(deg2rad(alt), deg2rad(az));
  long double ACr = vec3_dot(AB, ACn);
  return ACr;
}