// gcc -Wall -Ofast test.c -otest -lrt

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <errno.h>
#include <string.h>
#include <fcntl.h>
#include <signal.h>
#include <ctype.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <time.h>

#define FALSE 0
#define TRUE !(FALSE)

#define BUILD 1

#define BOOL int
#define INT8 char
#define UINT8 unsigned char
#define INT16 short
#define UINT16 unsigned short
#define INT32 int
#define UINT32 unsigned int
#define INT64 long long int
#define UINT64 unsigned long long int

#define HEADER_LEN 4096

int main(int argc, char **argv)
{
//    char filename[300];

    if ( argc < 2 || strlen( argv[1] ) != 28 ) {
      printf ( "bad command line\nargc = %d",argc );
      exit(0);
    }

    int file_descr;

    int input_write;

    char header_buffer[HEADER_LEN]={0x00};

    int GPS_offset = 315964782;
    int GMT_offset = (8 * 60 * 60);
    int chan = atoi(argv[1]+22);

    int obs_id = atoi(argv[1]);
    int subobs_id = atoi(argv[1]+11);

    if ( chan == 0 || obs_id == 0 || subobs_id == 0 ) {
      printf ( "bad command line\n" );
      exit(0);
    }

    int obs_offset = subobs_id - obs_id;
    int exposure_secs = 120;
    int coarse_channel = 103 + chan;
    int corr_coarse_channel = chan;
    int unixtime = obs_id + GPS_offset;

    long int my_time = obs_id + GPS_offset - GMT_offset;
    char* t = ctime(&my_time);

    char months[] = "Jan,Feb,Mar,Apr,May,Jun,Jul,Aug,Sep,Oct,Nov,Dec ";
    char srch[4]={0,0,0,0};
    memcpy (srch,&t[4],3) ;
    int mnth = ( strstr ( months, srch ) - months )/4 + 1;

    char utc_start[200];
    sprintf( utc_start, "%.4s-%02d-%.2s-%.8s", &t[20], mnth, &t[8], &t[11] );

    char* head_mask =
      "HDR_SIZE 4096\n"
      "POPULATED 1\n"
      "OBS_ID %d\n"
      "SUBOBS_ID %d\n"
      "COMMAND CAPTURE\n"
      "UTC_START %s\n"
      "OBS_OFFSET %d\n"
      "NBIT 8\n"
      "NPOL 2\n"
      "NTIMESAMPLES 51200\n"
      "NINPUTS 256\n"
      "NINPUTS_XGPU 256\n"
      "APPLY_PATH_WEIGHTS 0\n"
      "APPLY_PATH_DELAYS 0\n"
      "INT_TIME_MSEC 1000\n"
      "FSCRUNCH_FACTOR 40\n"
      "APPLY_VIS_WEIGHTS 0\n"
      "TRANSFER_SIZE 5269094400\n"
      "PROJ_ID C001\n"
      "EXPOSURE_SECS %d\n"
      "COARSE_CHANNEL %d\n"
      "CORR_COARSE_CHANNEL %d\n"
      "SECS_PER_SUBOBS 8\n"
      "UNIXTIME %d\n"
      "UNIXTIME_MSEC 0\n"
      "FINE_CHAN_WIDTH_HZ 10000\n"
      "NFINE_CHAN 128\n"
      "BANDWIDTH_HZ 1280000\n"
      "SAMPLE_RATE 1280000\n"
      "MC_IP 0.0.0.0\n"
      "MC_PORT 0\n"
      "MC_SRC_IP 0.0.0.0\n"
      ;

    sprintf( header_buffer, head_mask, obs_id, subobs_id, utc_start, obs_offset, exposure_secs, coarse_channel, corr_coarse_channel, unixtime );

    printf( "--------------------\n%s\n--------------------\n", header_buffer );

    file_descr = open( argv[1], O_WRONLY );
    if (file_descr < 0) {
        printf( "Error opening destination file:%s\n", argv[2] );
        exit(0);
    }

    input_write = write ( file_descr, header_buffer, HEADER_LEN );
    if (input_write != HEADER_LEN) {
        printf( "Write to %s failed.  Returned a value of %d.\n", argv[2], input_write );
        exit(0);
    }

    close ( file_descr );

    printf( "Header updated successfully\n" );

    exit(0);
}


