//===================================================================================================================================================
// dw - disk write test program
//
// Author(s)  BWC Brian Crosse brian.crosse@curtin.edu.au
// Commenced 2016-07-13
//
// 1.00a-001    2019-01-30 BWC  Write big disk files as fast as possible to test disk write speed.
//
//===================================================================================================================================================
//
// To Compile:  gcc -Wall -Ofast dw.c -odw -lpthread -D_GNU_SOURCE
//
//              There should be NO warnings or errors on compile!
//
// To run:      Change to the directory to which you'd like to write the test files
//              ./dw
//
//===================================================================================================================================================

#define BUILD 1

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <stdio.h>
#include <pthread.h>
#include <stdlib.h>
#include <signal.h>

//#include <unistd.h>
//#include <time.h>

//---------------- Define our old friends -------------------

#define FALSE 0
#define TRUE !(FALSE)

#define BOOL int
#define INT8 char
#define UINT8 unsigned char
#define INT16 short
#define UINT16 unsigned short
#define INT32 int
#define UINT32 unsigned int
#define INT64 long long int
#define UINT64 unsigned long long int

//---------------------------------------------------------------------------------------------------------------------------------------------------
// These variables are shared by all threads.  "file scope"
// Some are only accessable via a Mutex lock.  Some are basically #defines in the form of variables.  Others are volatile.
//---------------------------------------------------------------------------------------------------------------------------------------------------

volatile BOOL terminate = FALSE;                        // Global request for everyone to close down

//---------------------------------------------------------------------------------------------------------------------------------------------------
// buff2disk - Locate buffers that need to be written to disk and do that.
//---------------------------------------------------------------------------------------------------------------------------------------------------

void *buff2disk()
{

    char my_file[300];                                          // Room for the name of the file we're creating
    int filedesc;                                               // The file descriptor we'll use for our file writes
    int files_so_far=0;                                         // Let's count how many 1 sec buffer files we've written since start up

    INT64 Buffsize = 10538192896;
    INT64 Buffwritten;
    INT64 Buff2go;
    INT64 writesize;
    INT64 maxsize = 8L * 1024L * 1024L * 1024L ;

    char *buffptr, *buffwptr;

   if (posix_memalign((void **)&buffptr,4096, Buffsize) !=0 ) {             // Malloc up the whole output file
      printf( "malloc failed\n" );
      return(0);
    }

    struct timespec start_time;                                 // Used for testing file IO performance
    struct timespec lap_time;                                   // Used for testing file IO performance
    struct timespec end_time;                                   // Used for testing file IO performance

    printf("buff2disk started\n");

    clock_gettime( CLOCK_REALTIME, &start_time);                        // When did we begin writing the file?
    end_time = start_time;

    while ( !terminate ) {                                              // There's a pthread_exit on terminate inside this loop if we ever want to leave.

          sprintf( my_file, "%d.sub", files_so_far++ );

//        filedesc = open( my_file, O_WRONLY | O_CREAT | O_EXCL                     , S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH );          // Open file is DIRECT mode.  This one is FAST! GOOD!
          filedesc = open( my_file, O_WRONLY | O_CREAT | O_EXCL | O_DIRECT | O_SYNC , S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH );          // Open file is DIRECT mode.  This one is FAST! GOOD!

          if (filedesc == -1) {
            printf("File open failed\n");
          } else {

            posix_fallocate( filedesc, 0, Buffsize );           // Preallocate the file size to warn the OS how big the file will be
            posix_fadvise( filedesc, 0, 0, POSIX_FADV_DONTNEED);        // Advise the OS that we don't need *it* to retain any buffers.  We will do that if necessary

            Buff2go = Buffsize;
            buffwptr= buffptr;

            while ( Buff2go > 0 ) {

              if ( Buff2go > maxsize ) {
                writesize = maxsize;
              } else {
                writesize = Buff2go;
              }

              Buffwritten = write( filedesc, buffwptr, writesize );

//              printf( "Disk write.  Wanted %lld.  Got %lld\n", Buff2go, Buffwritten );
              Buff2go -= Buffwritten;
              buffwptr += Buffwritten;
            }

            posix_fadvise( filedesc, 0, 0, POSIX_FADV_DONTNEED);        // Again advise the OS that *it* can reuse any buffers it took.  Presumably either or both this line or the advise above are unneeded

            close( filedesc );                                          // ...and we're done  :)

            lap_time = end_time;
            clock_gettime( CLOCK_REALTIME, &end_time);                  // When did we finish writing the file?

            printf( "Write took %ld ms for file %s\n",
              ((end_time.tv_sec - lap_time.tv_sec) * 1000) + ((end_time.tv_nsec - lap_time.tv_nsec)/1000000), my_file );

          }

    }

    return(0);
}

//---------------------------------------------------------------------------------------------------------------------------------------------------

void sig_handler(int signo)
{
  printf("\n\nAsked to shut down\n");
  terminate = TRUE;
}

// ------------------------ Start of world -------------------------

int main(int argc, char **argv)
{
    terminate = FALSE;

    signal(SIGINT, sig_handler);                // Tell the OS that we want to trap SIGINT calls

    pthread_t buff2disk_pt;

    pthread_create(&buff2disk_pt,NULL,buff2disk,NULL);

    while(!terminate) sleep(1);                 // The master thread currently does nothing! Zip! Nada!  What a waste eh?

    terminate = TRUE;                           // Dumb since we can only get here if terminate is already true

    pthread_join(buff2disk_pt,NULL);
    printf("Joined buff2disk\n");

    printf("Done\n");
}
