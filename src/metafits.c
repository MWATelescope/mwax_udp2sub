#define _GNU_SOURCE

#include <stdint.h>
#include <dirent.h>
#include <stdio.h>
#include <string.h>
#include <math.h>
#include <time.h>
#include <stdbool.h>
#include <pthread.h>
#include <unistd.h>

#include <fitsio.h>

#include "mwax_udp2sub.h"

const char *TAG = "metafits-task";

#define FITS_CHECK(OP) if (status) { log_error("Query operation on FITS file failed: %s", OP); fits_report_error(stderr, status); status = 0; }

void *task_add_metafits(void *args) {
    app_state_t *state = (app_state_t *)args;
    instance_config_t conf = state->cfg;

    log_info("Initialising...");

    DIR *dir;
    struct dirent *dp;
    int len_filename;
    char metafits_file[300];                                            // The metafits file name
    int64_t bcsf_obsid;                                                   // 'best candidate so far' for the metafits file
    int64_t mf_obsid;
    int loop;
    int subobs_ready2write;
    int meta_result;                                                    // Temp storage for the result before we write it back to the array for others to see.  Once we do that, we can't change our mind.
    bool go4meta;
    subobs_udp_meta_t *subm;                                            // pointer to the sub metadata array I'm working on
    struct timespec started_meta_write_time;
    struct timespec ended_meta_write_time;
    static long double two_over_num_blocks_sqrd = ( ((long double) 2L) / ( (long double) (BLOCKS_PER_SUB*FFT_PER_BLOCK) * (long double) (BLOCKS_PER_SUB*FFT_PER_BLOCK) ) ); // Frequently used during fitting parabola
    static long double one_over_num_blocks = ( ((long double) 1L) / (long double) (BLOCKS_PER_SUB*FFT_PER_BLOCK) );      // Frequently used during fitting parabola
    static long double res_max = (long double) 2L;                                                      // Frequently used in range checking. Largest allowed residual.
    static long double res_min = (long double) -2L;                                                     // Frequently used in range checking. Smallest allowed residual.
    long double d2r = M_PIl / 180.0L;					// Calculate a long double conversion factor from degrees to radians (we'll use it a few times)
    long double SinAlt, CosAlt, SinAz, CosAz;				// temporary variables for storing the trig results we need to do delay tracking
    long double a,b,c;							// coefficients of the delay fitting parabola
    fitsfile *fptr;                                                                             // FITS file pointer, defined in fitsio.h
    int status;                                                                                 // CFITSIO status value MUST be initialized to zero!
    int hdupos;
    char channels_ascii[200];                                           // Space to read in the metafits channel string before we parse it into individual channels
    unsigned int ch_index;                                              // assorted variables to assist parsing the channel ascii csv into something useful like an array of ints
    char* token;
    char* saveptr;
    char* ptr;
    int temp_CHANNELS[24];
    int course_swap_index;

//---------------- Main loop to live in until shutdown -------------------

    clock_gettime( CLOCK_REALTIME, &ended_meta_write_time);             // Fake the ending time for the last metafits file ('cos like there wasn't one y'know but the logging will expect something)

    while (!state->terminate) {                                                // If we're not supposed to shut down, let's find something to do

//---------- look for a sub which needs metafits info ----------

      subobs_ready2write = -1;                                          // Start by assuming there's nothing to do

      for(loop = 0; loop < 4; loop++) {                            // Look through all four subobs meta arrays
        if ( ( state->sub[loop].state == SLOT_ACTIVE ) && ( state->sub[loop].meta_done == SLOT_META_ABSENT ) ) {       // If this sub is ready to have M&C metadata added
          if ( subobs_ready2write == -1 ) {                             // check if we've already found a different one to do and if we haven't
            subobs_ready2write = loop;                                  // then mark this one as the best so far
          } else {                                                      // otherwise we need to work out
            if ( state->sub[subobs_ready2write].subobs > state->sub[loop].subobs ) {  // if that other one was for a later sub than this one
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
        subm = &state->sub[subobs_ready2write];                                // Temporary pointer to our sub's metadata array
        subm->meta_msec_wait = ( ( started_meta_write_time.tv_sec - ended_meta_write_time.tv_sec ) * 1000 ) + ( ( started_meta_write_time.tv_nsec - ended_meta_write_time.tv_nsec ) / 1000000 );        // msec since the last metafits processed
        subm->meta_done = SLOT_META_IN_PROGRESS;                        // Record that we're working on this one!
        meta_result = SLOT_META_FAILED;                                                // Start by assuming we failed  (ie result==5).  If we get everything working later, we'll swap this for a 4.
        go4meta = true;                                                 // All good so far

//---------- Look in the metafits directory and find the most applicable metafits file ----------

        if(go4meta) {                                                                                // If everything is okay so far, enter the next block of code
          go4meta = false;                                                                              // but go back to assuming a failure unless we succeed in the next bit
          bcsf_obsid = 0;                                                                               // 'best candidate so far' is very bad indeed (ie non-existent)
          dir = opendir( conf.metafits_dir );                                                           // Open up the directory where metafits live
          if ( dir == NULL ) {                                                                          // If the directory doesn't exist we must be running on an incorrectly set up server
            log_error("FATAL: Directory %s does not exist", conf.metafits_dir);
            state->terminate = true;                                                                           // Tell every thread to close down
            pthread_exit(NULL);                                                                         // and close down ourselves.
          }

          while ( (dp=readdir(dir)) != NULL) {                                                          // Read an entry and while there are still directory entries to look at
            if ( ( dp->d_type == DT_REG ) && ( dp->d_name[0] != '.' ) ) {                                       // If it's a regular file (ie not a directory or a named pipe etc) and it's not hidden
              if ( ( (len_filename = strlen( dp->d_name )) >= 15) && ( strcmp( &dp->d_name[len_filename-14], "_metafits.fits" ) == 0 ) ) {           // If the filename is at least 15 characters long and the last 14 characters are "_metafits.fits"
                mf_obsid = strtoll( dp->d_name,0,0 );                                                   // Get the obsid from the name
                if ( mf_obsid <= subm->subobs ) {                                                       // if the obsid is less than or the same as our subobs id, then it's a candidate for the one we need
                  if ( mf_obsid > bcsf_obsid ) {                                                        // If this metafits is later than the 'best candidate so far'?
                    bcsf_obsid = mf_obsid;                                                              // that would make it our new best candidate so far
                    go4meta = true;                                                                     // We've found at least one file worth looking at.
                  }
                }
              }
            }
          }
          closedir(dir);
        }

//---------- Open the 'best candidate so far' metafits file ----------

        if ( go4meta ) {                                                                                // If everything is okay so far, enter the next block of code
          go4meta = false;                                                                              // but go back to assuming a failure unless we succeed in the next bit
          sprintf( metafits_file, "%s/%lld_metafits.fits", conf.metafits_dir, bcsf_obsid );             // Construct the full file name including path
          status = 0;                                                                                   // CFITSIO status value MUST be initialized to zero!

          if ( !fits_open_file( &fptr, metafits_file, READONLY, &status ) ) {                           // Try to open the file and if it works
            fits_get_hdu_num( fptr, &hdupos );                                                          // get the current HDU position
                                                                                                        // terminate listing with END

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

if (state->debug_mode || state->force_cable_delays) subm->CABLEDEL = 1;

            if (status) {
              printf ( "Failed to read CABLEDEL\n" );
              fflush(stdout);
            }

            fits_read_key( fptr, TINT, "GEODEL", &(subm->GEODEL), NULL, &status );			// Read the GEODEL field. (0=nothing, 1=zenith, 2=tile-pointing, 3=az/el table tracking)

if (state->debug_mode || state->force_geo_delays) subm->GEODEL = 3;

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

            if ( (subm->GPSTIME + (int64_t)subm->EXPOSURE - 1) < (int64_t)subm->subobs ) {                  // If the last observation has expired. (-1 because inclusive)
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
            FITS_CHECK("read_key UNIXTIME");
//---------- We now have everything we need from the 1st HDU ----------

            int hdutype=0;
            int colnum;
            int anynulls;
            long nrows;
            long ntimes;

            long frow, felem;

            int cfitsio_ints[MAX_INPUTS];                                       // Temp storage for integers read from the metafits file (in metafits order) before copying to final structure (in sub file order)
            int64_t cfitsio_J[3];							// Temp storage for long "J" type integers read from the metafits file (used in pointing HDU)
            float cfitsio_floats[MAX_INPUTS];                                   // Temp storage for floats read from the metafits file (in metafits order) before copying to final structure (in sub file order)

            char cfitsio_strings[MAX_INPUTS][15];                               // Temp storage for strings read from the metafits file (in metafits order) before copying to final structure (in sub file order)
            char *cfitsio_str_ptr[MAX_INPUTS];                                  // We also need an array of pointers to the stings
            for (int loop = 0; loop < MAX_INPUTS; loop++) {
              cfitsio_str_ptr[loop] = &cfitsio_strings[loop][0];                // That we need to fill with the addresses of the first character in each string in the list of inputs
            }

            int metafits2sub_order[MAX_INPUTS];                                 // index is the position in the metafits file starting at 0.  Value is the order in the sub file starting at 0.

            fits_movrel_hdu( fptr, 1 , &hdutype, &status );                             // Shift to the next HDU, where the antenna table is
            if (status) {
              FITS_CHECK("Move to 2nd HDU");
            }

            fits_get_num_rows( fptr, &nrows, &status );
            FITS_CHECK("get_num_rows 2nd HDU");
            if ( nrows != subm->NINPUTS ) {
              printf ( "NINPUTS (%d) doesn't match number of rows in tile data table (%ld)\n", subm->NINPUTS, nrows );
            }

        frow = 1;
        felem = 1;
//        nullval = -99.;

        fits_get_colnum( fptr, CASEINSEN, "Antenna", &colnum, &status );
        FITS_CHECK("get_colnum Antenna");
        fits_read_col( fptr, TINT, colnum, frow, felem, nrows, 0, cfitsio_ints, &anynulls, &status );

        fits_get_colnum( fptr, CASEINSEN, "Pol", &colnum, &status );
        FITS_CHECK("get_colnum Pol");
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

//---------- Read and write the 'Flavors' field --------

        fits_get_colnum( fptr, CASEINSEN, "Flavors", &colnum, &status );
        FITS_CHECK("get_colnum FLAVORS");
        fits_read_col( fptr, TSTRING, colnum, frow, felem, nrows, 0, &cfitsio_str_ptr, &anynulls, &status );
        for (int loop = 0; loop < nrows; loop++) strcpy( subm->rf_inp[ metafits2sub_order[loop] ].Flavors, cfitsio_str_ptr[loop] );

//---------- Read and write the 'Calib_Delay' field --------

        fits_get_colnum( fptr, CASEINSEN, "Calib_Delay", &colnum, &status );
        FITS_CHECK("get_colnum CALIB_DELAY");
        fits_read_col( fptr, TFLOAT, colnum, frow, felem, nrows, 0, cfitsio_floats, &anynulls, &status );
        for (int loop = 0; loop < nrows; loop++) subm->rf_inp[ metafits2sub_order[loop] ].Calib_Delay = cfitsio_floats[loop];           // Copy each float from the array we got from the metafits (via cfitsio) into one element of the rf_inp array structure

//---------- Read and write the 'Calib_Gains' field --------

        fits_get_colnum( fptr, CASEINSEN, "Calib_Gains", &colnum, &status );
        FITS_CHECK("get_colnum CALIB_GAINS");
        // Like 'Gains' and 'Delays, this is an array. This time of floats.
        for (int loop = 0; loop < nrows; loop++) {
          fits_read_col( fptr, TFLOAT, colnum, loop+1, felem, 24, 0, subm->rf_inp[ metafits2sub_order[loop] ].Calib_Gains, &anynulls, &status );
        }

//---------- Now we have read everything available from the 2nd HDU but we want to do some conversions and calculations per tile.  That can wait until after we read the 3rd HDU ----------
//           We need to read in the AltAz information (ie the 3rd HDU) for the beginning, middle and end of this subobservation
//           Note the indent change caused by moving code around. Maybe I'll fix that later... Maybe not.

            fits_movrel_hdu( fptr, 1 , &hdutype, &status );                             // Shift to the next HDU, where the AltAz table is
            FITS_CHECK("Move to 3nd HDU");


            fits_get_num_rows( fptr, &ntimes, &status );				// How many rows (times) are written to the metafits?
            DEBUG_LOG("ntimes=%ld\n", ntimes);

            // They *should* start at GPSTIME and have an entry every 4 seconds for EXPOSURE seconds, inclusive of the beginning and end.  ie 3 times for 8 seconds exposure @0sec, @4sec & @8sec
            // So the number of them should be '(subm->EXPOSURE >> 2) + 1'
            // The 3 we want for the beginning, middle and end of this subobs are '((subm->subobs - subm->GPSTIME)>>2)+1', '((subm->subobs - subm->GPSTIME)>>2)+2' & '((subm->subobs - subm->GPSTIME)>>2)+3'
            // a lot of the time, we'll be past the end of the exposure time, so if we are, we'll need to fill the values in with something like a zenith pointing.

            if ( ( subm->GEODEL == 1 ) ||												// If we have been specifically asked for zenith pointings *or*
              ( (((subm->subobs - subm->GPSTIME) >>2)+3) > ntimes ) ) {									// if we want times which are past the end of the list available in the metafits
              DEBUG_LOG("Not going to do delay tracking!! GEODEL=%d subobs=%d GPSTIME=%lld ntimes=%ld\n", subm->GEODEL, subm->subobs, subm->GPSTIME, ntimes);
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
//
                subm->altaz[ loop ].SinAzCosAlt = SinAz * CosAlt;									// this conversion factor will be multiplied by tile East
                subm->altaz[ loop ].CosAzCosAlt = CosAz * CosAlt;									// this conversion factor will be multiplied by tile North
                subm->altaz[ loop ].SinAlt = SinAlt;											// this conversion factor will be multiplied by tile Height
              }

            }


//---------- We now have everything we need from the fits file ----------

          }			// End of 'if the metafits file opened correctly'

          if (status == END_OF_FILE)  status = 0;                                                       // Reset after normal error

          fits_close_file(fptr, &status);
          if (status) {
            log_error("Failed to close metafits file: ");
            fits_report_error(stderr, status);                                                // print any error message
          }
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


//---------- Do Geometric delays ----------

          if ( subm->GEODEL >= 1 ){										// GEODEL field. (0=nothing, 1=zenith, 2=tile-pointing, 3=az/el table tracking)
            delay_so_far_start_mm  += get_path_difference(rfm->North, rfm->East, rfm->Height, subm->altaz[0].Alt, subm->altaz[0].Az);
            delay_so_far_middle_mm += get_path_difference(rfm->North, rfm->East, rfm->Height, subm->altaz[1].Alt, subm->altaz[1].Az);
            delay_so_far_end_mm    += get_path_difference(rfm->North, rfm->East, rfm->Height, subm->altaz[2].Alt, subm->altaz[2].Az);
            
            DEBUG_LOG("dels: %Lf, delm: %Lf, del: %Lf, ",
              get_path_difference(rfm->North, rfm->East, rfm->Height, subm->altaz[0].Alt, subm->altaz[0].Az),
              get_path_difference(rfm->North, rfm->East, rfm->Height, subm->altaz[1].Alt, subm->altaz[1].Az), 
              get_path_difference(rfm->North, rfm->East, rfm->Height, subm->altaz[2].Alt, subm->altaz[2].Az));


          }
          DEBUG_LOG("north: %Lf, east: %Lf, height: %Lf, ",  rfm->North, rfm->East, rfm->Height);
//---------- Do Calibration delays ----------
//	WIP.  Calibration delays are just a dream right now


//---------- Convert 'start', 'middle' and 'end' delays from millimetres to samples --------

          long double start_sub_s  = delay_so_far_start_mm * mm2s_conv_factor;					// Convert start delay into samples at the coarse channel sample rate
          long double middle_sub_s = delay_so_far_middle_mm * mm2s_conv_factor;					// Convert middle delay into samples at the coarse channel sample rate
          long double end_sub_s    = delay_so_far_end_mm * mm2s_conv_factor;					// Convert end delay into samples at the coarse channel sample rate
          DEBUG_LOG("start: %Lf, middle: %Lf, end: %Lf, ",  start_sub_s, middle_sub_s, end_sub_s);
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

//---------- Now treat the start, middle & end  residuals as points on a parabola at x=0, x=800, x=1600 ----------
//      Calculate a, b & c of this parabola in the form: delay = ax^2 + bx + c where x is the (tenth of a) block number

          a = ( start_sub_s - middle_sub_s - middle_sub_s + end_sub_s ) * two_over_num_blocks_sqrd ;                                                          // a = (s+e-2*m)/(n*n/2)
          b = ( middle_sub_s + middle_sub_s + middle_sub_s + middle_sub_s - start_sub_s - start_sub_s - start_sub_s - end_sub_s ) * one_over_num_blocks;      // b = (4*m-3*s-e)/(n)
          c = start_sub_s ;                                                                                                                                   // c = s
          DEBUG_LOG("a: %Lf, b: %Lf, c: %Lf, ",  a, b, c);
//      residual delays can now be interpolated for any time using 'ax^2 + bx + c' where x is the time

//---------- We'll be calulating each value in turn so we're better off passing back in a form only needing 2 additions per data point.
//		The phase-wrap delay correction phase wants the delay at time points of x=.5, x=1.5, x=2.5, x=3.5 etc, so
//		we'll set an initial value of a×0.5^2 + b×0.5 + c to get the first point and our first step in will be:
          rfm->initial_delay = a * 0.25L + b * 0.5L + c;							// ie a×0.5^2 + b×0.5 + c for our initial value of delay(0.5)
          rfm->delta_delay = a + a + b;									// That's our first step.  ie delay(1.5) - delay(0.5) or if you like, (a*1.5^2+b*1.5+c) - (a*0.5^2+b*0.5+c)
          rfm->delta_delta_delay = a + a;									// ie 2a because it's the 2nd derivative
          rfm->ws_delay = (int16_t) whole_sample_delay;				// whole_sample_delay has already been roundl(ed) somewhere above here
          rfm->start_total_delay = start_sub_s;
          rfm->middle_total_delay = middle_sub_s;
          rfm->end_total_delay = end_sub_s;
          DEBUG_LOG("ws: %d, initial: %d, delta: %d, delta_delta: %d\n", rfm->ws_delay, rfm->initial_delay, rfm->delta_delay, rfm->delta_delta_delay);
          meta_result = SLOT_META_DONE;

//---------- Print out a bunch of debug info ----------

          if (state->debug_mode) {										// Debug logging to screen
            printf( "%d,%d,%d,%d,%d,%d,%s,%s,%d,%d,%d,%Lf,%Lf,%Lf,%Lf,%d,%f,%f,%f,%Lf,%Lf,%Lf,%f,%s,%f:",
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


            printf( "\n" );
          }												// Only see this if we're in debug mode

        }

//---------- And we're basically done reading the metafits and preping for the sub file write which is only a few second away (done by another thread)

        clock_gettime( CLOCK_REALTIME, &ended_meta_write_time);

        subm->meta_msec_took = ( ( ended_meta_write_time.tv_sec - started_meta_write_time.tv_sec ) * 1000 ) + ( ( ended_meta_write_time.tv_nsec - started_meta_write_time.tv_nsec ) / 1000000 );    // msec since this sub started

        subm->meta_done = meta_result;                                      // Record that we've finished working on this one even if we gave up.  Will be a 4 or a 5 depending on whether it worked or not.


      }				// End of 'if there is a metafits to read' (actually an 'else' off 'is there nothing to do')
    }				// End of huge 'while !terminate' loop
}				// End of function
