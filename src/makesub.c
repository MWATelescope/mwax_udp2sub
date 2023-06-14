
//---------------------------------------------------------------------------------------------------------------------------------------------------
// makesub - Every time an 8 second block of udp packets is available, try to generate a sub files for the correlator
//---------------------------------------------------------------------------------------------------------------------------------------------------

void *makesub() {

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
    bool go4sub;

    pkt_data_t dummy_udp={0};                                     // Make a dummy udp packet full of zeros and NULLs.  We'll use this to stand in for every missing packet!
    void *dummy_volt_ptr = &dummy_udp.volts[0];                         // and we'll remember where we can find UDP_PAYLOAD_SIZE (4096LL) worth of zeros
    uint8_t *dummy_map; /* = calloc(active_rf_inputs*UDP_PER_RF_PER_SUB/8, 1); */ // Input x Packet Number bitmap of dummy packets used. All 1s = no dummy packets.
    
    subobs_udp_meta_t *subm;                                            // pointer to the sub metadata array I'm working on
    tile_meta_t *rfm;							// Pointer to the tile metadata for the tile I'm working on
    
    int block;                                                          // loop variable
    int MandC_rf;                                                       // loop variable
    int ninputs_pad;							// The number of inputs in the sub file padded out with any needed dummy tiles (used to be a thing but isn't any more)
    int ninputs_xgpu;                                                   // The number of inputs in the sub file but padded out to whatever number xgpu needs to deal with them in (not actually padded until done by Ian's code later)

    MandC_meta_t my_MandC_meta[MAX_INPUTS];                             // Working copies of the changeable metafits/metabin data for this subobs
    MandC_meta_t *my_MandC;                                             // Make a temporary pointer to the M&C metadata for one rf input

    delay_table_entry_t block_0_working;				// Construct a single tile's metadata struct to write to the 0th block.  We'll update and write this out as we step through tiles.
    double w_initial_delay;						// tile's initial fractional delay (times 2^20) working copy
    double w_delta_delay;						// tile's initial fractional delay step (like its 1st deriviative)

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

        if ( sub[loop].state == 2  && sub[loop].meta_done == 4) {       // If this sub is ready to write out

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

        go4sub = true;                                                                                                  // Say it all looks okay so far

        ninputs_pad = subm->NINPUTS;									                                                               		// We don't pad .sub files any more so these two variables are the same
        
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

        int64_t obs_offset = subm->subobs - subm->GPSTIME;                                                                // How long since the observation started?

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
          "APPLY_PATH_DELAYS %d\n"
          "APPLY_PATH_PHASE_OFFSETS %d\n"
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
          "IDX_PACKET_MAP %d+%d\n"
          "IDX_METAFITS %d+%d\n"
          "IDX_DELAY_TABLE %d+%d\n"
          "IDX_MARGIN_DATA %d+%d\n"
          "MWAX_SUB_VER 2\n"
          ;

        sprintf( sub_header, head_mask, SUBFILE_HEADER_SIZE, subm->GPSTIME, subm->subobs, subm->MODE, utc_start, obs_offset,
              NTIMESAMPLES, subm->NINPUTS, ninputs_xgpu, subm->CABLEDEL || subm->GEODEL, subm->CABLEDEL || subm->GEODEL, subm->INTTIME_msec, (subm->FINECHAN_hz/ULTRAFINE_BW), transfer_size, subm->PROJECT, subm->EXPOSURE, subm->COARSE_CHAN,
              conf.coarse_chan, subm->UNIXTIME, subm->FINECHAN_hz, (COARSECHAN_BANDWIDTH/subm->FINECHAN_hz), COARSECHAN_BANDWIDTH, SAMPLES_PER_SEC, BUILD );

//---------- Look in the shared memory directory and find the oldest .free file of the correct size ----------

        if ( go4sub ) {                                                                                 // If everything is okay so far, enter the next block of code
          go4sub = false;                                                                               // but go back to assuming a failure unless we succeed in the next bit

          dir = opendir( conf.shared_mem_dir );                                                         // Open up the shared memory directory

          if ( dir == NULL ) {                                                                          // If the directory doesn't exist we must be running on an incorrectly set up server
            printf( "Fatal error: Directory %s does not exist\n", conf.shared_mem_dir );
            fflush(stdout);
            terminate = true;                                                                           // Tell every thread to close down
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
                      go4sub = true;                                                                    // We've found at least one file we can reuse.  It may not be the best, but we know we can do this now.
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
          go4sub = false;                                                                               // but go back to assuming a failure unless we succeed in the next bit

          // printf( "The winner is %s\n", best_file );

          if ( rename( best_file, conf.temp_file_name ) != -1 ) {                                       // If we can rename the file to our temporary name

            if ( (ext_shm_fd = shm_open( &conf.temp_file_name[8], O_RDWR, 0666)) != -1 ) {              // Try to open the shmem file (after removing "/dev/shm" ) and if it opens successfully

              if ( (ext_shm_buf = (char *) mmap (NULL, desired_size, PROT_READ | PROT_WRITE, MAP_SHARED , ext_shm_fd, 0)) != ((char *)(-1)) ) {      // and if we can mmap it successfully
                go4sub = true;                                                                          // Then we are all ready to use the buffer so remember that
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
            block_0_working.ws_delay_applied = rfm->ws_delay;				// Copy the tile's whole sample delay value into the structure we're about to write to the sub file's 0th block

            w_initial_delay = rfm->initial_delay;				// Copy the tile's initial fractional delay (times 2^20) to a working copy we can update as we step through the delays
            w_delta_delay = rfm->delta_delay;					// Copy the tile's initial fractional delay step (like its 1st deriviative) to a working copy we can update as we step through the delays 

            block_0_working.initial_delay = w_initial_delay;			// Copy the tile's initial fractional delay (times 2^20) to the structure we're about to write to the 0th block
            block_0_working.delta_delay = w_delta_delay;			// Copy the tile's initial fractional delay step (like its 1st deriviative) to the structure we're about to write to the 0th block
            block_0_working.delta_delta_delay = rfm->delta_delta_delay;		// Copy the tile's fractional delay step's step (like its 2nd deriviative) to the structure we're about to write to the 0th block
            block_0_working.start_total_delay = rfm->start_total_delay;
            block_0_working.middle_total_delay = rfm->middle_total_delay;
            block_0_working.end_total_delay = rfm->end_total_delay;
            block_0_working.num_pointings = 1;					// Initially 1, but might grow to 10 or more if beamforming.  The first pointing is for delay tracking in the correlator.

            for ( int loop = 0; loop < POINTINGS_PER_SUB; loop++ ) {		// populate the fractional delay lookup table for this tile (one for each 5ms)
              block_0_working.frac_delay[ loop ] = w_initial_delay;	// Rounding and everything was all included when we did the original maths in the other thread
              w_initial_delay += w_delta_delay;					// update our *TEMP* copies as we cycle through
              w_delta_delay += block_0_working.delta_delta_delay;		// and update out *TEMP* delta.  We need to do this using temp values, so we don't 'use up' the initial values we want to write out.
            }

            dest = mempcpy( dest, &block_0_working, sizeof(block_0_working) );	// write them out one at a time
          }

          dummy_map = (uint8_t*) dest;

          // 'dest' now points to the address after our last memory write, BUT we need to pad out to a whole block size!
          // 'block1_add points to the address at the beginning of block 1.  The remainder of block 0 needs to be null filled

          memset( dest, 0, block1_add - dest );					// Write out a bunch of nulls to line us up with the first voltage block (ie block 1)
          dest = block1_add;							// And set our write pointer to the beginning of block 1

//---------- Write out the dummy map ----------
          uint32_t dummy_map_len = ninputs_pad * (UDP_PER_RF_PER_SUB-2)/8;
	        DEBUG_LOG("UDP Packet Map at 0x%08llx (len=%ld)\n", 
    	 		  (uint64_t) dummy_map - (uint64_t) ext_shm_buf, 
	 	    	  dummy_map_len);
          
          for ( MandC_rf = 0; MandC_rf < ninputs_pad; MandC_rf++ ) {
            rfm = &subm->rf_inp[ MandC_rf ];		                     			// Tile metadata
            uint16_t row = subm->rf2ndx[rfm->rf_input];
            char **packets = subm->udp_volts[row];
            for (int t=0; t<UDP_PER_RF_PER_SUB-2; t+=8) {
              uint8_t bitmap = (packets[t+1] != NULL) << 7
                           | (packets[t+2] != NULL) << 6
                           | (packets[t+3] != NULL) << 5
                           | (packets[t+4] != NULL) << 4
                           | (packets[t+5] != NULL) << 3
                           | (packets[t+6] != NULL) << 2
                           | (packets[t+7] != NULL) << 1
                           | (packets[t+8] != NULL);
              dummy_map[(MandC_rf * (UDP_PER_RF_PER_SUB-2) + t)/ 8] = bitmap;
            }
          }

//---------- Write out the volt data for the "margin packets" of every RF source to block 0. ----------
/* We need two packet's worth at each end to be able to undo delays, because if we shift out samples
 * at the start of a block, we won't be able to later find them in the very first packet, they were
 * only ever stored in the second - but we still need that first packet in case we later want to shift
 * in samples, going the other way.
 */
          uint32_t block0_margin_len = 0;
          uint8_t *block0_margin_dst = dummy_map + dummy_map_len;
          //uint8_t *block0_margin_start = block0_margin_dst;
          bytes2copy = UDP_PAYLOAD_SIZE;
          for ( MandC_rf = 0; MandC_rf < ninputs_pad; MandC_rf++ ) {
            my_MandC = &my_MandC_meta[MandC_rf];

            source_ptr = subm->udp_volts[my_MandC->seen_order][0];
            if ( source_ptr == NULL ) source_ptr = dummy_volt_ptr;
            block0_margin_dst = mempcpy( block0_margin_dst, source_ptr, bytes2copy );

            source_ptr = subm->udp_volts[my_MandC->seen_order][1];
            if ( source_ptr == NULL ) source_ptr = dummy_volt_ptr;
            block0_margin_dst = mempcpy( block0_margin_dst, source_ptr, bytes2copy );

            source_ptr = subm->udp_volts[my_MandC->seen_order][UDP_PER_RF_PER_SUB-2];
            if ( source_ptr == NULL ) source_ptr = dummy_volt_ptr;
            block0_margin_dst = mempcpy( block0_margin_dst, source_ptr, bytes2copy );

            source_ptr = subm->udp_volts[my_MandC->seen_order][UDP_PER_RF_PER_SUB-1];
            if ( source_ptr == NULL ) source_ptr = dummy_volt_ptr;
            block0_margin_dst = mempcpy( block0_margin_dst, source_ptr, bytes2copy );

            block0_margin_len += bytes2copy * 4;
          }
          DEBUG_LOG("UDP margin packets at 0x%08llx (len=%d)\n", 
	 		      (uint64_t) block0_margin_start - (uint64_t) ext_shm_buf, 
	    		  block0_margin_len);
          fflush(stdout);


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
                  monitor.udp_dummy++;
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
            (int64_t)(ended_sub_write_time.tv_sec - GPS_offset), subm->subobs, subm->GPSTIME, subm->MODE, subm->state, free_files, bad_free_files, subm->msec_wait, subm->msec_took, subm->udp_at_end_write - subm->first_udp, subm->udp_count, subm->udp_dummy, subm->NINPUTS, subm->rf_seen, active_rf_inputs );

        fflush(stdout);

      }

    }                                                                   // Jump up to the top and look again (as well as checking if we need to shut down)

//---------- We've been told to shut down ----------

    printf( "Exiting makesub\n" );

    pthread_exit(NULL);
}
