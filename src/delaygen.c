
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
  log_warning("Launching in delay generator mode. No UDP data will be captured." );
  fprintf(stderr, "Using observation %d for delay generator.\n", delaygen_obs_id);
  fprintf(stderr, "Using subobservation index %d for delay generator.\n", delaygen_subobs_idx);
  fprintf(stderr, "entering delay generator mode\n");
  
  subobs_udp_meta_t *subm = &sub[0];
  uint32_t subobs_start = obs_id + subobs_idx * 8;
  sub[0].state = 1;
  sub[0].meta_done = 0;
  sub[0].subobs = subobs_start & ~7;

  fprintf(stderr, "obs ID: %d\n", obs_id);
  fprintf(stderr, "subobs ID: %d\n", sub[0].subobs);

  struct timespec time_start;
  struct timespec time_now;
  bool reader_done = false;
  //bool reader_success = false;
  clock_gettime( CLOCK_REALTIME, &time_start);
  clock_gettime( CLOCK_REALTIME, &time_now);
  pthread_t reader_thread;
  pthread_create(&reader_thread,NULL,add_meta_fits_thread,NULL);
  while(!reader_done && time_now.tv_sec - time_start.tv_sec < 5) {
    clock_gettime( CLOCK_REALTIME, &time_now);
    if(sub[0].meta_done == 4) {
      fprintf(stderr, "metadata loaded successfully.\n");
      reader_done = true;
      //reader_success = true;
    } else if(sub[0].meta_done == 5){
      fprintf(stderr, "error loading metadata.\n");
      reader_done = true;
      //reader_success = false;
    }
  }
  state.terminate = true;
  pthread_join(reader_thread,NULL);
  fprintf(stderr, "reader thread joined.\n");
  if(!reader_done) {
    fprintf(stderr, "timed out waiting for metadata.\n");
  }

  DEBUG_LOG("NINPUTS %d\n", sub[0].NINPUTS);
  //altaz_meta_t *altaz = subm->altaz;
  for(int i=0; i<3; i++) {
    DEBUG_LOG("AltAz[%d] = {Alt: %f, Az: %f, Dist_km: %f, SinAlt: %Lf, SinAzCosAlt: %Lf, CosAzCosAlt: %Lf, gpstime: %lld}\n", i, altaz[i].Alt, altaz[i].Az, altaz[i].Dist_km, altaz[i].SinAlt, altaz[i].SinAzCosAlt, altaz[i].CosAzCosAlt, altaz[i].gpstime);
  }
  fflush(stderr);

  tile_meta_t *rfm;
  delay_table_entry_t dt_entry_working;
  int MandC_rf;
  int ninputs_pad = sub[0].NINPUTS;
  double w_initial_delay;
  double w_delta_delay;

  /// Copied and pasted from `makesub`:
          for ( MandC_rf = 0; MandC_rf < ninputs_pad; MandC_rf++ ) {            // The zeroth block is the size of the padded number of inputs times SUB_LINE_SIZE.  NB: We dodn't pad any more!

            rfm = &subm->rf_inp[ MandC_rf ];					// Get a temp pointer for this tile's metadata

            dt_entry_working.rf_input = rfm->rf_input;			              	// Copy the tile's ID and polarization into the structure we're about to write to the sub file's 0th block
            dt_entry_working.ws_delay_applied = rfm->ws_delay;	       			// Copy the tile's whole sample delay value into the structure we're about to write to the sub file's 0th block
            w_initial_delay = rfm->initial_delay;				                    // Copy the tile's initial fractional delay (times 2^20) to a working copy we can update as we step through the delays
            w_delta_delay = rfm->delta_delay;				                      	// Copy the tile's initial fractional delay step (like its 1st deriviative) to a working copy we can update as we step through the delays 

            dt_entry_working.initial_delay = w_initial_delay;		           	// Copy the tile's initial fractional delay (times 2^20) to the structure we're about to write to the 0th block
            dt_entry_working.delta_delay = w_delta_delay;			              // Copy the tile's initial fractional delay step (like its 1st deriviative) to the structure we're about to write to the 0th block
            dt_entry_working.delta_delta_delay = rfm->delta_delta_delay;		// Copy the tile's fractional delay step's step (like its 2nd deriviative) to the structure we're about to write to the 0th block
            dt_entry_working.num_pointings = 1;				                    	// Initially 1, but might grow to 10 or more if beamforming.  The first pointing is for delay tracking in the correlator.

            for ( int loop = 0; loop < POINTINGS_PER_SUB; loop++ ) {	    	// populate the fractional delay lookup table for this tile (one for each 5ms)
              dt_entry_working.frac_delay[ loop ] = w_initial_delay;	      // Rounding and everything was all included when we did the original maths in the other thread
              w_initial_delay += w_delta_delay;				                      // update our *TEMP* copies as we cycle through
              w_delta_delay += dt_entry_working.delta_delta_delay;	       	// and update out *TEMP* delta.  We need to do this using temp values, so we don't 'use up' the initial values we want to write out.
            }

            //dest = mempcpy( dest, &block_0_working, sizeof(block_0_working) );	// write them out one at a time
            printf("%d,%d,%.*f,%.*f,%.*f,%.*f,%.*f,%.*f,%d,%d,", 
              
              rfm->rf_input,
              rfm->ws_delay, 
              DECIMAL_DIG, 
              rfm->start_total_delay, 
              DECIMAL_DIG, 
              rfm->middle_total_delay, 
              DECIMAL_DIG, 
              rfm->end_total_delay, 
              DECIMAL_DIG, 
              rfm->initial_delay, 
              DECIMAL_DIG, 
              rfm->delta_delay, 
              DECIMAL_DIG,
              rfm->delta_delta_delay,
              1,
              0);
            for ( int loop = 0; loop < POINTINGS_PER_SUB-1; loop++ ) {
              printf("%.*f,", DECIMAL_DIG, dt_entry_working.frac_delay[loop]);
            }
            printf("%.*f\n", DECIMAL_DIG, dt_entry_working.frac_delay[POINTINGS_PER_SUB-1]);
            memset(&dt_entry_working, 0, sizeof(delay_table_entry_t));
          }


  //for(int i=0; i<sub[0].NINPUTS; i++) {
   // rfm = &sub[0].rf_inp[i];
    
 // }
  //printf("%d", rfm->
  fflush(stdout);

  fprintf(stderr, "\ndone.\n");
  return 0;
}