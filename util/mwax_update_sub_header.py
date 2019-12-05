#!/usr/bin/python3
import argparse
import datetime
import glob
import numpy as np
import logging.handlers
import os
from astropy.io import fits

# Global constants
MAX_CORR_CHAN = 24
ULTRAFINE_WIDTH_HZ = 250


class MWAMetafitsProcessor:
    def __init__(self, logger, subfilename, metafits_path, coarse_channel_number, ultrafine_width_hz):
        self.logger = logger
        self.subfilename = subfilename

        # Get the appropriate metafits based on the subfile name
        # Subfilename will be 'gpstime.sub'
        # Get the gpstime == subobsid!
        subfilename_nopath = os.path.basename(subfilename)

        # Now remove the extension
        subfilename_filename_noext = os.path.splitext(subfilename_nopath)[0]

        # Check this seems ok- 10 digit integer
        self.subobs_id = None

        if len(subfilename_filename_noext) != 10:
            raise Exception(f"Error: subfilename {subfilename} must be in the form of <gpstime>.sub")

        try:
            self.subobs_id = int(subfilename_filename_noext)
        except ValueError:
            raise Exception(f"Error: subfilename {subfilename_nopath} must be in the form of <gpstime>.sub")

        # Now look for metafits file in metafits path which is at or after the subobs_id
        metafits_search = os.path.join(metafits_path, "*.fits")

        metafits_file_list = sorted(glob.glob(metafits_search), reverse=False)
        self.metafits_filename = None

        # print the list of possible metafits files
        self.logger.debug(f"{len(metafits_file_list)} metafits files in {metafits_search}...\n{metafits_file_list}")

        for metafits_filename in metafits_file_list:
            # Get gpstime from filename  (e.g. 1237245248_metafits.fits ==> 1237245248)
            metafits_filename_nopath = os.path.basename(metafits_filename)

            # Get filename and extension
            metafits_filename_filename = os.path.splitext(metafits_filename_nopath)[0]  # e.g. 1237245248_metafits
            metafits_filename_ext = os.path.splitext(metafits_filename_nopath)[1]       # e.g. .fits

            # First, ensure we have a fits file
            if metafits_filename_ext != ".fits":
                self.logger.debug(f"File {metafits_filename} does not end in '.fits'")
                continue

            # Second, check that it ends in _metafits
            if metafits_filename_filename[-9:] != "_metafits":
                self.logger.debug(f"File {metafits_filename} does not end in '_metafits.fits'")
                continue

            # Now is the obsid in range?
            this_metafits_obs_id = None

            try:
                this_metafits_obs_id = int(metafits_filename_filename[:-9])
            except ValueError:
                raise Exception(f"Error: metafits filename {metafits_filename} must be in the form "
                                f"of <gpstime>_metafits.fits")

            if self.subobs_id == this_metafits_obs_id:
                # Found an exact match!
                self.logger.debug(f"Found exact match {metafits_filename}")
                self.metafits_filename = metafits_filename
                break

            elif self.subobs_id > this_metafits_obs_id:
                # Found a contender!
                logger.debug(f"Found contender {metafits_filename}")
                self.metafits_filename = metafits_filename

            else:
                # The current metafits is out of range and can't be it
                self.logger.debug(f"Metafits is for AFTER the subobs {self.subobs_id} vs metafitsid={this_metafits_obs_id}")

        # We've looped through all metafits- was it found?
        if self.metafits_filename is None:
            raise Exception(f"Error: Could not find a metafits file for the sub file {subfilename}")
        else:
            self.logger.info(f"Found {self.metafits_filename} for the sub file {subfilename}!")

        # Store input parameters
        self.coarse_channel_number = coarse_channel_number  # this is 1 based. So 1..num of correlator boxes (e.g. 1-24)
        self.ultrafine_width_hz = ultrafine_width_hz        # Hz of ultrafine channels. Nominally 250 Hz

        # Read metafits file
        self.logger.info(f"Openning {self.metafits_filename}...")
        self.hdu_list = fits.open(self.metafits_filename)
        self.primary_hdu = self.hdu_list[0]
        self.tile_table = self.hdu_list[1]

        # Value variables from metafits
        self.gpstime = None
        self.mode = None
        self.datestrt = None
        self.ninputs = None
        self.finechan = None
        self.inttime = None
        self.project = None
        self.exposure = None
        self.goodtime = None
        self.quacktim = None
        self.nav_freq = None
        self.bandwidth = None
        self.receiver_channel_string = None
        self.correlator_channel_string = None

        # variables from tile table
        self.ninputs = None

        # Calculated values for header
        self.command = None
        self.utc_start = None
        self.obs_offset = None
        self.ninputs_xgpu = None
        self.inttime_msec = None
        self.unix_time = None
        self.finechan_hz = None
        self.receiver_channel_list = None
        self.correlator_channel_list = None
        self.nchan = None
        self.bandwidth_hz = None
        self.nfinechan = None
        self.receiver_channel_number = None
        self.ntimesamples = None

    def read_metafits(self):
        self.gpstime = self.read_value("GPSTIME")       # [s] GPS time of observation start
        self.mode = self.read_value("MODE")             # Observation mode
        self.datestrt = self.read_value("DATESTRT")     # [UT] Date and time of correlations start
        self.ninputs = self.read_value("NINPUTS")       # Number of inputs into the correlation products
        self.finechan = self.read_value('FINECHAN')     # [kHz] Fine channel width - correlator freq_res
        self.inttime = self.read_value("INTTIME")       # [s] Individual integration time
        self.project = self.read_value("PROJECT")       # Project ID
        self.exposure = self.read_value("EXPOSURE")     # [s] duration of observation
        self.goodtime = self.read_value("GOODTIME")     # OBSID+QUACKTIME as Unix timestamp
        self.quacktim = self.read_value("QUACKTIM")     # Seconds of bad data after observation starts
        self.nav_freq = self.read_value("NAV_FREQ")     # Assumed frequency averaging
        self.bandwidth = self.read_value("BANDWDTH")    # [MHz] Total bandwidth (of observation)
        self.receiver_channel_string = self.read_value("CHANNELS")   # string of comma sep receiver channel numbers
        self.correlator_channel_string = self.read_value("CHANSEL")  # string of comma sep correlator channel numbers

    def read_value(self, keyword):
        value = self.primary_hdu.header[keyword]
        self.logger.debug(f"Read {keyword} == {str(value)}")
        return value

    def read_tile_table(self):
        # Get the tiledata table
        self.ninputs = np.shape(self.tile_table.data)[0]
        self.logger.debug(f"Read number of rf chains (tiles x pols) == {self.ninputs}")

    def process(self):
        # UTC Start
        # needs to be in the form: yyyy-mm-dd-hh:mm:ss
        utc_start_datetime = datetime.datetime.strptime(self.datestrt, "%Y-%m-%dT%H:%M:%S")
        self.utc_start = utc_start_datetime.strftime("%Y-%m-%d-%H:%M:%S")

        # Obs offset- how many seconds since start of obs?
        self.obs_offset = self.subobs_id - self.gpstime

        # Check that rf_chains mod 16 == 0 (XGPU can only handle blocks of 16 inputs)
        remainder = self.ninputs % 16

        if remainder != 0:
            self.ninputs_xgpu = self.ninputs + (16 - remainder)
            self.logger.info(f"XGPU inputs will be rounded up to {self.ninputs_xgpu}")
        else:
            self.ninputs_xgpu = self.ninputs

        # Integration time needs to be converted to msec
        self.inttime_msec = int(1000 * self.inttime)

        # Get Unix time
        self.unix_time = int(self.goodtime - self.quacktim)

        # Get fine channel width in Hz
        self.finechan_hz = self.finechan * 1000

        # Get coarse channel info
        self.receiver_channel_list = self.receiver_channel_string.split(",")
        self.correlator_channel_list = self.correlator_channel_string.split(",")

        # Get coarse channel width
        self.bandwidth_hz = int((self.bandwidth * 1000000) / len(self.correlator_channel_list))

        # Get number of fine channels
        self.nfinechan = int(self.bandwidth_hz / self.finechan_hz)

        # Get receiver channel number
        self.receiver_channel_number = self.receiver_channel_list[self.coarse_channel_number - 1]

        # Get number of time samples
        self.ntimesamples = int(self.bandwidth_hz / self.ultrafine_width_hz)

    def write_psrdada_header(self):
        # Generate new 4096 byte header
        output = ""

        output += f"HDR_SIZE 4096\n"
        output += f"POPULATED 1\n"
        output += f"OBS_ID {self.gpstime}\n"
        output += f"SUBOBS_ID {self.subobs_id}\n"
        output += f"MODE {self.mode}\n"
        output += f"UTC_START {self.utc_start}\n"
        output += f"OBS_OFFSET {self.obs_offset}\n"
        output += f"NBIT 8\n"
        output += f"NPOL 2\n"
        output += f"NTIMESAMPLES {self.ntimesamples}\n"
        output += f"NINPUTS {self.ninputs}\n"
        output += f"NINPUTS_XGPU {self.ninputs_xgpu}\n"
        output += f"APPLY_PATH_WEIGHTS 0\n"
        output += f"APPLY_PATH_DELAYS 0\n"
        output += f"INT_TIME_MSEC {self.inttime_msec}\n"
        output += f"FSCRUNCH_FACTOR {self.nav_freq}\n"
        output += f"APPLY_VIS_WEIGHTS 0\n"
        output += f"TRANSFER_SIZE 5269094400\n"
        output += f"PROJ_ID {self.project}\n"
        output += f"EXPOSURE_SECS {self.exposure}\n"
        output += f"COARSE_CHANNEL {self.receiver_channel_number}\n"
        output += f"CORR_COARSE_CHANNEL {self.coarse_channel_number}\n"
        output += f"SECS_PER_SUBOBS 8\n"
        output += f"UNIXTIME {self.unix_time}\n"
        output += f"UNIXTIME_MSEC 0\n"
        output += f"FINE_CHAN_WIDTH_HZ {self.finechan_hz}\n"
        output += f"NFINE_CHAN {self.nfinechan}\n"
        output += f"BANDWIDTH_HZ {self.bandwidth_hz}\n"
        output += f"SAMPLE_RATE 1280000\n"
        output += f"MC_IP 0.0.0.0\n"
        output += f"MC_PORT 0\n"

        # pad out the header to be exactly 4096 characters
        output = output.ljust(4096, "\0")

        self.logger.debug(f"New header:\n{output}\n({len(output)} bytes)")

        # Now update the sub file
        with open(self.subfilename, 'r+') as subfile:
            subfile.seek(0)
            subfile.write(output)

        self.logger.info(f"Updated {self.subfilename} with new header")


class Processor:
    def __init__(self):
        self.logger = logging.getLogger('mwax_update_sub_header')

    def initialise(self):
        parser = argparse.ArgumentParser()
        parser.add_argument("--subfilename", help="MWAX high time resolution subfile filename")
        parser.add_argument("--chan", help=f"Correlator Channel number (1..{MAX_CORR_CHAN})", type=int)
        parser.add_argument("--metafits_directory", help="Metafits file directory (usually /vulcan/metafits/)")

        # Parse the command line
        args = vars(parser.parse_args())

        # Check inputs
        self.subfilename = args["subfilename"]
        self.chan = args["chan"]
        self.metafits_directory = args["metafits_directory"]

        if not os.path.exists(self.subfilename):
            print(f"The --subfilename {self.subfilename} does not exist or is not readable.")
            exit(1)

        if self.chan < 1 or self.chan > MAX_CORR_CHAN:
            print(f"The --chan value {self.chan} is not valid (should be 1-{MAX_CORR_CHAN}.")
            exit(2)

        # start logging
        self.logger.setLevel(logging.DEBUG)
        self.logger.propagate = False
        handler = logging.StreamHandler()
        handler.setLevel(logging.DEBUG)
        handler.setFormatter(logging.Formatter('%(asctime)s, %(levelname)s, %(message)s'))
        self.logger.addHandler(handler)

    def start(self):
        self.logger.info("Starting mwax_update_sub_header...")

        # Run!
        # "1247842824_metafits.fits"
        this_obs_metafits = MWAMetafitsProcessor(logger = self.logger,
                                                 subfilename=self.subfilename,
                                                 metafits_path=self.metafits_directory,
                                                 coarse_channel_number=self.chan,
                                                 ultrafine_width_hz=ULTRAFINE_WIDTH_HZ)
        this_obs_metafits.read_metafits()
        this_obs_metafits.read_tile_table()
        this_obs_metafits.process()
        this_obs_metafits.write_psrdada_header()

        self.logger.info("mwax_update_sub_header SUCCESS")


if __name__ == '__main__':
    p = Processor()

    try:
        p.initialise()
        p.start()
        exit(0)

    except Exception as e:
        if p.logger:
            p.logger.exception(str(e))
        else:
            print(str(e))
