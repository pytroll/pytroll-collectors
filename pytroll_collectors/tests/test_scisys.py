#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2013, 2014 Martin Raspaud

# Author(s):

#   Martin Raspaud <martin.raspaud@smhi.se>

# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.

# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.

# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

"""Test suite for the scisys receiver."""


# Test cases.

import datetime
import os
import unittest

from pytroll_collectors.scisys import MessageReceiver, TwoMetMessage

hostname = 'localhost'

input_stoprc = '<message timestamp="2013-02-18T09:21:35" sequence="7482" severity="INFO" messageID="0" type="2met.message" sourcePU="SMHI-Linux" sourceSU="POESAcquisition" sourceModule="POES" sourceInstance="1"><body>STOPRC Stop reception: Satellite: NPP, Orbit number: 6796, Risetime: 2013-02-18 09:08:09, Falltime: 2013-02-18 09:21:33</body></message>'  # noqa

input_dispatch_viirs = '<message timestamp="2013-02-18T09:24:20" sequence="27098" severity="INFO" messageID="8250" type="2met.filehandler.sink.success" sourcePU="SMHI-Linux" sourceSU="GMCSERVER" sourceModule="GMCSERVER" sourceInstance="1"><body>FILDIS File Dispatch: /data/npp/RNSCA-RVIRS_npp_d20130218_t0908103_e0921256_b00001_c20130218092411165000_nfts_drl.h5 ftp://{hostname}:21/tmp/RNSCA-RVIRS_npp_d20130218_t0908103_e0921256_b00001_c20130218092411165000_nfts_drl.h5</body></message>'.format(  # noqa
    hostname=hostname)

input_dispatch_atms = '<message timestamp="2013-02-18T09:24:21" sequence="27100" severity="INFO" messageID="8250" type="2met.filehandler.sink.success" sourcePU="SMHI-Linux" sourceSU="GMCSERVER" sourceModule="GMCSERVER" sourceInstance="1"><body>FILDIS File Dispatch: /data/npp/RATMS-RNSCA_npp_d20130218_t0908194_e0921055_b00001_c20130218092411244000_nfts_drl.h5 ftp://{hostname}:21/tmp/RATMS-RNSCA_npp_d20130218_t0908194_e0921055_b00001_c20130218092411244000_nfts_drl.h5</body></message>'.format(  # noqa
    hostname=hostname)

viirs = {'platform_name': 'Suomi-NPP', 'format': 'RDR',
         'start_time': datetime.datetime(2013, 2, 18, 9, 8, 10, 300000),
         'data_processing_level': '0', 'orbit_number': 6796,
         'uri': 'ssh://{hostname}/tmp/RNSCA-RVIRS_npp_d20130218_t0908103_e0921256_b00001_c20130218092411165000_nfts_drl.h5'.format(hostname=hostname),  # noqa
         'uid': 'RNSCA-RVIRS_npp_d20130218_t0908103_e0921256_b00001_c20130218092411165000_nfts_drl.h5',
         'sensor': 'viirs',
         'end_time': datetime.datetime(2013, 2, 18, 9, 21, 25, 600000),
         'type': 'HDF5', 'variant': 'DR'}

atms = {'platform_name': 'Suomi-NPP', 'format': 'RDR', 'start_time':
        datetime.datetime(2013, 2, 18, 9, 8, 19, 400000),
        'data_processing_level': '0', 'orbit_number': 6796, 'uri':
        'ssh://{hostname}/tmp/RATMS-RNSCA_npp_d20130218_t0908194_e0921055_b00001_c20130218092411244000_nfts_drl.h5'.format(  # noqa
            hostname=hostname),
        'uid':
        'RATMS-RNSCA_npp_d20130218_t0908194_e0921055_b00001_c20130218092411244000_nfts_drl.h5',
        'sensor': 'atms',
        'end_time': datetime.datetime(2013, 2, 18, 9, 21, 5, 500000),
        'type': 'HDF5', 'variant': 'DR'}

stoprc_terra = '<message timestamp="2014-10-30T21:03:50" sequence="6153" severity="INFO" messageID="0" type="2met.message" sourcePU="SMHI-Linux" sourceSU="POESAcquisition" sourceModule="POES" sourceInstance="1"><body>STOPRC Stop reception: Satellite: TERRA, Orbit number: 79082, Risetime: 2014-10-30 20:49:50, Falltime: 2014-10-30 21:03:50</body></message>'  # noqa

fildis_terra = '<message timestamp="2014-10-30T21:03:57" sequence="213208" severity="INFO" messageID="8250" type="2met.filehandler.sink.success" sourcePU="SMHI-Linux" sourceSU="GMCSERVER" sourceModule="GMCSERVER" sourceInstance="1"><body>FILDIS File Dispatch: /data/modis/P0420064AAAAAAAAAAAAAA14303204950001.PDS ftp://{hostname}:21/tmp/P0420064AAAAAAAAAAAAAA14303204950001.PDS</body></message>'.format(  # noqa
    hostname=hostname)

msg_terra = {"platform_name": "EOS-Terra", "uri":
             "ssh://{hostname}/tmp/P0420064AAAAAAAAAAAAAA14303204950001.PDS".format(hostname=hostname), "format": "PDS",
             "start_time": datetime.datetime(2014, 10, 30, 20, 49, 50),
             "data_processing_level": "0", "orbit_number": 79082, "uid":
             "P0420064AAAAAAAAAAAAAA14303204950001.PDS",
             "sensor": "modis",
             "end_time": datetime.datetime(2014, 10, 30, 21, 3, 50),
             "type": "binary", 'variant': 'DR'}

stoprc_n19 = '<message timestamp="2014-10-28T07:25:37" sequence="472" severity="INFO" messageID="0" type="2met.message" sourcePU="SMHI-Linux" sourceSU="HRPTAcquisition" sourceModule="FSSRVC" sourceInstance="1"><body>STOPRC Stop reception: Satellite: NOAA 19, Orbit number: 29477, Risetime: 2014-10-28 07:16:01, Falltime: 2014-10-28 07:25:37</body></message>'  # noqa

fildis_n19 = '<message timestamp="2014-10-28T07:25:43" sequence="203257" severity="INFO" messageID="8250" type="2met.filehandler.sink.success" sourcePU="SMHI-Linux" sourceSU="GMCSERVER" sourceModule="GMCSERVER" sourceInstance="1"><body>FILDIS File Dispatch: /data/hrpt/20141028071601_NOAA_19.hmf ftp://{hostname}:21/tmp/20141028071601_NOAA_19.hmf</body></message>'.format(  # noqa
    hostname=hostname)

msg_n19 = {"platform_name": "NOAA-19", "format": "HRPT",
           "start_time": datetime.datetime(2014, 10, 28, 7, 16, 1),
           "data_processing_level": "0", "orbit_number": 29477,
           "uri": "ssh://{hostname}/tmp/20141028071601_NOAA_19.hmf".format(hostname=hostname),
           "uid": "20141028071601_NOAA_19.hmf",
           "sensor": ("avhrr/3", "mhs", "amsu-a", "hirs/4"),
           "end_time": datetime.datetime(2014, 10, 28, 7, 25, 37),
           "type": "binary", 'variant': 'DR'}

stoprc_m01 = '<message timestamp="2014-10-28T08:45:22" sequence="1157" severity="INFO" messageID="0" type="2met.message" sourcePU="SMHI-Linux" sourceSU="HRPTAcquisition" sourceModule="FSSRVC" sourceInstance="1"><body>STOPRC Stop reception: Satellite: METOP-B, Orbit number: 10948, Risetime: 2014-10-28 08:30:10, Falltime: 2014-10-28 08:45:22</body></message>'  # noqa

fildis_m01 = '<message timestamp="2014-10-28T08:45:27" sequence="203535" severity="INFO" messageID="8250" type="2met.filehandler.sink.success" sourcePU="SMHI-Linux" sourceSU="GMCSERVER" sourceModule="GMCSERVER" sourceInstance="1"><body>FILDIS File Dispatch: /data/metop/MHSx_HRP_00_M01_20141028083003Z_20141028084510Z_N_O_20141028083010Z ftp://{hostname}:21/tmp/MHSx_HRP_00_M01_20141028083003Z_20141028084510Z_N_O_20141028083010Z</body></message>'.format( # noqa
    hostname=hostname)

msg_m01 = {"platform_name": "Metop-B", "format": "EPS",
           "start_time": datetime.datetime(2014, 10, 28, 8, 30, 3),
           "data_processing_level": "0", "orbit_number": 10948,
           "uri": "ssh://{hostname}/tmp/MHSx_HRP_00_M01_20141028083003Z_20141028084510Z_N_O_20141028083010Z".format(hostname=hostname),  # noqa
           "uid": "MHSx_HRP_00_M01_20141028083003Z_20141028084510Z_N_O_20141028083010Z",
           "sensor": "mhs",
           "end_time": datetime.datetime(2014, 10, 28, 8, 45, 10),
           "type": "binary", 'variant': 'DR'}

startrc_npp2 = '<message timestamp="2014-10-31T08:53:52" sequence="9096" severity="INFO" messageID="0" type="2met.message" sourcePU="SMHI-Linux" sourceSU="POESAcquisition" sourceModule="POES" sourceInstance="1"><body>STRTRC Start reception: Satellite: NPP, Orbit number: 15591, Risetime: 2014-10-31 08:53:52, Falltime: 2014-10-31 09:06:28</body></message>'  # noqa

stoprc_npp2 = '<message timestamp="2014-10-31T09:06:28" sequence="9340" severity="INFO" messageID="0" type="2met.message" sourcePU="SMHI-Linux" sourceSU="POESAcquisition" sourceModule="POES" sourceInstance="1"><body>STOPRC Stop reception: Satellite: NPP, Orbit number: 15591, Risetime: 2014-10-31 08:53:52, Falltime: 2014-10-31 09:06:28</body></message>'  # noqa

fildis_npp2 = '<message timestamp="2014-10-31T09:06:25" sequence="216010" severity="INFO" messageID="8250" type="2met.filehandler.sink.success" sourcePU="SMHI-Linux" sourceSU="GMCSERVER" sourceModule="GMCSERVER" sourceInstance="1"><body>FILDIS File Dispatch: /data/npp/RCRIS-RNSCA_npp_d20141031_t0905166_e0905484_b00001_c20141031090623200000_nfts_drl.h5 ftp://{hostname}:21//tmp</body></message>'.format(  # noqa
    hostname=hostname)

msg_npp2 = {"orbit_number": 15591,
            "uid": "RCRIS-RNSCA_npp_d20141031_t0905166_e0905484_b00001_c20141031090623200000_nfts_drl.h5",
            "format": "RDR", "sensor": "cris",
            "start_time": datetime.datetime(2014, 10, 31, 9, 5, 16, 600000),
            "uri": "ssh://{hostname}//tmp/RCRIS-RNSCA_npp_d20141031_t0905166_e0905484_b00001_c20141031090623200000_nfts_drl.h5".format(hostname=hostname),  # noqa
            "platform_name": "Suomi-NPP",
            "end_time": datetime.datetime(2014, 10, 31, 9, 5, 48, 400000),
            "type": "HDF5", "data_processing_level": "0", 'variant': 'DR'}


def touch(fname):
    """Create an empty file."""
    open(fname, 'a').close()


class ScisysReceiverTest(unittest.TestCase):
    """Testing the Scisys receiver."""

    def test_reception(self):
        """Test the reception."""
        msg_rec = MessageReceiver("nimbus")

        # NPP

        string = TwoMetMessage(input_stoprc)
        to_send = msg_rec.receive(string)
        self.assertTrue(to_send is None)

        filename = os.path.join('/tmp', viirs['uid'])
        touch(filename)
        string = TwoMetMessage(input_dispatch_viirs)
        to_send = msg_rec.receive(string)
        self.assertDictEqual(to_send, viirs)
        os.remove(filename)

        filename = os.path.join('/tmp', atms['uid'])
        touch(filename)
        string = TwoMetMessage(input_dispatch_atms)
        to_send = msg_rec.receive(string)
        self.assertDictEqual(to_send, atms)
        os.remove(filename)

        # NPP with start

        string = TwoMetMessage(startrc_npp2)
        to_send = msg_rec.receive(string)
        self.assertTrue(to_send is None)

        filename = os.path.join('/tmp', msg_npp2['uid'])
        touch(filename)
        string = TwoMetMessage(fildis_npp2)
        to_send = msg_rec.receive(string)
        self.assertDictEqual(to_send, msg_npp2)
        os.remove(filename)

        string = TwoMetMessage(stoprc_npp2)
        to_send = msg_rec.receive(string)
        self.assertTrue(to_send is None)

        filename = os.path.join('/tmp', msg_npp2['uid'])
        touch(filename)
        string = TwoMetMessage(fildis_npp2)
        to_send = msg_rec.receive(string)
        self.assertDictEqual(to_send, msg_npp2)
        os.remove(filename)

        # Terra

        string = TwoMetMessage(stoprc_terra)
        to_send = msg_rec.receive(string)
        self.assertTrue(to_send is None)

        filename = os.path.join('/tmp', msg_terra['uid'])
        touch(filename)
        string = TwoMetMessage(fildis_terra)
        to_send = msg_rec.receive(string)
        self.assertDictEqual(to_send, msg_terra)
        os.remove(filename)

        # NOAA-19

        string = TwoMetMessage(stoprc_n19)
        to_send = msg_rec.receive(string)
        self.assertTrue(to_send is None)

        filename = os.path.join('/tmp', msg_n19['uid'])
        touch(filename)
        string = TwoMetMessage(fildis_n19)
        to_send = msg_rec.receive(string)
        self.assertDictEqual(to_send, msg_n19)
        os.remove(filename)

        # Metop-B

        string = TwoMetMessage(stoprc_m01)
        to_send = msg_rec.receive(string)
        self.assertTrue(to_send is None)

        filename = os.path.join('/tmp', msg_m01['uid'])
        touch(filename)
        string = TwoMetMessage(fildis_m01)
        to_send = msg_rec.receive(string)
        self.assertDictEqual(to_send, msg_m01)
        os.remove(filename)


def suite():
    """Test suite for test_scisys."""
    loader = unittest.TestLoader()
    mysuite = unittest.TestSuite()
    mysuite.addTest(loader.loadTestsFromTestCase(ScisysReceiverTest))

    return mysuite


if __name__ == '__main__':
    unittest.main()
