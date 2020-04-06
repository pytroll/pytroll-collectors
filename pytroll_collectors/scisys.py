#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2020 Pytroll Developers
#
# Author(s):
#
#   Martin Raspaud <martin.raspaud@smhi.se>
#   Janne Kotro <janne.kotro@fmi.fi>
#   Panu Lahtinen <panu.lahtinen@fmi.fi>
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

"""Receiver for 2met messages, through zeromq.

Outputs messages with the following metadata:
- satellite
- format
- start_time
- end_time
- filename
- uri
- type
- orbit_number
- [instrument, number]

"""
import logging
import os
import socket
import xml.etree.ElementTree as etree
from datetime import datetime, timedelta
from time import sleep
from six.moves.urllib.parse import SplitResult, urlsplit, urlunsplit

from posttroll.message import Message
from posttroll.publisher import Publish
from pytroll_collectors.helper_functions import is_uri_on_server

logger = logging.getLogger(__name__)

JPSS_INSTRUMENTS_FROM_FILENAMES = {"RATMS-RNSCA_": "atms",
                                   "RCRIS-RNSCA_": "cris",
                                   "RNSCA-RVIRS_": "viirs",
                                   "RATMS_": "atms",
                                   "RCRIS_": "cris"}
JPSS_PLATFORM_NAME = {'npp': 'Suomi-NPP',
                      'jpss1': 'NOAA-20',
                      'noaa20': 'NOAA-20',
                      'j01': 'NOAA-20',
                      'j02': 'NOAA-21'}

SCISYS_NAMES = {'Suomi-NPP': 'NPP',
                'NOAA-20': 'NOAA 20',
                'NOAA-21': 'NOAA 21'}


class TwoMetMessage(object):
    """Interperter for 2met! messages."""

    def __init__(self, mstring=None):
        """Init the message."""
        self._id = 0
        self._type = ""
        self._time = datetime.utcnow()
        self.body = ''
        if mstring is not None:
            self._decode(mstring.strip())
        self._attrs = {}

    def _internal_decode(self, mstring):
        """Decode 2met! messages, internal format."""
        dummy, content = mstring.split("[", 1)
        content = content.rsplit("]", 1)[0]
        dic = dict((item.split("=", 1) for item in content.split(", ", 3)))
        self._id = eval(dic["ID"])
        self._time = datetime.strptime(
            eval(dic["time"]), "%d %m %Y - %H:%M:%S")
        try:
            self.body = eval(dic["body"])
        except SyntaxError:
            self.body = str(dic["body"])
        self._type = eval(dic["type"])

    def _xml_decode(self, mstring):
        """Decode xml 2met! messages."""
        root = etree.fromstring(mstring)
        self._attrs = dict(root.items())

        self._id = int(root.get("sequence"))
        self._type = root.get("type")
        self._time = datetime.strptime(root.get("timestamp"),
                                       "%Y-%m-%dT%H:%M:%S")
        for child in root:
            if child.tag == "body":
                self.body = child.text

    def _decode(self, mstring):
        """Decode 2met! messages."""
        if mstring.startswith("Message["):
            self._internal_decode(mstring)
        elif mstring.startswith("<message"):
            try:
                self._xml_decode(mstring)
            except Exception:
                logger.exception("Spurious message! %s", str(mstring))
        else:
            logger.warning("Don't know how to decode message: %s",
                           str(mstring))


def pass_name(utctime, satellite):
    """Construct a unique pass name from a risetime and a satellite name."""
    # return utctime.strftime("%Y%m%dT%H%M%S") + "_".join(satellite.split(" "))
    return utctime, "_".join(satellite.split(" "))


class PassRecorder(dict):
    """Recorder for passes."""

    def iter(self):
        """Return an iterator over the passes."""
        if hasattr(self, 'iteritems'):
            return self.iteritems()  # noqa
        else:
            return self.items()

    def get(self, key, default=None):
        """Get a pass."""
        utctime, satellite = key
        for (rectime, recsat), val in self.iter():
            if(recsat == satellite and
               (abs(rectime - utctime)).seconds < 30 * 60 and
               (abs(rectime - utctime)).days == 0):
                return val
        return default


class MessageReceiver(object):
    """Interprets received messages between stop reception and file dispatch."""

    def __init__(self, emitter, excluded_satellite_list=None,
                 target_server=None, ftp_prefix=None):
        """Init the receiver."""
        self._received_passes = PassRecorder()
        self._distributed_files = {}
        self._emitter = emitter
        if excluded_satellite_list is not None:
            self._excluded_platforms = excluded_satellite_list
        else:
            self._excluded_platforms = []
        self._target_server = target_server
        self._ftp_prefix = ftp_prefix

    def add_pass(self, message):
        """Format pass info and add it to the object."""
        info = dict((item.split(": ", 1) for item in message.split(", ", 3)))
        logger.info("Adding pass: %s", str(info))
        pass_info = {}
        for key, val in info.items():
            pass_info[key.lower()] = val
        pass_info["start_time"] = datetime.strptime(pass_info["risetime"],
                                                    "%Y-%m-%d %H:%M:%S")
        del pass_info['risetime']
        pass_info["end_time"] = datetime.strptime(pass_info["falltime"],
                                                  "%Y-%m-%d %H:%M:%S")
        del pass_info['falltime']

        if 'orbit number' in pass_info:
            pass_info['orbit_number'] = int(pass_info['orbit number'])
            del pass_info['orbit number']
        else:
            logger.warning("No 'orbit number' in message!")

        pname = pass_name(pass_info["start_time"], pass_info["satellite"])
        self._received_passes[pname] = pass_info

    def clean_passes(self, days=1):
        """Clean old passes from the pass dict (`_received_passes`)."""
        oldies = []

        for key, val in self._received_passes.items():
            if (datetime.utcnow() - val["start_time"]).days >= days:
                oldies.append(key)

        for key in oldies:
            del self._received_passes[key]

    def handle_distrib(self, url):
        """React to a file dispatch message."""
        url = urlsplit(url)
        dummy, filename = os.path.split(url.path)
        logger.debug("filename = %s", filename)
        # TODO: Should not make any assumptions on filename formats, should
        # load a description of it from a config file instead.
        if filename.endswith(".hmf"):
            risestr, satellite = filename[:-4].split("_", 1)
            risetime = datetime.strptime(risestr, "%Y%m%d%H%M%S")
            pname = pass_name(risetime, satellite)
            satellite = satellite.replace("_", "-")
            if satellite in self._excluded_platforms:
                return None
            swath = self._received_passes.get(pname, {}).copy()
            swath.pop('satellite', None)
            swath["platform_name"] = satellite
            swath["start_time"] = risetime
            swath["type"] = "binary"
            swath["format"] = "HRPT"
            if satellite == "NOAA-15":
                swath["sensor"] = ("avhrr/3", "amsu-a", "amsu-b", "hirs/3")
            elif satellite in ["NOAA-18", "NOAA-19"]:
                swath["sensor"] = ("avhrr/3", "mhs", "amsu-a", "hirs/4")
            swath["data_processing_level"] = "0"

        elif filename.startswith("P042") or filename.startswith("P154"):
            pds = {}
            pds["format"] = filename[0]
            pds["apid1"] = filename[1:8]
            pds["apid2"] = filename[8:15]
            pds["apid3"] = filename[15:22]
            pds["time"] = datetime.strptime(filename[22:33], "%y%j%H%M%S")
            pds["nid"] = filename[33]
            pds["ufn"] = filename[34:36]
            pds["extension"] = filename[36:40]
            risetime = pds["time"]
            if pds["apid1"][:3] == "042":
                satellite = "EOS-Terra"
                pname = pass_name(risetime, 'TERRA')
            elif pds["apid1"][:3] == "154":
                satellite = "EOS-Aqua"
                pname = pass_name(risetime, 'AQUA')
            else:
                raise ValueError(
                    "Unrecognized satellite ID: " + pds["apid1"][:3])

            if not satellite or satellite in self._excluded_platforms:
                logger.debug("Platform name %s is excluded...", str(satellite))
                return None

            swath = self._received_passes.get(pname, {}).copy()
            swath.pop('satellite', None)
            swath['platform_name'] = satellite
            swath['start_time'] = risetime
            instruments = {"0064": "modis",
                           "0141": "ceres+y",
                           "0157": "ceres-y",
                           "0261": "amsu-a1",
                           "0262": "amsu-a1",
                           "0290": "amsu-a2",
                           "0342": "hsb",
                           "0402": "amsr-e",
                           "0404": "airs",
                           "0405": "airs",
                           "0406": "airs",
                           "0407": "airs",
                           "0414": "airs",
                           "0415": "airs",
                           "0419": "airs",
                           "0957": "gbad",
                           }
            swath["sensor"] = instruments.get(pds["apid1"][3:],
                                              pds["apid1"][3:])
            swath["format"] = "PDS"
            swath["type"] = "binary"
            swath["data_processing_level"] = "0"

        # NPP/JPSS RDRs
        elif filename.startswith("R") and filename.endswith(".h5"):
            # Occassionaly RT-STPS produce files with a nonstandard file
            # naming, lacking the 'RNSCA' field. We will try to deal with this
            # below (Adam - 2013-06-04):
            mda = {}
            mda["format"] = filename[0]
            file_ok = False
            for prefix in JPSS_INSTRUMENTS_FROM_FILENAMES:
                if filename.startswith(prefix):
                    mda["sensor"] = JPSS_INSTRUMENTS_FROM_FILENAMES[prefix]
                    start_time_items = filename.strip(prefix).split('_')[1:3]
                    end_time_item = filename.strip(prefix).split('_')[3]
                    satellite = JPSS_PLATFORM_NAME.get(filename.strip(prefix).split('_')[0], None)
                    orbit = filename.strip(prefix).split('_')[4].strip('b')
                    file_ok = True
                    break

            if not file_ok:
                logger.warning("Seems to be a NPP/JPSS RDR "
                               "file but name is not standard!")
                logger.warning("filename = %s", filename)
                return None

            # satellite = "Suomi-NPP, NOAA-20, NOAA-21,..."
            if not satellite or satellite in self._excluded_platforms:
                logger.debug("Platform name %s is excluded...", str(satellite))
                return None

            mda["start_time"] = \
                datetime.strptime(start_time_items[0] + start_time_items[1],
                                  "d%Y%m%dt%H%M%S%f")
            end_time = \
                datetime.strptime(start_time_items[0] + end_time_item,
                                  "d%Y%m%de%H%M%S%f")
            if mda["start_time"] > end_time:
                end_time += timedelta(days=1)
            mda["orbit"] = orbit

            # FIXME: swath start and end time is granule dependent.
            # Get the end time as well! - Adam 2013-06-03:
            start_time = mda["start_time"]
            pname = pass_name(start_time, SCISYS_NAMES.get(satellite))

            swath = self._received_passes.get(pname, {}).copy()
            swath.pop("satellite", None)
            swath["platform_name"] = satellite
            swath["start_time"] = start_time
            swath['end_time'] = end_time
            swath["sensor"] = mda["sensor"]
            swath["format"] = "RDR"
            swath["type"] = "HDF5"
            swath["data_processing_level"] = "0"

        # metop
        elif filename[4:12] == "_HRP_00_":
            # "AVHR": "avhrr",
            instruments = {"ASCA": "ascat",
                           "AMSA": "amsu-a",
                           "ATOV": "atovs",
                           "AVHR": "avhrr/3",
                           "GOME": "gome",
                           "GRAS": "gras",
                           "HIRS": "hirs/4",
                           "IASI": "iasi",
                           "MHSx": "mhs",
                           "SEMx": "sem",
                           "ADCS": "adcs",
                           "SBUV": "sbuv",
                           "HKTM": "vcdu34"}

            satellites = {"M02": "Metop-A",
                          "M01": "Metop-B",
                          "M03": "Metop-C"}

            satellite = satellites[filename[12:15]]
            risetime = datetime.strptime(filename[16:31], "%Y%m%d%H%M%SZ")
            falltime = datetime.strptime(filename[32:47], "%Y%m%d%H%M%SZ")

            pname = pass_name(risetime, satellite.upper())
            logger.debug("pname= % s", str(pname))
            swath = self._received_passes.get(pname, {}).copy()
            swath.pop('satellite', None)
            swath["start_time"] = risetime
            swath["end_time"] = falltime
            swath["platform_name"] = satellite
            swath["sensor"] = instruments[filename[:4]]
            swath["format"] = "EPS"
            swath["type"] = "binary"
            swath["data_processing_level"] = "0"
        else:
            return None

        if url.scheme in ["", "file"]:
            scheme = "ssh"
            netloc = self._emitter
            uri = urlunsplit(SplitResult(scheme,
                                         netloc,
                                         url.path,
                                         url.query,
                                         url.fragment))
        elif url.scheme == "ftp":
            scheme = "ssh"
            netloc = url.hostname
            uri = urlunsplit(SplitResult(scheme,
                                         netloc,
                                         url.path,
                                         url.query,
                                         url.fragment))
        else:
            logger.debug("url.scheme not expected: %s", url.scheme)

        swath["uid"] = os.path.split(url.path)[1]
        swath["uri"] = uri
        swath['variant'] = 'DR'
        return swath

    def receive(self, message):
        """Receive the messages and triage them."""
        metadata_stop = "STOPRC Stop reception: "
        metadata_start = "STRTRC Start reception: Satellite"
        dispatch_prefix = "FILDIS File Dispatch: "
        dispatch_prefix2 = "SUCTRN "
        if (message.body.startswith(metadata_stop) or
                message.body.startswith(metadata_start)):
            self.add_pass(message.body.split(":", 1)[1].strip())
            return None
        elif message.body.startswith(dispatch_prefix):
            # Check hostname in message:
            filename, url = message.body[len(dispatch_prefix):].split(" ")
            url = compose_dest_url(filename, url)
            if is_uri_on_server(url, strict=True):
                return self.handle_distrib(url)
        elif message.body.startswith(dispatch_prefix2):
            filename, _, url = message.body[len(dispatch_prefix2):].split(" ")
            ip_address, path = url.split(":")
            logger.debug("Sent to IP: %s", str(ip_address))
            logger.debug("Target server IPs: %s ", str(self._target_server))
            if ip_address in self._target_server:
                logger.debug("Found correct target server: %s", ip_address)
                if self._ftp_prefix is not None:
                    path = os.path.join(self._ftp_prefix, path)
                    logger.debug("ftp_prefix added: %s", path)
                url = 'ftp://' + ip_address + path
                logger.debug("filename = <%s> url = <%s>", filename, url)
                url = compose_dest_url(filename, url)
                logger.debug("url = %s", url)
                return self.handle_distrib(url)
            else:
                logger.debug("File is not on targer server.")
                return None


def compose_dest_url(source, dest):
    """Take the filename from *source* and append it to *dest* if needed."""
    filename = os.path.split(source)[1]
    if not dest.endswith(filename):
        dest = urlsplit(dest)
        dest = urlunsplit((dest[0], dest[1],
                           os.path.join(dest[2], filename),
                           dest[3], dest[4]))
    return dest


class GMCSubscriber(object):
    """Listener for GMC messages."""

    def __init__(self, host, port):
        """Init the subscriber."""
        self._host = host
        self._port = port
        self._sock = None
        self.msg = ""
        self._bufsize = 256
        self.loop = True

    def recv(self):
        """Receive messages."""
        while self.loop:
            self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            try:
                self._sock.connect((self._host, self._port))
            except socket.error:
                logger.error("Cannot connect to %s, retrying in 60 seconds.",
                             str((self._host, self._port)))
                sleep(60)
                continue
            self._sock.settimeout(1.0)
            try:
                while self.loop:
                    try:
                        data = self._sock.recv(self._bufsize)
                    except socket.timeout:
                        pass
                    else:
                        if not data:
                            break
                        self.msg += data.decode('utf-8')
                        messages = self.msg.split("</message>")
                        if len(messages) > 1:
                            for mess in messages[:-1]:
                                yield mess + "</message>"
                            if messages[-1].endswith("</body>"):
                                yield messages[-1] + "</message>"
                                self.msg = ""
                            else:
                                self.msg = messages[-1]
                        elif self.msg.endswith("</message>"):
                            yield self.msg
                            self.msg = ""
            finally:
                self._sock.close()

    def stop(self):
        """Stop the listener."""
        self.loop = False


def receive_from_zmq(host, port, station, environment, excluded_platforms,
                     target_server, ftp_prefix, topic_postfix,
                     publish_port=0, nameservers=None, days=1):
    """Receive 2met! messages from zeromq."""
    logger.debug("target_server: %s", str(target_server))
    logger.debug("ftp_prefix: %s", str(ftp_prefix))
    logger.debug("topic_postfix: %s", str(topic_postfix))
    logger.debug("station %s", str(station))
    logger.debug(type(target_server))

    # socket = Subscriber(["tcp://localhost:9331"], ["2met!"])
    sock = GMCSubscriber(host, port)
    msg_rec = MessageReceiver(host, excluded_platforms,
                              target_server, ftp_prefix)

    with Publish("receiver", port=publish_port,
                 aliases=["HRPT 0", "PDS", "RDR", "EPS 0"],
                 nameservers=nameservers) as pub:
        for rawmsg in sock.recv():
            # TODO:
            # - Watch for idle time in order to detect a hangout
            logger.debug("receive from 2met! %s", str(rawmsg))
            string = TwoMetMessage(rawmsg)
            to_send = msg_rec.receive(string)
            logger.debug("to_send: %s", str(to_send))
            if to_send is None:
                continue
            if topic_postfix is not None:
                subject = "/".join(("", to_send['format'],
                                   to_send['data_processing_level'],
                                   topic_postfix))
            else:
                subject = "/".join(("", to_send['format'],
                                   to_send['data_processing_level'],
                                   station, environment,
                                   "polar", "direct_readout"))
            logger.debug("Subject: %s", str(subject))
            msg = Message(subject,
                          "file",
                          to_send).encode()
            logger.debug("publishing %s", str(msg))
            pub.send(msg)
            if days:
                msg_rec.clean_passes(days)
