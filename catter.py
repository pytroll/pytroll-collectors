#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2014 Martin Raspaud

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

"""Concatenate granules at low level if needed.
"""

from posttroll.subscriber import Subscribe
from posttroll.publisher import Publish
from posttroll.message import Message
from ConfigParser import RawConfigParser
import logging
from trollsift import compose
from datetime import datetime
import bz2
import os.path

logger = logging.getLogger(__name__)

config = RawConfigParser()
config.read("catter.cfg")


if __name__ == '__main__':
    subjects = list(set([config.get(section, "subject")
                         for section in config.sections()]))
    print subjects
    with Publish("catter") as pub:
        with Subscribe("", subjects, addr_listener=True) as sub:
            print "waiting for messages"
            for msg in sub.recv():
                print "got message", msg
                if msg.type == "collection":
                    mda = msg.data[0].copy()
                    section = (mda["platform"] + " " +
                               mda["number"] + "/" +
                               mda["level"])
                    print section
                    if section not in config.sections():
                        continue
                    cat = config.get(section, "cat")
                    pattern = config.get(section, "pattern")
                    mda["proc_time"] = datetime.utcnow()
                    mda["end_time"] = msg.data[-1]["end_time"]
                    fname = compose(pattern, mda)
                    mda["uri"] = fname
                    mda["filename"] = os.path.basename(fname)
                    if cat == "bz2":
                        with open(fname, "wb") as out:
                            for cmda in msg.data:
                                infile = bz2.BZ2File(cmda["uri"], "r")
                                out.write(infile.read())
                                infile.close()
                    new_msg = Message(msg.subject, "file", mda)
                    print "sending", new_msg
                    pub.send(str(new_msg))
