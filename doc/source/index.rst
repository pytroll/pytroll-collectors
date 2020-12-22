.. pytroll-collectors documentation master file, created by
   sphinx-quickstart on Tue Dec 22 16:13:24 2020.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Welcome to pytroll-collectors's documentation!
==============================================

.. toctree::
   :maxdepth: 2
   :caption: Contents:

Pytroll-collectors is a collection of scripts and modules to aid in
operational processing of satellite reception data.

There are example configurations in the example/ directory.

There are no examples yet on how to run any of the scripts.

Scripts
-------

cat
^^^

Concatenates different segments, after they are gathered?

catter
^^^^^^

??

gatherer
^^^^^^^^

Probably different from segment_gatherer.

scisys_receiver
^^^^^^^^^^^^^^^

For direct readout using the scisys system?

segment_gatherer
^^^^^^^^^^^^^^^^

Geostationary (and polar?) satellite sensor files may be distributed in
segments, but the user may want to wait with processing those until all
segments are complete.
This script sends a message (event?) using posttroll when all segments are
ready.

trollstalker
^^^^^^^^^^^^

Monitors when files appear.
This may be the case if a software package external to pytroll copies
files read from direct readout or EUMETCAST to an input directory on a
system.
When a file appears, trollstalker does something (what?).

trollstalker2
^^^^^^^^^^^^^

New implementation of trollstalker?

Interface to other packages in the Pytroll ecosystem
----------------------------------------------------

posttroll
^^^^^^^^^

The pytroll-collection scripts use posttroll to exchange messages with
other pytroll packages.  For example, that message might be "input
file available".

pytroll-schedule
^^^^^^^^^^^^^^^^

trollflow2
^^^^^^^^^^

Trollflow2 is the successor of the now-retired trollduction package.
Some of the content of trollduction was moved to pytroll-collectors.

trollsift
^^^^^^^^^

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
