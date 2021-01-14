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
The scripts communicate with each other using `posttroll`_ messages.

For example, a chain for processing MetOp AVHRR data from direct
reception, in which external software deposits files on the file system,
may look somewhat like this:

* The chain starts with :ref:`trollstalker` to monitor files.
  :ref:`trollstalker` uses inotify and sends a `posttroll`_ message
  when a file appears.
* This message is received by :ref:`gatherer`.  Depending on the
  reception system, a single MetOp AVHRR overpass may produce multiple files.
  :ref:`gatherer` determines what files belong together in a region and sends
  a posttroll message containing all those filenames.
* AVHRR data need preprocessing with the external software `AAPP`_ before
  `Satpy`_ can read the data.  This preprocessing can be done with
  `aapp-runner`_, For this preprocessing, it is advantageous to pass a single
  file.  Therefore, the :ref:`cat` script may be listening to messages from
  :ref:`gatherer` and concatenate files together (it will need `Kai`_ to do
  so).  When done, it sends another message.
* For pre-processing data with AAPP and ANA, `aapp-runner`_ is responsible
  and can be configured to read posttroll messages either from :ref:`cat` or
  directly from :ref:`gatherer`.  See documentation for `aapp-runner`_.

The exact configuration depends on factors that will vary depending on
what satellite data are processed, whether thore are from direct readout,
EUMETCAST, or another source, what system is used for direct readout,
and other factors.

There are example configurations in the ``examples/`` directory.

.. _posttroll: https://posttroll.readthedocs.io/en/latest/
.. _AAPP: https://nwp-saf.eumetsat.int/site/software/aapp/
.. _Satpy: https://satpy.readthedocs.io/
.. _aapp-runner: https://github.com/pytroll/pytroll-aapp-runner
.. _Kai: https://navigator.eumetsat.int/product/EO:EUM:SW:METOP:165

Scripts
-------

All scripts use posttroll messages to communicate.  They are normally
running in the background (as a daemon).  Most are waiting for a posttroll
message to trigger their role and will send another posttroll message when
their task is done.

.. _cat:

cat
^^^

Concatenates granules or segments to a single file.  This may be a useful step
before using external software for preprocessing data.

catter
^^^^^^

Alternative to cat that does something else.

.. _gatherer:

gatherer
^^^^^^^^

Determines what granules with different start times belong together.
A use case may be a reception system in which a single overpass results
in multiple files, that should be grouped together for further processing.

Configuration for gatherer is in ini files.  It needs exactly one ``[default]``
section with exactly one entry ``[regions]``, which contains one or more areas
for which granules are gathered.  Then it needs one or more additional sections
corresponding to what should be gathered.  Each of those additional sections
have the following mandatory fields:

pattern
    Defines the file pattern.  This is needed to create the full list of
    expected files to know what to wait for.

topics
    Defines what posttroll topics to listen to for messages related to files
    having arrived.

publish_topic
    Defines what posttroll topic shall be used to publish the news of all the
    files that have been gathered.

timeliness
    Defines how long to wait before not expecting any more files?

And the following optional fields:

sensor
    Defines the sensor.  This is used for ...

platform_name
    Defines the platform name.  This is used for ...

format
    Defines the file format.  This is used for ...

variant
    Defines variant through which data come in.  Used how?

level
    Data level.  Used how?

duration
    Different from timeliness?

orbit_type
    What type of orbit?  Used how?


scisys_receiver
^^^^^^^^^^^^^^^

.. automodule:: pytroll_collectors.scisys

.. _segment-gatherer:

segment_gatherer
^^^^^^^^^^^^^^^^

.. automodule:: pytroll_collectors.segments

Configuration for ``segment_gatherer`` can be either in ini or yaml
files.  There are several examples in the ``examples/`` directory in
the pytroll-collectors source tree.

.. _trollstalker:

trollstalker
^^^^^^^^^^^^

Monitor when files appear using inotify, typically run as a deamon.
When a file appears, send a message using posttroll, which must be running.
The config determines what file patterns are monitored and what posttroll
messages will be sent, among other things.
Listeners to this message may be, for example,
:ref:`segment-gatherer` or `aapp-runner`_.

.. _aapp-runner: https://github.com/pytroll/pytroll-aapp-runner

trollstalker2
^^^^^^^^^^^^^

New, alternative implementation of trollstalker.  Not really needed,
as trollstalker works fine and is actively maintained.

zipcollector_runner
^^^^^^^^^^^^^^^^^^^

To be documented.

Interface to other packages in the Pytroll ecosystem
----------------------------------------------------

posttroll
^^^^^^^^^

The pytroll-collection scripts use `posttroll`_ to exchange messages with
other pytroll packages.  For example, that message might be "input
file available".  Therefore, `posttroll`_ must be running for the processing
with pytroll-collectors to function.

pytroll-aapp-runner
^^^^^^^^^^^^^^^^^^^

`aapp-runner`_ may be listening to messages from :ref:`cat` or
:ref:`segment-gatherer`.

trollflow2
^^^^^^^^^^

Trollflow2 is the successor of the now-retired trollduction package.
Some of the scripts in pytroll-collectors, such as trollstalker,
segment_gatherer, and gatherer, were previously part of trollduction,
but are now here rather than in trollflow2.  Today trollflow2 may be 
listening to messages sent by scripts from pytroll-collectors.

trollsift
^^^^^^^^^

Used for filename pattern matching, see `trollsift`_ documentation.

.. _trollsift: https://trollsift.readthedocs.io/en/latest/

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
