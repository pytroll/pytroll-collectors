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
The scripts run continuously (like daemons) and
communicate with each other using `posttroll`_ messages.

For example, a chain for processing Metop AVHRR data from direct
reception, in which external software deposits files on the file system,
may look somewhat like this:

* The chain starts with :ref:`trollstalker` to monitor files.
  :ref:`trollstalker` uses inotify and sends a `posttroll`_ message
  when a file appears.
* This message is received by :ref:`gatherer`.  Depending on the
  reception system, a single Metop AVHRR overpass may produce multiple files.
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
what satellite data are processed, whether those are from direct readout,
EUMETCAST, or another source, what system is used for direct readout,
and other factors.
Some users use the 3rd party software `supervisor`_ to start and monitor
the different scripts in pytroll-collectors.

There are example configurations in the ``examples/`` directory.

.. _posttroll: https://posttroll.readthedocs.io/en/latest/
.. _AAPP: https://nwp-saf.eumetsat.int/site/software/aapp/
.. _Satpy: https://satpy.readthedocs.io/
.. _aapp-runner: https://github.com/pytroll/pytroll-aapp-runner
.. _Kai: https://navigator.eumetsat.int/product/EO:EUM:SW:METOP:165
.. _supervisor: http://supervisord.org/

Scripts
-------

All scripts use posttroll messages to communicate.  They are normally
running in the background (as a daemon).  Most are waiting for a posttroll
message to trigger their role and will send another posttroll message when
their task is done.

.. _cat:

cat.py
^^^^^^

Concatenates granules or segments from NOAA and Metop level 0 data to a
single file.  This may be a useful step before using external software
for preprocessing data.  In particular, AAPP removes some scanlines from
each end of the granule, so processing single granules will leave gaps
between them.  You will need `Kai`_ from EUMETSAT to concatenate Metop
granules (but not for NOAA granules).  Cat listens to input messages
via posttroll according to topics defined in the configuration file.
Upon completion, it will publish a posttroll message with topic defined
in the configuration file.

The kai configuration file is in the INI format and should have one ore
more sections.  Each section may have the following fields:

output_file_pattern
    A pattern (trollsift syntax) on what the output file should be.

aliases
    Optional (what does it do?)

min_length
    Optional, integer, minimum number of minutes needed to consider the data

command
    Command used for concatenation.

stdout
    Optional; if command writes to stdout, redirect output here.

publish_topic
    Optional; publish a message when file is produced, using this topic.

publish_port
    Optional; use a custom port when publishing a message.

nameservers
    Optional; nameservers to publish on.

subscriber_nameserver
    Optional; nameserver to listen to.

.. _Kai: https://navigator.eumetsat.int/product/EO:EUM:SW:METOP:165

Example configuration:

.. literalinclude:: ../../examples/cat.cfg_template
   :language: ini

catter
^^^^^^

Alternative to cat that does something else.

.. literalinclude:: ../../examples/catter.cfg_template
   :language: ini

.. _gatherer:

gatherer
^^^^^^^^

Collects granulated swath data so that the granules cover the configured
target area(s) in a contiguous manner.  Uses `pytroll-schedule`_ (which
uses `pyorbital`_) to calculate the required granules using orbital
parameters (Three Line Elements; TLEs).

Watches files or messages and gathers satellite granules in "collections",
sending then the collection of files in a message for further processing.
Determines what granules with different start times belong together.
A use case may be a reception system in which a single overpass results in
multiple files, that should be grouped together for further processing.
It uses `pytroll-schedule`_ to estimate the area coverage based on start
and end times contained in filenames.

The  ``gatherer`` collection is started when it receives a posttroll message,
perhaps from `trollstalker`_ or `segment-gatherer`_.  Using the configured
granule duration and the area of interest, it calculates the starting times
of granules it should expect to be covered in this area before and after the
granule it was messaged about.  Collection is considered finished when either
all expected granules have been collected or when a timeout is reached,
whatever comes first.  Timeout is configured with the ``timeliness`` option
(see below).

.. _pytroll-schedule: http://pytroll-schedule.readthedocs.org/
.. _pyorbital: https://pyorbital.readthedocs.io/en/latest/

The configuration file in INI format needs a section called ``[DEFAULT]``
and one or more sections
corresponding to what should be gathered.  The ``[DEFAULT]`` section holds common items for all other sections.  It can be used to define the regions:

regions
    A whitespace separated list of names corresponding to areas for
    which granules are gathered.

All other sections have the following mandatory fields:

pattern
    Defines the file pattern.  This is needed to create the full list of
    expected files to know what to wait for.  If you don't pass this,
    gatherer will not fail, but ...

topics
    Defines what posttroll topics to listen to for messages related to files
    having arrived.

publish_topic
    Defines what posttroll topic shall be used to publish the news of all the
    files that have been gathered.

timeliness
    Defines the maximum allowed age of the granule in minutes (Warning:
    unit different compared to duration).  Collection is stopped
    ``timeliness`` minutes after the expected end time of the last expected
    granule.

And the following optional fields:

service
    The posttroll service name which publishing the messages.
    If given, only subscribe to messages from this service.

sensor
    Defines the sensor.  This is used for ...

platform_name
    Defines the platform name.  This is used for ...

format
    Defines the file format.  This is used for ...

type
    File type.  Used how?  Difference with format?

variant
    Defines variant through which data come in.  Used how?

level
    Data level.  Some downstream scripts may expect to see this in the
    messages they receive.

duration
    Duration of a granule in seconds (Warning: unit different compared to timeliness)

orbit_type
    What type of orbit?  Some downstream scripts may expect to receive this
    information through posttroll messages.

nameserver
    Nameserver to use to publish posttroll messages.

.. literalinclude:: ../../examples/gatherer_config.ini_template
   :language: ini

scisys_receiver
^^^^^^^^^^^^^^^

Translates messages published by Scisys reception software to posttroll
messages.

.. automodule:: pytroll_collectors.scisys

.. _segment-gatherer:

segment_gatherer
^^^^^^^^^^^^^^^^

.. automodule:: pytroll_collectors.segments

Collects together files that belong together for a single time step.

Geostationary example: Single full disk dataset of Meteosat SEVIRI data
are segmented to 144 separate files. These are prolog (PRO), epilog (EPI),
24 segments for HRV and 8 segments for each of the lower level channels.
For processing, some of those segments are essential (if absent, no
processing can take place), others are optional (if one segment in the
middle is missing, an image can be produced, but it will have a gap).

Low Earth Orbit (LEO) example: EARS/VIIRS data are split to M-channel
(includes all M-channels) files and DNB-channel files. These files have
the same start and end times and coverage, just different data.

Historically this was created to collect SEVIRI segments, which has some
impact on the configuration.

The YAML format supports collection of several different data together. As
an example: SEVIRI data and NWC SAF GEO products.

Configuration for ``segment_gatherer`` can be either in ini or yaml
files.  There are several examples in the ``examples/`` directory in
the pytroll-collectors source tree.

Example ini config:

.. literalinclude:: ../../examples/segment_gatherer.ini_template
   :language: ini

Example yaml config:

.. literalinclude:: ../../examples/segment_gatherer_msg_and_iodc.yaml_template
   :language: yaml

.. _trollstalker:

trollstalker
^^^^^^^^^^^^

Trollstalker is an alternative for users who do not use the
`trollmoves`_ client/server system.  If file transfers are done through
`trollmoves`_, there is no need for trollstalker.  If file transfers are
done through any other software, trollstalker can be used to detect file
arrival.

It is typically run as a daemon or via a process control system such
as `supervisord`_ or `daemontools`_.  When such a file is detected,
a pytroll message is sent on the network via the posttroll nameserver
(which must be running) to notify other interested processes.

In order to start *trollstalker*::

  $ cd pytroll-collectors/bin/
  $ ./trollstalker.py -c ../examples/trollstalker_config.ini -C noaa_hrpt

Now you can test if the messaging works by copying a data file to your input
directory. *Trollstalker* should send a message, and depending on the
configuration, also print the message on the terminal. If there's no message,
check the configuration files that the input directory and file pattern are set
correctly.

The config determines what file patterns are monitored and what posttroll
messages will be sent, among other things.
Listeners to this message may be, for example,
:ref:`segment-gatherer` or `aapp-runner`_.

Configuration files have one section per file type that is listened to.
To listen to multiple file types, start ``trollstalker`` multiple times.
The message sent by ``trollstalker`` contains a dictionary which contains:

- All fields from the ``filepattern``, and
- Any keys starting with ``var_`` in the configuration file and their values.

The additional keys may be essential if the package listening to
trollstalker messages expects an entry in the posttroll message that
is normally extracted from the filename.  For example, :ref:`gatherer`
needs a ``platform_name`` to be present at all times.  If a filename does
not contain a platform name or is for some other reason not matched
with a trollsift pattern, it may need to be sent explicitly with
``var_platform_name``.

.. literalinclude:: ../../examples/trollstalker_config.ini_template
   :language: ini

.. _aapp-runner: https://github.com/pytroll/pytroll-aapp-runner
.. _supervisord: http://supervisord.org/
.. _daemontools: http://cr.yp.to/daemontools.html
.. _trollmoves: https://github.com/pytroll/trollmoves

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
