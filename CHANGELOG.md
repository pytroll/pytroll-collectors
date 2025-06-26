## Version 0.18.0 (2025/06/26)


### Pull Requests Merged

#### Features added

* [PR 162](https://github.com/pytroll/pytroll-collectors/pull/162) - Ensure filesystem is passed for remote file checks
* [PR 154](https://github.com/pytroll/pytroll-collectors/pull/154) - Handle sensor list in topic

In this release 2 pull requests were closed.


###############################################################################
## Version 0.17.0 (2024/11/13)

### Issues Closed

* [Issue 149](https://github.com/pytroll/pytroll-collectors/issues/149) - trollstalker seems to do nothing in current main

In this release 1 issue was closed.

### Pull Requests Merged

#### Features added

* [PR 157](https://github.com/pytroll/pytroll-collectors/pull/157) - Add SIGTERM handling to geographic gatherer
* [PR 156](https://github.com/pytroll/pytroll-collectors/pull/156) - Add SIGTERM handling to segment gatherer

In this release 2 pull requests were closed.

###############################################################################
## Version 0.16.0 (2024/02/16)

### Issues Closed

* [Issue 147](https://github.com/pytroll/pytroll-collectors/issues/147) - Installing fails on Python 3.12; versioneer update needed?

In this release 1 issue was closed.

### Pull Requests Merged

#### Bugs fixed

* [PR 144](https://github.com/pytroll/pytroll-collectors/pull/144) - Update versioneer

#### Features added

* [PR 153](https://github.com/pytroll/pytroll-collectors/pull/153) - Update CI Python versions
* [PR 152](https://github.com/pytroll/pytroll-collectors/pull/152) - Use factory function for publisher creation in trollstalker and switch to Watchdog
* [PR 141](https://github.com/pytroll/pytroll-collectors/pull/141) - Add tests and refactor trollstalker a bit
* [PR 139](https://github.com/pytroll/pytroll-collectors/pull/139) - Create monitored directory if does not exist
* [PR 138](https://github.com/pytroll/pytroll-collectors/pull/138) - Fix noaa21
* [PR 137](https://github.com/pytroll/pytroll-collectors/pull/137) - Create publish topic in scisys-receiver from config-pattern and message

In this release 7 pull requests were closed.


###############################################################################
## Version 0.15.1 (2023/04/27)

### Pull Requests Merged

#### Bugs fixed

* [PR 136](https://github.com/pytroll/pytroll-collectors/pull/136) - Drop S3 credentials from messages in S3Stalker (+ daemon)
* [PR 135](https://github.com/pytroll/pytroll-collectors/pull/135) - Fix calling s3.ls() to get also new files for the daemon version

#### Features added

* [PR 125](https://github.com/pytroll/pytroll-collectors/pull/125) - Change publish topic to start with the instrument name

In this release 3 pull requests were closed.


###############################################################################
## Version 0.15.0 (2023/03/27)


### Pull Requests Merged

#### Bugs fixed

* [PR 133](https://github.com/pytroll/pytroll-collectors/pull/133) - Make it possible to tell segment gatherer that all the files are local

#### Features added

* [PR 133](https://github.com/pytroll/pytroll-collectors/pull/133) - Make it possible to tell segment gatherer that all the files are local

In this release 2 pull requests were closed.

## Version 0.14.0 (2023/03/20)

### Issues Closed

* [Issue 127](https://github.com/pytroll/pytroll-collectors/issues/127) - ImportError in bin/trollstalker2.py ([PR 128](https://github.com/pytroll/pytroll-collectors/pull/128) by [@paulovcmedeiros](https://github.com/paulovcmedeiros))

In this release 1 issue was closed.

### Pull Requests Merged

#### Bugs fixed

* [PR 132](https://github.com/pytroll/pytroll-collectors/pull/132) - Fix group_by_minutes to include composed filenames
* [PR 131](https://github.com/pytroll/pytroll-collectors/pull/131) - Bugfix segment gathering when segment name has a dash in it
* [PR 128](https://github.com/pytroll/pytroll-collectors/pull/128) - Fix import error in bin/trollstalker2.py ([127](https://github.com/pytroll/pytroll-collectors/issues/127))

#### Features added

* [PR 131](https://github.com/pytroll/pytroll-collectors/pull/131) - Bugfix segment gathering when segment name has a dash in it
* [PR 117](https://github.com/pytroll/pytroll-collectors/pull/117) - Check for existing segments on S3 storage
* [PR 114](https://github.com/pytroll/pytroll-collectors/pull/114) - Add an s3stalker daemon

In this release 6 pull requests were closed.


## Version 0.13.0 (2022/10/18)

### Issues Closed

* [Issue 112](https://github.com/pytroll/pytroll-collectors/issues/112) - segment_gatherer publishes incorrect end_time when using group_by_minutes
* [Issue 72](https://github.com/pytroll/pytroll-collectors/issues/72) - gatherer does not complain when configuration file does not exist

In this release 2 issues were closed.

### Pull Requests Merged

#### Bugs fixed

* [PR 107](https://github.com/pytroll/pytroll-collectors/pull/107) - Fix too many protocols

#### Features added

* [PR 118](https://github.com/pytroll/pytroll-collectors/pull/118) - Ensure files in S3 keep their full URIs in messages
* [PR 116](https://github.com/pytroll/pytroll-collectors/pull/116) - Catch KeyError from collector from missing TLE
* [PR 115](https://github.com/pytroll/pytroll-collectors/pull/115) - Add CI labels
* [PR 111](https://github.com/pytroll/pytroll-collectors/pull/111) - When adding to existing timeslot, indicate which one
* [PR 106](https://github.com/pytroll/pytroll-collectors/pull/106) - Add end to end test for the geographic gatherer

#### Documentation changes

* [PR 110](https://github.com/pytroll/pytroll-collectors/pull/110) - Improving segment gatherer documentation

In this release 7 pull requests were closed.


## Version 0.12.0 (2022/05/19)


### Pull Requests Merged

#### Features added

* [PR 105](https://github.com/pytroll/pytroll-collectors/pull/105) - Allow direct listening to host/port in geographic gatherer.
* [PR 104](https://github.com/pytroll/pytroll-collectors/pull/104) - Refactor the fsspec to message functions
* [PR 102](https://github.com/pytroll/pytroll-collectors/pull/102) - Make it possible to disable nameserver connections for publisher in Segment and Geographic gatherers
* [PR 101](https://github.com/pytroll/pytroll-collectors/pull/101) - Cleanup dependencies
* [PR 98](https://github.com/pytroll/pytroll-collectors/pull/98) - Change tested Python versions to 3.8, 3.9 and 3.10
* [PR 97](https://github.com/pytroll/pytroll-collectors/pull/97) - Replace `yaml.load()` with `yaml.safe_load()` in `zipcollector_runner.py`

In this release 6 pull requests were closed.


## Version 0.11.1 (2021/08/27)

### Pull Requests Merged

#### Bugs fixed

* [PR 96](https://github.com/pytroll/pytroll-collectors/pull/96) - Fix coverage logging for non-file messages in region collector

In this release 1 pull request was closed.

## Version 0.11.0 (2021/08/25)

### Issues Closed

* [Issue 91](https://github.com/pytroll/pytroll-collectors/issues/91) - Starting the geographic gatherer fails with ModuleNotFoundError
* [Issue 87](https://github.com/pytroll/pytroll-collectors/issues/87) - Remove usage of six ([PR 88](https://github.com/pytroll/pytroll-collectors/pull/88) by [@pnuu](https://github.com/pnuu))
* [Issue 80](https://github.com/pytroll/pytroll-collectors/issues/80) - SegmentGatherer: configuration of time_name via ini not possible
* [Issue 73](https://github.com/pytroll/pytroll-collectors/issues/73) - gatherer fails with KeyError: 'format' when fallback to default format and no format in metadata
* [Issue 70](https://github.com/pytroll/pytroll-collectors/issues/70) - ValueError: can't have unbuffered text I/O with python-dæmon 2.2.4
* [Issue 65](https://github.com/pytroll/pytroll-collectors/issues/65) - Go through Gatherer and Segment Gatherer log messages ([PR 93](https://github.com/pytroll/pytroll-collectors/pull/93) by [@pnuu](https://github.com/pnuu))
* [Issue 52](https://github.com/pytroll/pytroll-collectors/issues/52) - Add option to adjust start and/or end time after pass calculation is made in gatherer ([PR 90](https://github.com/pytroll/pytroll-collectors/pull/90) by [@pnuu](https://github.com/pnuu))
* [Issue 14](https://github.com/pytroll/pytroll-collectors/issues/14) - Documentation is missing! ([PR 68](https://github.com/pytroll/pytroll-collectors/pull/68) by [@gerritholl](https://github.com/gerritholl))

In this release 8 issues were closed.

### Pull Requests Merged

#### Bugs fixed

* [PR 95](https://github.com/pytroll/pytroll-collectors/pull/95) - Use only 'path' portion of the URI in `cat.py`

#### Features added

* [PR 93](https://github.com/pytroll/pytroll-collectors/pull/93) - Adjust logging levels ([65](https://github.com/pytroll/pytroll-collectors/issues/65))
* [PR 90](https://github.com/pytroll/pytroll-collectors/pull/90) - Limit geographic gathering using overpass schedules ([52](https://github.com/pytroll/pytroll-collectors/issues/52))
* [PR 88](https://github.com/pytroll/pytroll-collectors/pull/88) - Remove 'six' as requirement ([87](https://github.com/pytroll/pytroll-collectors/issues/87))
* [PR 85](https://github.com/pytroll/pytroll-collectors/pull/85) - Refactor geographic gathering
* [PR 84](https://github.com/pytroll/pytroll-collectors/pull/84) - Add a way to populate segment gatherer with pre-existing files
* [PR 81](https://github.com/pytroll/pytroll-collectors/pull/81) - Add Github Action to run unit tests
* [PR 68](https://github.com/pytroll/pytroll-collectors/pull/68) - First begin with documentation ([14](https://github.com/pytroll/pytroll-collectors/issues/14))
* [PR 67](https://github.com/pytroll/pytroll-collectors/pull/67) - Add s3stalker.py

#### Documentation changes

* [PR 68](https://github.com/pytroll/pytroll-collectors/pull/68) - First begin with documentation ([14](https://github.com/pytroll/pytroll-collectors/issues/14))

In this release 10 pull requests were closed.


## Version 0.10.0 (2020/11/11)


### Pull Requests Merged

#### Features added

* [PR 64](https://github.com/pytroll/pytroll-collectors/pull/64) - Add the possibility to handle collections and datasets in input messages.

In this release 1 pull request was closed.


## Version 0.9.0 (2020/04/15)


### Pull Requests Merged

#### Bugs fixed

* [PR 62](https://github.com/pytroll/pytroll-collectors/pull/62) - publish service name needs to be equal each time
* [PR 59](https://github.com/pytroll/pytroll-collectors/pull/59) - Fix target_server's default value to be localhost's ip adresses
* [PR 56](https://github.com/pytroll/pytroll-collectors/pull/56) - Fixed naming of the publish service name.
* [PR 54](https://github.com/pytroll/pytroll-collectors/pull/54) - Fix times from posttroll messages when file duration is needed
* [PR 50](https://github.com/pytroll/pytroll-collectors/pull/50) - Floor minutes only if configured
* [PR 49](https://github.com/pytroll/pytroll-collectors/pull/49) - Fix bug when naming publisher with config_item
* [PR 42](https://github.com/pytroll/pytroll-collectors/pull/42) - Fix tests

#### Features added

* [PR 58](https://github.com/pytroll/pytroll-collectors/pull/58) - Discard messages without correct type in gatherer
* [PR 57](https://github.com/pytroll/pytroll-collectors/pull/57) - Add more config options to cat.py
* [PR 55](https://github.com/pytroll/pytroll-collectors/pull/55) - Add publish port and nameserver options to scisys receiver
* [PR 54](https://github.com/pytroll/pytroll-collectors/pull/54) - Fix times from posttroll messages when file duration is needed
* [PR 51](https://github.com/pytroll/pytroll-collectors/pull/51) - Add providing server, nameservers and services as parameters to gatherer and cat
* [PR 47](https://github.com/pytroll/pytroll-collectors/pull/47) - Make it possible to group segments for full minutes
* [PR 46](https://github.com/pytroll/pytroll-collectors/pull/46) - Added unique service name where possible.
* [PR 45](https://github.com/pytroll/pytroll-collectors/pull/45) - Add commandline options for publisher port and publisher nameservers
* [PR 43](https://github.com/pytroll/pytroll-collectors/pull/43) - Fix style
* [PR 41](https://github.com/pytroll/pytroll-collectors/pull/41) - Add filter hour pattern to segment gatherer
* [PR 31](https://github.com/pytroll/pytroll-collectors/pull/31) - New options to scisys receiver

In this release 18 pull requests were closed.
