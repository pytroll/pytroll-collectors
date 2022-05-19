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
