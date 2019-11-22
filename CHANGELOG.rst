Changelog
=========


v0.8.4 (2019-07-03)
-------------------
- Update changelog. [Martin Raspaud]
- Bump version: 0.8.3 → 0.8.4. [Martin Raspaud]
- Merge pull request #36 from mraspaud/fix-posttroll-times. [Martin
  Raspaud]

  Fix times comming from posttroll messages.
- Add missing import. [Martin Raspaud]
- Fix times comming from posttroll messages. [Martin Raspaud]
- Merge pull request #35 from pytroll/add-stickler-config. [Martin
  Raspaud]

  Adding .stickler.yml configuration file
- Adding .stickler.yml. [stickler-ci]
- Merge pull request #34 from hundahl/fix-for-python3. [Martin Raspaud]

  Change write mode from 'w' to 'wb' for tmp files
- Change write mode from 'w' to 'wb' for tmp files. [Camilla Hundahl
  Johnsen]
- Bugfix segment gatherer when files have no segments nor channels (#29)
  [Martin Raspaud]

  Bugfix segment gatherer when files have no segments nor channels
- Update unit tests. [Panu Lahtinen]
- Add globified filename to fname set when there are no
  segments/channels in pattern. [Panu Lahtinen]


v0.8.3 (2019-04-24)
-------------------
- Update changelog. [Martin Raspaud]
- Bump version: 0.8.2 → 0.8.3. [Martin Raspaud]
- Fix starttime of collection if first segment recieved isn't the
  earliest (#32) [Martin Raspaud]

  Fix starttime of collection if first segment recieved isn't the earliest
- Fix starttime of collection if first segment recieved isn't the
  earliest. [Martin Raspaud]


v0.8.2 (2019-02-04)
-------------------
- Update changelog. [Panu Lahtinen]
- Bump version: 0.8.1 → 0.8.2. [Panu Lahtinen]
- Merge pull request #28 from pnuu/bugfix-segments-empty-fileset. [Panu
  Lahtinen]

  Bugfix segment gatherer empty fileset
- Add tests for INI config on dataset without critical files. [Panu
  Lahtinen]
- Fix unittests. [Panu Lahtinen]
- Fix mock import for Python 3. [Panu Lahtinen]
- Add a INI config for data having no critical segments. [Panu Lahtinen]
- Add a check that empty channel/segment combination isn't added. [Panu
  Lahtinen]
- Merge pull request #27 from pytroll/feature-skip-empty-slots. [Panu
  Lahtinen]

  Do not publish empty slots from segment gatherer
- Do not publish empty slots from segment gathere. [Martin Raspaud]
- Merge pull request #26 from yufeizhu600/master. [Panu Lahtinen]

  add on_moved process to watchdog trigger.
- Fix the _process calling issue. [Yufei Zhu]
- Put common processing part of watchdog trigger into a '_process'
  method. [Yufei Zhu]
- Add on_moved process to watchdog processor. [Yufei Zhu]
- Merge pull request #25 from mraspaud/fix-multifile-messages. [Panu
  Lahtinen]

  Ignore inconvenient messages in segment gatherer
- Ignore inconvenient messages in segment gatherer. [Martin Raspaud]


v0.8.1 (2018-11-08)
-------------------
- Update changelog. [Panu Lahtinen]
- Bump version: 0.8.0 → 0.8.1. [Panu Lahtinen]
- Merge pull request #24 from pytroll/bugfix-Pass-single-sensor. [Panu
  Lahtinen]

  Pass only one instrument to overpass calculations
- Fix a typo: use attribute instead of variable. [Panu Lahtinen]
- Ensure only one instrument is used in Pass() [Panu Lahtinen]
- Merge pull request #23 from pytroll/bugfix-sensors-in-a-list. [Panu
  Lahtinen]

  Pass only one sensor name to overpass calculations
- Remove duplicate format tags from filename patterns. [Panu Lahtinen]
- Ensure that only one instrument is passed to Pass() calculations.
  [Panu Lahtinen]
- Merge pull request #22 from pytroll/feature-faster-image-scaler. [Panu
  Lahtinen]

  Make scale_image faster
- Clarify log message. [Panu Lahtinen]
- Remove use of .get_data(), replace old with new if mode/shape doesn't
  match. [Panu Lahtinen]


v0.8.0 (2018-10-23)
-------------------
- Update changelog. [Panu Lahtinen]
- Bump version: 0.7.0 → 0.8.0. [Panu Lahtinen]
- Merge pull request #21 from pytroll/bugfix-pattern-time-name. [Panu
  Lahtinen]

  Fix adjust_pattern_time_name to use public trollsift functions
- Fix adjust_pattern_time_name to use public trollsift functions. [David
  Hoese]
- Merge pull request #19 from pytroll/scale-images-overviews. [Panu
  Lahtinen]

  Add overview support to scale_images
- Fix save options, add overview settings. [Panu Lahtinen]
- Set PPP_CONFIG_DIR. [Panu Lahtinen]
- Use areas.yaml instead of areas.def. [Panu Lahtinen]
- Merge pull request #20 from pytroll/feature-satpy-independency. [Panu
  Lahtinen]

  Feature satpy independency
- Bug fix: Use ConfigParser instead of RawConfigParser. [Adam.Dybbroe]
- Add log info. [Adam.Dybbroe]
- Make satpy optional. [Adam.Dybbroe]
- Fix test correct results. [Panu Lahtinen]
- Fix handling of missing/empty segment string. [Panu Lahtinen]
- Merge branch 'master' of github.com:pytroll/pytroll-collectors.
  [Adam.Dybbroe]
- Ensure `sensors` is a list. [Panu Lahtinen]
- Fix sensor name collection. [Panu Lahtinen]
- Remove obsolete keyword argument. [Panu Lahtinen]
- Merge pull request #18 from pytroll/bugfix-segment_gatherer. [Panu
  Lahtinen]

  Fix checks for slot readiness in segment gatherer
- Fix slot readines, fix reading variable tags from config. [Panu
  Lahtinen]
- Merge pull request #17 from pytroll/bugfix-fill_value. [Panu Lahtinen]

  Fix fill value handling in image scaler
- Adjust image mode to match overlay, if necessary. [Panu Lahtinen]
- Add fill_value to save options, fix value used to check masking. [Panu
  Lahtinen]
- Update image scaler unit tests. [Panu Lahtinen]
- Fix copy-paste typo, use single-value fill_value. [Panu Lahtinen]
- Fix testing for NoneType. [Panu Lahtinen]
- Fix typo in variable name. [Panu Lahtinen]
- Get the datatype min/max from the input image. [Panu Lahtinen]
- Fix fill value handling. [Panu Lahtinen]
- Change default fill_value to None, read a single fill_value from
  config. [Panu Lahtinen]
- Merge pull request #16 from TAlonglong/develop. [Panu Lahtinen]

  Add pyinotify watch to directories created under current watched directory
- Merge branch 'master-fork' into develop-fork. [Trygve Aspenes]
- Possible to more than one directory separated by comma. [Trygve
  Aspenes]
- Added feature inotify watch new directories. [Trygve Aspenes]
- Try catch exception when there is an OSError. [Adam.Dybbroe]


v0.7.0 (2018-08-23)
-------------------

Fix
~~~
- Bugfix: The destination server should go in the message and not the
  host. [Adam.Dybbroe]

Other
~~~~~
- Update changelog. [Panu Lahtinen]
- Bump version: 0.6.0 → 0.7.0. [Panu Lahtinen]
- Merge pull request #15 from pytroll/develop. [Panu Lahtinen]

  Add Python 3 support
- Merge pull request #13 from pytroll/feature-python3-support. [Panu
  Lahtinen]

  Add Python 3 support
- Get the items explicitly as a list so that the checks work. [Panu
  Lahtinen]
- Fix handling of empty item strings. [Panu Lahtinen]
- Use RawConfigParser instead of ConfigParser. [Panu Lahtinen]
- Handle empty item strings, use six to import config parser. [Panu
  Lahtinen]
- Fix import of queue.Empty. [Panu Lahtinen]
- Fix dictionary usage in iteration and indexing. [Panu Lahtinen]
- Fix ConfigParser to match imported RawConfigParser. [Panu Lahtinen]
- Handle iterators properly in both Py2 and Py3. [Panu Lahtinen]
- Add EPSG:4326 projection. [Panu Lahtinen]
- Use SatPy and Trollimage for I/O. [Panu Lahtinen]
- Fix imports, use RawConfigParser. [Panu Lahtinen]
- Move WorldCompositeDaemon to own test class. [Panu Lahtinen]
- Remove unused arguments. [Panu Lahtinen]
- Remove blending, update tests. [Panu Lahtinen]
- Use size attributes, fix exceptions, fix area attribute handling, fix
  blending. [Panu Lahtinen]
- Remove unused argument from read_image() and _get_existing_image()
  [Panu Lahtinen]
- Add checks that listener and publisher are there before stopping them.
  [Panu Lahtinen]
- Use satpy Scene to read and save images, refactor everything. [Panu
  Lahtinen]
- Remove extra space. [Panu Lahtinen]
- Use six and reorder imports. [Panu Lahtinen]
- Use get_area_def from satpy instead of mpop. [Panu Lahtinen]
- Fix lambda syntax to support Python 3. [Panu Lahtinen]
- Use urllib.parse for Python 3. [Panu Lahtinen]
- Import from configparser for Python 3, and from ConfigParser for
  Python 2. [Panu Lahtinen]
- Replace print statements with print() [Panu Lahtinen]
- Add sensors to collection metadata. [Panu Lahtinen]
- Fix filename patterns for MSG HRIT files. [Panu Lahtinen]
- Merge pull request #11 from pytroll/feature_multiple_patterns. [Panu
  Lahtinen]

  Add support to multiple filename patterns for segment gatherer
- Merge branch 'feature_multiple_patterns' of https://github.com/pytroll
  /pytroll-collectors into feature_multiple_patterns. [Panu Lahtinen]
- Fix key for delayed files from string 'uid' to variable uid. [Panu
  Lahtinen]
- Add more comments to segment gatherer example configs. [Panu Lahtinen]
- Add tests for using .ini config file. [Panu Lahtinen]
- Add PRO and EPI to all_files and wanted_files. [Panu Lahtinen]
- Add more unittests. [Panu Lahtinen]
- Restructure code for easier testing. [Panu Lahtinen]
- Fix patterns, add variable tags. [Panu Lahtinen]
- Add test config for two non-segmented filesets. [Panu Lahtinen]
- Set orig_platform_name as variable tag. [Panu Lahtinen]
- Add all one and two dataset combinations to
  test_get_collection_status() [Panu Lahtinen]
- Clarify if-elif-else structure. [Panu Lahtinen]
- Fix behaviour when noncritical set is the only set. [Panu Lahtinen]
- Add check for SLOT_NOT_READY in case when other sets are ready. [Panu
  Lahtinen]
- Fix behaviour when slot is ready but wanted files are missing. [Panu
  Lahtinen]
- Add unittests for segments.py. [Panu Lahtinen]
- Handle completed slot correctly when timeout has occured. [Panu
  Lahtinen]
- Handle missing itm_str inside _compose_filenames() [Panu Lahtinen]
- Add example config for collecting HRPT and PPS files. [Panu Lahtinen]
- Fix handling missing wanted/all segment option. [Panu Lahtinen]
- Fix typo: config -> self._config. [Panu Lahtinen]
- Add absolute path when reading configs in unittests. [Panu Lahtinen]
- Add unittests for __init__ [Panu Lahtinen]
- Add test configs for segment gatherer unit tests. [Panu Lahtinen]
- Add unit test file for segment gatherer. [Panu Lahtinen]
- Move YAML config reading to helper_functions.py. [Panu Lahtinen]
- Set publish topic in setup_messaging() [Panu Lahtinen]
- Move messaging init to a method, move pub/sub to class instances.
  [Panu Lahtinen]
- Delete obsolete main() [Panu Lahtinen]
- Move config.ini reading from main() to segments.ini_to_dict() [Panu
  Lahtinen]
- Move ini_to_dict from main script to segments.py. [Panu Lahtinen]
- Add example config for collecting multiple sets of files. [Panu
  Lahtinen]
- Restructure for multiple collectable sets / filename patterns. [Panu
  Lahtinen]
- Change "required" to "is_critical_set" [Panu Lahtinen]
- Stop testing parsers if correct is found. [Panu Lahtinen]
- Add comments, comment out optional settings. [Panu Lahtinen]
- Add example YAML config for MSG/0deg segment gatherer. [Panu Lahtinen]
- Use start_time instead of nominal_time in filename patterns. [Panu
  Lahtinen]
- Fix examples for MSG/0deg and MSG/RSS segments. [Panu Lahtinen]
- Set nameservers to None by default. [Panu Lahtinen]
- Fix YAML config loading. [Panu Lahtinen]
- Add separate config reading for .ini and .yaml files. [Panu Lahtinen]
- Split segment gatherer to separate main and library files. [Panu
  Lahtinen]
- Rename original segment_gatherer.py. [Panu Lahtinen]
- Fix key for delayed files from string 'uid' to variable uid. [Panu
  Lahtinen]
- Add more comments to segment gatherer example configs. [Panu Lahtinen]
- Add tests for using .ini config file. [Panu Lahtinen]
- Add PRO and EPI to all_files and wanted_files. [Panu Lahtinen]
- Add more unittests. [Panu Lahtinen]
- Restructure code for easier testing. [Panu Lahtinen]
- Fix patterns, add variable tags. [Panu Lahtinen]
- Add test config for two non-segmented filesets. [Panu Lahtinen]
- Set orig_platform_name as variable tag. [Panu Lahtinen]
- Add all one and two dataset combinations to
  test_get_collection_status() [Panu Lahtinen]
- Clarify if-elif-else structure. [Panu Lahtinen]
- Fix behaviour when noncritical set is the only set. [Panu Lahtinen]
- Add check for SLOT_NOT_READY in case when other sets are ready. [Panu
  Lahtinen]
- Fix behaviour when slot is ready but wanted files are missing. [Panu
  Lahtinen]
- Add unittests for segments.py. [Panu Lahtinen]
- Handle completed slot correctly when timeout has occured. [Panu
  Lahtinen]
- Handle missing itm_str inside _compose_filenames() [Panu Lahtinen]
- Add example config for collecting HRPT and PPS files. [Panu Lahtinen]
- Fix handling missing wanted/all segment option. [Panu Lahtinen]
- Fix typo: config -> self._config. [Panu Lahtinen]
- Add absolute path when reading configs in unittests. [Panu Lahtinen]
- Add unittests for __init__ [Panu Lahtinen]
- Add test configs for segment gatherer unit tests. [Panu Lahtinen]
- Add unit test file for segment gatherer. [Panu Lahtinen]
- Move YAML config reading to helper_functions.py. [Panu Lahtinen]
- Set publish topic in setup_messaging() [Panu Lahtinen]
- Move messaging init to a method, move pub/sub to class instances.
  [Panu Lahtinen]
- Delete obsolete main() [Panu Lahtinen]
- Move config.ini reading from main() to segments.ini_to_dict() [Panu
  Lahtinen]
- Move ini_to_dict from main script to segments.py. [Panu Lahtinen]
- Add example config for collecting multiple sets of files. [Panu
  Lahtinen]
- Restructure for multiple collectable sets / filename patterns. [Panu
  Lahtinen]
- Change "required" to "is_critical_set" [Panu Lahtinen]
- Stop testing parsers if correct is found. [Panu Lahtinen]
- Add comments, comment out optional settings. [Panu Lahtinen]
- Add example YAML config for MSG/0deg segment gatherer. [Panu Lahtinen]
- Use start_time instead of nominal_time in filename patterns. [Panu
  Lahtinen]
- Fix examples for MSG/0deg and MSG/RSS segments. [Panu Lahtinen]
- Set nameservers to None by default. [Panu Lahtinen]
- Fix YAML config loading. [Panu Lahtinen]
- Add separate config reading for .ini and .yaml files. [Panu Lahtinen]
- Split segment gatherer to separate main and library files. [Panu
  Lahtinen]
- Rename original segment_gatherer.py. [Panu Lahtinen]
- Merge pull request #10 from TAlonglong/feature-publish-message-at-
  each-update. [Panu Lahtinen]

  Added feature to publish the accumulated message after each new segme…
- Merge branch 'develop' into feature-publish-message-at-each-update.
  [Panu Lahtinen]
- Make sure that listener and publisher are stopped even after crash.
  [Panu Lahtinen]
- Move listener and publisher stop() commands outside the loop. [Panu
  Lahtinen]
- Stop also publisher at KeyboardInterrupt. [Panu Lahtinen]
- Add GDAL as optional reader (needed for LA mode images) [Panu
  Lahtinen]
- Rename GOES-R to GOES-16. [Panu Lahtinen]
- Update result images. [Panu Lahtinen]
- Prepare to check also LA mode images, check that image shapes match.
  [Panu Lahtinen]
- Make it possible to use both RGBA and LA mode images. [Panu Lahtinen]
- Merge branch 'develop' of github.com:pytroll/pytroll-collectors into
  develop. [Adam.Dybbroe]
- Merge branch 'develop' of https://github.com/pytroll/pytroll-
  collectors into develop. [Panu Lahtinen]
- Try to import get_area_def from satpy.resample, fallback to mpop if
  not found. [Panu Lahtinen]
- Fix missing orbit number in NOAA-20 messages. [Adam.Dybbroe]
- Remove redundant check for server. [Adam.Dybbroe]
- Bugfix, remove pdb! [Adam.Dybbroe]
- Bugfix JPSS-1, and handle inconsistent url's from new 2met.
  [Adam.Dybbroe]
- Bugfix - messages from new 2met on Merlin. [Adam.Dybbroe]
- Allow url with the ip-adress in addition to host name. [Adam.Dybbroe]
- Add support for the new scisys dispatching messages. [Martin Raspaud]
- Need to check if last file was added to the area. Else message where
  sent each time a new file arrived even if the area was not updated.
  [Trygve Aspenes]
- Added handeling of service and providing server. [Trygve Aspenes]
- Added feature to publish the accumulated message after each new
  segment is received. Eg when collecting EARS ascat bufr data. [Trygve
  Aspenes]
- Merge pull request #9 from pytroll/develop. [Panu Lahtinen]

  Merge develop to master


v0.6.0 (2017-12-08)
-------------------

Fix
~~~
- Bugfix: yaml config reading. [Adam.Dybbroe]

Other
~~~~~
- Update changelog. [Panu Lahtinen]
- Bump version: 0.5.1 → 0.6.0. [Panu Lahtinen]
- Merge pull request #8 from TAlonglong/develop. [Panu Lahtinen]

  Develop
- Bin/geo_gatherer.py possible to configure services used with
  ListenerContainer. Possible config of providing_server, skipping all
  messages not from the providing-server. [Trygve Aspenes]
- Bin/cat.py possible to configure service used with Subscribe. [Trygve
  Aspenes]
- Merge remote-tracking branch 'upstream/develop' into develop. [Trygve
  Aspenes]
- Pytroll_collectors/trigger.py propagate nameserver into the system.
  [Trygve Aspenes]
- Bin/gatherer.py handling nameserver. [Trygve Aspenes]
- Pytroll_collectors/region_collector.py if tle_platform_name in
  metadata use this as platform name. Need to introduce this to handle
  when TLE platform name differs form given platform name. [Trygve
  Aspenes]
- Pytroll_collectors/region_collector.py adding end_time based on
  start_time and duration of not given. [Trygve Aspenes]
- Merge pull request #7 from pytroll/feature-listener-port. [Panu
  Lahtinen]

  Expose listener addresses to segment gatherer
- Set PPP_CONFIG_DIR in setup.py, not in unit test. [Panu Lahtinen]
- Catch NoOptionError for area definition, as it's not required for PIL
  images. [Panu Lahtinen]
- Fix areaname. [Panu Lahtinen]
- Add areas.def and mpop.cfg files. [Panu Lahtinen]
- Set PPP_CONFIG_DIR. [Panu Lahtinen]
- Use mpop built-in area. [Panu Lahtinen]
- Fix tests: add milliseconds to start and end times. [Panu Lahtinen]
- Make excluded_satellite_list kwarg. [Panu Lahtinen]
- Fix typo in error message. [Panu Lahtinen]
- Comment out extra listener addresses. [Panu Lahtinen]
- Add required "tcp://"s to example config. [Panu Lahtinen]
- Revert renaming: "nameserver" back to "nameservers" [Panu Lahtinen]
- Expose listener addresses, publish_port and rename nameservers to
  nameserver. [Panu Lahtinen]
- Bugfix, take care of tenths of seconds in RDR filename. [Adam.Dybbroe]
- Bugfix list of excluded satellites, default is an empty list.
  [Adam.Dybbroe]
- Bugfix. [Adam.Dybbroe]
- Add debug message. [Adam.Dybbroe]
- Prepare for JPSS-1 and add platform exlude list. [Adam.Dybbroe]
- Bugfix. [Adam.Dybbroe]
- Change to use yaml configuration file, and add post-hook (e.g for
  nagios monitoring) [Adam.Dybbroe]
- Copy file fisrt to a temporary filename, then move it to the correct
  name on the same directory. [Adam.Dybbroe]
- More debug info. [Adam.Dybbroe]
- Add zipcollector runner. [Adam.Dybbroe]
- Check that next_img is not None before trying to use it. [Panu
  Lahtinen]
- Retry reading image once after 5 s wait. [Panu Lahtinen]
- Merge pull request #6 from pytroll/feature-wrapping-crop. [Panu
  Lahtinen]

  Add cropping for areas wrapping around from right edge to left edge
- Add cropping for areas wrapping around from right edge to left edge.
  [Panu Lahtinen]
- Merge pull request #5 from pytroll/feature_no_memory_cache. [Panu
  Lahtinen]

  Do not cache overlays in memory
- Do not cache overlays in memory. [Panu Lahtinen]
- Merge pull request #4 from pytroll/feature_publishercontainer. [Panu
  Lahtinen]

  Feature publisher
- Add published message to log when file is written. [Panu Lahtinen]
- Use NoisyPublisher directly. [Panu Lahtinen]
- Stop compositor daemon. [Panu Lahtinen]
- Add new message settings. [Panu Lahtinen]
- Remove obsolete and unused file. [Panu Lahtinen]
- Use posttroll.publish.PublisherContainer for sending messages. [Panu
  Lahtinen]
- Fix comparison of parsed filename parts. [Panu Lahtinen]

  The comparison was made incorrectly against the class attribute, not
  local variable



v0.5.1 (2017-04-06)
-------------------
- Update changelog. [Panu Lahtinen]
- Bump version: 0.5.0 → 0.5.1. [Panu Lahtinen]
- Add Python3 configparser, try to get log config from a file. [Panu
  Lahtinen]
- Handle "ValueError: corrupted page" when reading TIFF images. [Panu
  Lahtinen]


v0.5.0 (2017-03-22)
-------------------
- Update changelog. [Panu Lahtinen]
- Bump version: 0.4.0 → 0.5.0. [Panu Lahtinen]
- Fix crop tuple. [Panu Lahtinen]
- Fix cropping. [Panu Lahtinen]
- Add UID and URI to sent message. [Panu Lahtinen]
- Compose topic. [Panu Lahtinen]
- Log sent message. [Panu Lahtinen]
- Fix format. [Panu Lahtinen]
- PEP8. [Panu Lahtinen]
- Fix publisher name. [Panu Lahtinen]
- Add message settings. [Panu Lahtinen]
- Add message sending for saved global mosaics. [Panu Lahtinen]
- Add function for sending messages. [Panu Lahtinen]
- Merge branch 'develop' of https://github.com/pytroll/pytroll-
  collectors into develop. [Panu Lahtinen]
- Merge pull request #3 from pytroll/feature-check-local-files. [Panu
  Lahtinen]

  Check presence of local files when a new slot is initialised in segment-gatherer
- Update timeout when checking segments on disk. [Martin Raspaud]
- Fix setup.cfg to require python-pillow. [Martin Raspaud]
- Add checking for locally received files in segment gatherer. [Martin
  Raspaud]
- Move crop area adjustment inside None check. [Panu Lahtinen]
- Delete image object after it is not used anymore. [Panu Lahtinen]
- Add option for garbage collection to config example. [Panu Lahtinen]
- Add optional garbage collection to image scaler. [Panu Lahtinen]
- Pass logger to create_world_composite and add log messages. [Panu
  Lahtinen]
- Clarify log messages. [Panu Lahtinen]
- Fix logger call. [Panu Lahtinen]
- Force garbage collection after each handled message. [Panu Lahtinen]


v0.4.0 (2017-03-15)
-------------------
- Update changelog. [Panu Lahtinen]
- Bump version: 0.3.0 → 0.4.0. [Panu Lahtinen]
- Use self.time_name when checking for existing files. [Panu Lahtinen]
- Add log message with check pattern. [Panu Lahtinen]
- Set composite as wildcard when searching for existing files. [Panu
  Lahtinen]
- PEP8 logging syntax. [Panu Lahtinen]
- Handle missing 'tags' [Panu Lahtinen]
- Add return which was missing. [Panu Lahtinen]
- Convert read TIFF image to PIL image object. [Panu Lahtinen]
- Use mpop.imageo.formats.tifffile.imread() to read TIFF images. [Panu
  Lahtinen]
- Move call to _tidy_platform_name() to correct place. [Panu Lahtinen]
- Add missing parameter name. [Panu Lahtinen]
- Set execute bit. [Panu Lahtinen]
- Merge pull request #2 from pytroll/feature_scale_image. [Panu
  Lahtinen]

  Feature scale image
- Fix updating existing image, split parts to new functions. [Panu
  Lahtinen]

  - use the mode of the new image
  - fix masking
  - use fill_value to create values for new alpha channel for the old image
    if mode changes from L to LA or from L or RGB to RGBA
  - add enough channels if the existing image had fewer than the new one
  - remove "obsolete" channels if channel number is reduced

- Test all combinations of updating L, LA, RGB and RGBA images. [Panu
  Lahtinen]
- Fix updating existing image. [Panu Lahtinen]

  - use the mode of the new image
  - fix masking
  - add enough channels if the existing image had fewer than the new one
  - use fill_value to create values for new alpha channel if mode changes
    from L to LA or from L or RGB to RGBA

- Enhance test_update_existing_image. [Panu Lahtinen]
- Fix and adjust many things. [Panu Lahtinen]

  - fix image updating
  - change static_image_fname to static_image_fname_pattern
  - use tuples as fill_value
  - fix filename composing
  - fix reading fill_value, compression and blocksize from config

- Use assertIsNone(), fix fill_values. [Panu Lahtinen]
- Adjust static image option name, fix default of fill_value. [Panu
  Lahtinen]
- Add checks for area definition availability. [Panu Lahtinen]
- Add new config options. [Panu Lahtinen]
- Fix time_name handling, expose more settings, fix saving. [Panu
  Lahtinen]

   - add save settings
   - find the correct name for the "nominal time"
   - adjust in_pattern and out_pattern to use the same time_name as incoming
     message
   - use save_image instead of direct img.save()

- Add check for GSHHS_DATA_ROOT environment variable. [Panu Lahtinen]
- Add pycoast as requirement. [Panu Lahtinen]
- Install pycoast. [Panu Lahtinen]
- Remove obsolete config, add TODO. [Panu Lahtinen]
- Install Pillow and trollsift. [Panu Lahtinen]
- Add example config for scale_images.py. [Panu Lahtinen]
- Copy image before modifying, catch AttributeError when reading fonts.
  [Panu Lahtinen]
- Fix looping in save_images, don't join out_dir and out_pattern when
  reading config, fix _check_existing() [Panu Lahtinen]
- Fix filenames, parse from basename, more tests for ImageScaler class.
  [Panu Lahtinen]
- Import test_image_scaler. [Panu Lahtinen]
- Fix out_patterns. [Panu Lahtinen]
- Add an empty image for testing ImageScaler. [Panu Lahtinen]
- Change file patterns and areaname. [Panu Lahtinen]
- Require postroll 1.3.0 or later. [Panu Lahtinen]
- Fix existing_fname_parts, fix listener queue name, fix in_pattern.
  [Panu Lahtinen]
- Add more test requirements. [Panu Lahtinen]
- Add tests for ImageScaler class. [Panu Lahtinen]
- Add another section for testing crops/sizes/tags. [Panu Lahtinen]
- Add better filename patterns. [Panu Lahtinen]
- Remove unnecessary self.subject arguments, adjust raised errors. [Panu
  Lahtinen]
- Move public methods before privates. [Panu Lahtinen]
- Add section for testing ImageScaler class. [Panu Lahtinen]
- Fix _get_bool(), fix config item names. [Panu Lahtinen]
- Add a function to get config value with a default value as backup.
  [Panu Lahtinen]
- Remove unused comment. [Panu Lahtinen]
- Update TODO. [Panu Lahtinen]
- Change text_location to differenve value as default. [Panu Lahtinen]
- Read defaults from the module. [Panu Lahtinen]
- Change values so that they are not the same as defaults. [Panu
  Lahtinen]
- Continue refactoring. [Panu Lahtinen]

  - add default values for config items in a dict
  - remove many try-excepts
  - handle mandatory config items in a method
  - move parsing of crops, sizes and tags to methods
  - rename "use_platform_name_hack" to "tidy_platform_name" and make it a method
  - use default dictionary for _get_text_settings()
  - replace config.getint() with int(config.get())
  - replace config.getbool() with own method

- Fix "font" to "font_name" [Panu Lahtinen]
- Add tests for read_image() and update_existing_image() [Panu Lahtinen]
- Use copies of the images, add tests for add_image_as_overlay. [Panu
  Lahtinen]
- Check overlay validitu, raise ValueError for invalid, handle error.
  [Panu Lahtinen]
- Add tests for adjust_img_mode_for_text(), add placeholder tests for
  untested functions. [Panu Lahtinen]
- Fix test name, fix correct value. [Panu Lahtinen]
- Add more tests for different text/bg color settings. [Panu Lahtinen]
- Shorten lines. [Panu Lahtinen]
- Convert to RGB(A) only if text color dictates so. [Panu Lahtinen]
- Remove unnecessary if-elses, as bg_extra_width defaults to 0 not None.
  [Panu Lahtinen]
- Add tests for text and background color box locations. [Panu Lahtinen]
- Move text location calculation to separate functions. [Panu Lahtinen]
- Add test for _is_rgb_color. [Panu Lahtinen]
- Add a function to convert image mode suitable for the text. [Panu
  Lahtinen]
- Add test for _get_font() [Panu Lahtinen]
- Make a function to get font. [Panu Lahtinen]
- Add config parser and tests for _get_text_settings() and _add_text()
  [Panu Lahtinen]
- Change default value from None to 0. [Panu Lahtinen]
- Add config file with text related test settings. [Panu Lahtinen]
- Add static font that can be used in tests. [Panu Lahtinen]
- Move saving of staticly named images to a function. [Panu Lahtinen]
- Rename latest_composite_image to static_image_fname. [Panu Lahtinen]
- Remove exception handling, add filename as kwarg to
  self._update_existing_img() [Panu Lahtinen]
- Add text based on the image type, use single save command. [Panu
  Lahtinen]
- Add interface funtion self._add_text() to add_text() [Panu Lahtinen]
- Move updating of existing image to self._update_existing_img() [Panu
  Lahtinen]
- Add unit tests for resize_image() [Panu Lahtinen]
- Move image resizing to a separate function. [Panu Lahtinen]
- Add tests for crop_image() [Panu Lahtinen]
- Check crop limits, fix name of the returned image. [Panu Lahtinen]
- Move image crop to a separate function. [Panu Lahtinen]
- Add tests for save_image() [Panu Lahtinen]
- Convert only to GeoImage if adef and time_slot are given. [Panu
  Lahtinen]
- Add first unittests for those functions that are more or less
  finalized. [Panu Lahtinen]
- Refactor. [Panu Lahtinen]

  - move config items to class attributes
  - split run() to several smaller functions

- Move shape file environment variable to image_scaler.py. [Panu
  Lahtinen]
- Add tests for image_scaler. [Panu Lahtinen]
- Move functionality to pytroll_collectors.image_scaler. [Panu Lahtinen]
- Initial commit for the library part of scale_images. [Panu Lahtinen]
- Initial version of image scaler. [Panu Lahtinen]
- Merge pull request #1 from TAlonglong/develop. [Panu Lahtinen]

  bin/cat.py if publish_topic is given in config, replace topic
- Bin/segment_gatherer.py remove diff newline. [Trygve Aspenes]
- Bin/segment_gatherer.py Go back to similar handleing as original.
  [Trygve Aspenes]
- Bin/segment_gatherer.py dont need the msg handeling here as it is
  already done. [Trygve Aspenes]
- Fixed conflict. [Trygve Aspenes]
- Bin/segment_gatherer.py fixed _init... and process to avoid
  overwriting end_time with data parsed anew from filename. [Trygve
  Aspenes]
- Bin/cat.py if publish_topic is given in config, replace topic. [Trygve
  Aspenes]


v0.3.0 (2017-01-18)
-------------------
- Update changelog. [Panu Lahtinen]
- Bump version: 0.2.0 → 0.3.0. [Panu Lahtinen]
- Merge branch 'develop' of https://github.com/pytroll/pytroll-
  collectors into develop. [Panu Lahtinen]
- Make sure that end_time > start_time. [Martin Raspaud]
- Take preference on data in message over what's parsed from the
  filename. [Panu Lahtinen]

  Fixes eg. end time for EARS/VIIRS data

- Fix syntax error. [Panu Lahtinen]
- Make sure that the sensor names are in an iterable. [Panu Lahtinen]
- Add try-except around deletion of unnecessary tags. [Panu Lahtinen]
- Use full pattern, not EPI specific. [Panu Lahtinen]
- Remove confusing - and unusable - config files. [Panu Lahtinen]
- Add examples for different Metop Level-0 files. [Panu Lahtinen]
- Add example for collecting HRPT L0 files for AAPP. [Panu Lahtinen]
- Add a possibility to have a time range for files belonging to the same
  time slot. [Panu Lahtinen]
- Collect all instruments, not only the one in latest received message.
  [Panu Lahtinen]


v0.2.0 (2016-11-23)
-------------------
- Update changelog. [Panu Lahtinen]
- Bump version: 0.1.1 → 0.2.0. [Panu Lahtinen]
- Add check for listener's output queue name. [Panu Lahtinen]


v0.1.1 (2016-11-16)
-------------------

Fix
~~~
- Bugfix: granule metadata is now copied and not shared amoung
  collectors. [Martin Raspaud]
- Bugfix: process instead of process_message. [Adam.Dybbroe]

Other
~~~~~
- Update changelog. [Panu Lahtinen]
- Bump version: 0.1.0 → 0.1.1. [Panu Lahtinen]
- Fix typo in import. [Panu Lahtinen]
- Fix listener's queue name. [Panu Lahtinen]
- Try to use miniconda. [Panu Lahtinen]
- Adjust scipy pip install command. [Panu Lahtinen]
- Add scipy as test requirement. [Panu Lahtinen]
- Use pip instead of apt-get for installing scipy. [Panu Lahtinen]
- Add pykdtree as install requirement. [Panu Lahtinen]
- Fix URLs. [Panu Lahtinen]
- Try testing with "python setup.py test" instead of coverage. [Panu
  Lahtinen]
- Add watchdog as install and test requirement. [Panu Lahtinen]
- Add required packages also for tests_require list. [Panu Lahtinen]
- Python 2.6 compatibility, autopep8. [Panu Lahtinen]
- Fix coverage command. [Panu Lahtinen]
- Fix coverage command. [Panu Lahtinen]
- Moved from trollduction.producer. [Panu Lahtinen]
- Add a note about helper functions. [Panu Lahtinen]
- Initial commit. [Panu Lahtinen]
- Move/copy from trollduction. [Panu Lahtinen]
- Initial commit. [Panu Lahtinen]
- Initial commit. [Panu Lahtinen]
- Adjust imports to pytroll-collectors. [Panu Lahtinen]
- Copy needed functions from trollduction to remove dependency. [Panu
  Lahtinen]
- Add execute bit. [Panu Lahtinen]
- Remove duplicates. [Panu Lahtinen]
- Merge branch 'feature_reorganize' [Panu Lahtinen]
- Move files to proper places. [Panu Lahtinen]
- Collect files for pytroll-collectors. [Panu Lahtinen]
- Add emacs temp files. [Panu Lahtinen]
- Merge branch 'master' of https://github.com/pytroll/pytroll-
  collectors. [Panu Lahtinen]
- Initial commit. [Panu Lahtinen]
- Pep8. [Adam.Dybbroe]
- Add the min_length config option for catter. [Martin Raspaud]
- Add missing colon. [Panu Lahtinen]
- Prevent "ValueError: max() arg is an empty sequence" for equal sets,
  add more information on logging these occurences. [Panu Lahtinen]
- Fix intendation error. [Panu Lahtinen]
- Add a function that checks swath completeness, clearer log messages.
  [Panu Lahtinen]
- Bug in region collector printout. [Martin Raspaud]
- Change timeout in gatherer when last granule is not arriving last.
  [Martin Raspaud]
- Merge branch 'develop' of https://github.com/mraspaud/trollduction
  into develop. [Panu Lahtinen]

  Conflicts:
  	trollduction/collectors/trigger.py
  	trollduction/producer.py

- Bugfix. publish_topic added as a keyword argument to WatchDogTrigger.
  [Adam.Dybbroe]
- Merge branch 'develop' into my-new-aapp-runner. [Adam.Dybbroe]

  Conflicts:
  	trollduction/collectors/trigger.py
- Bugfix. [Adam.Dybbroe]
- Merge branch 'feature-trollstalker2' into my-new-aapp-runner.
  [Adam.Dybbroe]

  Conflicts:
  	trollduction/collectors/trigger.py

- Make sure that l2processor doesn't hang on crash. [Panu Lahtinen]
- Fixed incorrect function names in PostTrollTrigger. [Panu Lahtinen]
- Merge branch 'feature-trollstalker2' into develop. [Adam.Dybbroe]

  Conflicts:
  	trollduction/collectors/trigger.py
- Merge branch 'develop' into feature-trollstalker2. [Adam.Dybbroe]

  Conflicts:
  	trollduction/collectors/trigger.py
- First iteration of the trollstalker rewrite. [Martin Raspaud]
- Merge branch 'feature_area_msg' into develop. [Panu Lahtinen]

  Conflicts:
  	trollduction/collectors/region_collector.py
  	trollduction/producer.py
  	trollduction/xml_read.py

- Fixes for logging (PEP8) [Panu Lahtinen]
- For inbound messages where type is collection, check if the area
  matches to the configured area. Also some cleanup for PEP8. [Panu
  Lahtinen]
- Making landscape happier. [Panu Lahtinen]
- Config option "publish_topic" for setting custom topic for published
  messages by gatherer. [Panu Lahtinen]
- Replace the corner estimation in region_collector with trollsched's
  routines. [Martin Raspaud]
- Try bug fixing debug printout... [Adam Dybbroe]
- Gatherer: add the possibility to choose which observer is being used.
  [Martin Raspaud]
- Fix multiple Thread inheritance. [Martin Raspaud]
- Mock out entire watchdogtrigger on importerror. [Martin Raspaud]
- Mock watchdog if not present. [Martin Raspaud]
- Catch importerrors when watchdog is imported. [Martin Raspaud]
- Add the collector __init__.py. [Martin Raspaud]
- Move gatherer to bin. [Martin Raspaud]
- Fix gatherer and regioncollector for new metadata and npp granules.
  [Martin Raspaud]
- Add PostTrollTrigger to triggers. [Martin Raspaud]
- Logging and argparsing in catter. [Martin Raspaud]
- Remove hardcoded link to configuration files. [Martin Raspaud]
- Granule handling, first commit. [Martin Raspaud]

  * Region collection is implemented.
  * catter cats the low level data.



