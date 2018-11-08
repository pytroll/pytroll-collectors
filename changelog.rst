Changelog
=========


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
- Merge branch 'feature_multiple_patterns' of
  https://github.com/pytroll/pytroll-collectors into
  feature_multiple_patterns. [Panu Lahtinen]
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



v1.1.0 (2016-11-01)
-------------------
- Update changelog. [Panu Lahtinen]
- Bump version: 1.0.1 → 1.1.0. [Panu Lahtinen]
- Allow Travis to fail with Python 2.6. [Panu Lahtinen]
- Merge branch 'develop' of https://github.com/pytroll/trollduction into
  develop. [Panu Lahtinen]
- Add bump and changelog config files. [Martin Raspaud]
- Add bumversion config. [Panu Lahtinen]
- Fix version string. [Panu Lahtinen]
- Use actual files for scisys testing. [Martin Raspaud]
- Use localhost for network tests on travis. [Martin Raspaud]
- Add unittests for producer's url tools, and fix associated bug.
  [Martin Raspaud]
- Beautify producer.py. [Martin Raspaud]
- Fix scisys receiver tests. [Martin Raspaud]
- Reorganize imports. [Martin Raspaud]
- Allow leading zeros on segment numbers. [Martin Raspaud]
- Merge pull request #20 from pytroll/smhiprod. [Panu Lahtinen]

  changes to aapp and pps running
- Merge branch 'develop' into smhiprod. [Adam.Dybbroe]
- Merge pull request #19 from khunger/feature-muliple-area-elements-
  same-id. [Panu Lahtinen]

  Allow muliple area elements with same id in config
- Unit test for duplicate areas in product config. [Christian Kliche]
- Allow muliple area elements with same id in config. [Christian Kliche]

  To support products within same areas assigned to different
  l2processor instances, it must be possible to use area elements
  with same id but different process_number attribute.

- Fix "test_requires" to "tests_require" [Panu Lahtinen]
- Merge pull request #18 from khunger/feature-write-options. [Panu
  Lahtinen]

  Feature write options
- Product config with subnodes in common section. [Christian Kliche]

  ```
      <common>
  	...
          <!-- Default parameters for the file writers.
              All items listed in <format_params> will be as well forwarded
              to custom writers (like NinJoTiff)
              as a dictionary named "writer_options".
             -->
          <format_params>
              <nbits>8</nbits>
              <fill_value_subst>1</fill_value_subst>
          </format_params>

      </common>
  ```

- Fixed python 2.6 compatibility and formatting. [Christian Kliche]
- Added example for format-options in product config. [Christian Kliche]
- Fixed indention bug. [Christian Kliche]
- Use writer_options dict parameter for saving. [Christian Kliche]

  Uses new functionality as implented in mpop feature-writer-options

- Merge pull request #17 from khunger/feature-create-scene-with-end-
  time. [Panu Lahtinen]

  Use end_time if available for creating scene
- Use end_time if available for creating scene. [Christian Kliche]

  If "end_time" was found in posttroll message (created by trollstalker),
  the tuple (time, end_time) is used to create the scene. This is necessary
  to read all segments of an Himawari8 dataset.

- Do not block reprocessing of same scene if failed in AAPP.
  [Adam.Dybbroe]
- Run pps with date and time arguments for all satellites, not only
  Terra/Aqua. [Adam.Dybbroe]
- Merge branch 'develop' into smhiprod. [Adam.Dybbroe]

  Conflicts:
  	trollduction/producer.py

- Bump up version number. [Adam.Dybbroe]
- Pep8. [Adam.Dybbroe]
- Merge branch 'develop' of github.com:pytroll/trollduction into
  develop. [Adam.Dybbroe]
- Make a copy of object before manipulating it in producer. [Martin
  Raspaud]
- Cleanup producer.py. [Martin Raspaud]
- Fix save retry to pass the same parameters as the first time. [Martin
  Raspaud]
- Adapt to new EUMETCast SST file names with less info. [Adam.Dybbroe]
- In log files print platform and orbit number to idnetify scene.
  [Adam.Dybbroe]

  Passing the job-dict and the key to the worker was needed.

- Merge pull request #16 from khunger/feature-file-format-params. [Panu
  Lahtinen]

  Support for format parameters in file config
- Support for format parameters in file config. [Christian Kliche]

  The DataWriter was modified to support additional parameters for
  the file format specified with the attribute "format" of the file
  node. A new xml node "format_params" has to be inserted after the
  file name.
  Example:
  <file format="mpop.imageo.formats.ninjotiff">
      METEOSAT_EUROPA_GESAMT_RGB-Staub_nqeuro3km_{time:%Y%m%d%H%M}_ninjo.tif
      <format_params>
          <ninjo_product_name>abc</ninjo_product_name>
          ...
          <nbits>16</nbits>
      </format_params>
  </file

- Merge pull request #15 from khunger/feature-composite-with-params.
  [Panu Lahtinen]

  Feature composite with params
- Add example for parametrized composites. [Christian Kliche]
- Support for parametrized composites. [Christian Kliche]

  The product configuration can be modified to allow
  parametrized composites:
  <product id="sample_comp" name="my_sample">
    <composite_params>
      <param1>[0.0, 0.3, 0.0]</param1>
      <paramX>None</paramX>
      ..
    </composite_params>
    <file>sample.tif</file>
  </product>

- Merge pull request #14 from khunger/fix-sourceuri-and-create-dir.
  [Panu Lahtinen]

  Added missing arg. source_uri, ensure dir exists
- Added missing arg. source_uri, ensure dir exists. [Christian Kliche]
- Merge pull request #13 from khunger/feature-l2proc-area-processnum.
  [Panu Lahtinen]

  Share product_config between multiple l2processors
- Share product_config between multiple l2processors. [Christian Kliche]

  Allows to assign certain areas in product_config.xml to parallel running l2processor instances.

  Configuration steps:
  1. Start l2processor with additional argument "-N <PROCNUM>" (PROCNUM should be an int value, i.e. 0, 1,...).
  2. Add attribute "process_num" to <area> elements in product_config.xml to assign l2processor instance to an area that it should process.
  3. If the logger.ini should be shared between l2processor instances, use "%PROCNUM%" in configured log filenames. It will be replaced by the assigned PROCNUM at runtime when l2processor starts.

- Merge pull request #12 from khunger/feature-wait-for-channel. [Panu
  Lahtinen]

  Feature wait for channel
- Added example for "wait_for_channel" [Christian Kliche]
- Waiting for existence of file before loading chan. [Christian Kliche]

  for example:

  [l2processor]
  ...
  wait_for_channel_CloudType = /data/IN/NWCSAF/SAFNWC_MSG3*{time:%Y%m%d%H%M}*|120|10
  ...

  Before loading channel "CloudType", l2processor waits until a file matching the pattern exists. "120" denotes an timeout in seconds after that an error is thrown. "10" means, wait for another 10 seconds when file was found.

- Merge pull request #11 from khunger/feature-dwd-vza. [Panu Lahtinen]

  Added binding for DWD ViewZenithAngleManager
- Added binding for DWD ViewZenithAngleManager. [Christian Kliche]
- Merge pull request #10 from khunger/feature-nameserver-definition.
  [Panu Lahtinen]

  Another fix for handling missing nameservers param
- Another fix for handling missing nameservers param. [Christian Kliche]
- Merge pull request #9 from khunger/feature-nameserver-definition.
  [Panu Lahtinen]

  Nameserver definition for stalker, segment_gatherer + l2processor
- Fixed NoOpt handling for nameservers param. [Christian Kliche]
- Nameserver definition for stalker, seggath + l2pro. [Christian Kliche]

  New parameter in configuration file. i.e.:

  nameservers=localhost

  It defines the nameserver hosts to register publishers of trollstalker, segment_gatherer and l2processorWARNING:
  If nameservers option is set, address broadcasting via multicasting is not used any longer.
  The corresponding nameserver has to be started with command line option "--no-multicast".

- Merge pull request #8 from khunger/feature-trollstalker-temporal-
  align. [Panu Lahtinen]

  Stalker support for custom variables
- Stalker support for custom variables. [Christian Kliche]

  especially for Datetime format spec with temporal alignment

  Support for format specifications like {start_time:%Y%m%d%H%M%S|align(5)}
  to ceil/round a datetime to a multiple of a timedelta.
  Useful to equalize small time differences in name of files belonging to the same timeslot
  The first parameter represents the difference between timeslots in minutes.

  Example config:

  [trollstalker]
  ...
  var_gatherer_time={time:%Y%m%d%H%M|align(15)}
  ...
  This creates a new posttroll message dict entry "gatherer_time" with a datetime object ceiled
  to 15 minutes intervals.

  align(5):
  17:10:58 -> 17:10:00
  17:03:00 -> 17:00:00
  16:59:00 -> 16:55:00

  align(15):
  16:59:00 -> 16:45:00

  When called with two arguments, the second denote a kind of offset subtracted before ceiling (default: 0).

  align(15,-2):
  16:59:00 -> 17:00:00

  align(15,2):
  17:16:00 -> 17:00:00

  When called with three arguments, the specified number of intervals (defined by argument 1) will be added to
  the result.

  align(15,0,1):
  16:59:00 -> 17:00:00

  align(15,0,2):
  16:59:00 -> 17:15:00

  align(15,0,-1):
  16:59:00 -> 16:30:00

- Merge branch 'master' into develop. [Martin Raspaud]
- Merge branch 'develop' [Martin Raspaud]
- Merge pull request #3 from mraspaud/revert-2-zero_coverage. [Panu
  Lahtinen]

  Revert "Zero coverage"
- Revert "Zero coverage" [Panu Lahtinen]
- Merge pull request #2 from mraspaud/zero_coverage. [Panu Lahtinen]

  Merging zero coverage functionality to develop branch
- Bump version to provoke upgrade of buggy 1.0.0 releases at smhi.
  [Adam.Dybbroe]


v1.0.1 (2016-06-18)
-------------------
- Cosmetics only. [Adam.Dybbroe]


v1.0.0 (2016-06-15)
-------------------

Fix
~~~
- Bugfix: log-error message text. [Adam.Dybbroe]
- Bugfix: copy incoming message data. [Adam.Dybbroe]
- Bugfix: typo. [Martin Raspaud]
- Bugfix: check_uri now checks ip or hostname, not netloc.
  [Adam.Dybbroe]
- Bugfix: granule metadata is now copied and not shared amoung
  collectors. [Martin Raspaud]
- Bugfix: don't return from the group loop, just continue if the area is
  irrelevant. [Martin Raspaud]
- Bugfix: process instead of process_message. [Adam.Dybbroe]
- Bugfix: More robust in case where input file is not present.
  [Adam.Dybbroe@smhi.se]
- Bugfix: Fix correct call syntax to AAPP script. [Adam.Dybbroe]
- Bugfix: rename pps_runner package to nwcsafpps_runner.
  [Adam.Dybbroe@smhi.se]
- Bugfix: get_area_def_names is now returning the correct amount of
  areas. [Martin Raspaud]

Other
~~~~~
- Update changelog. [Martin Raspaud]
- Bump version: 0.2.0 → 1.0.0. [Martin Raspaud]
- Use globify instead of compose for more genericity with variable-timed
  files. [Panu Lahtinen]
- Add support to configuring search radius for nearest neighbour
  interpolation. [Panu Lahtinen]
- Add config examples for projection method selection and search radius
  definition for nearest neighbour interpolation. [Panu Lahtinen]
- Remove empty subscripe topics. [Adam.Dybbroe]
- Handle non-satellite scene messages. [Adam.Dybbroe]
- Merge branch 'develop' of github.com:pytroll/trollduction into
  develop. [Adam.Dybbroe]
- Add the (publish) 'port' as a possible option for trollduction.cfg.
  [Martin Raspaud]
- Merge pull request #7 from
  khunger/gatherer_without_hardcoded_segment_digits. [Panu Lahtinen]

  Removed hardcoded 6-digits segment substrings
- Removed hardcoded 6-digits segment substrings. [Christian Kliche]

  Some filenames differ from formerly implemented 6-digit scheme.

  i.e . Himawari8 files are named like IMG_DK01IR1_201604291009_010 (segment "010")

  The configured pattern must be adjusted to handle both cases. For example {segment:0>6s} for 6 digits
  and {segment:0>3s} for 3 digits.

- Avoid using tempfiles when linking. [Martin Raspaud]

  os.link can't work on an existing file.

- Merge pull request #6 from khunger/feature-seggath-premature-publish.
  [Panu Lahtinen]

  Support for "pre-mature" publishing
- Fixed typo. [Christian Kliche]

  Replaced constant name SLOT_OBSOLUTE_TIMEOUT by SLOT_OBSOLETE_TIMEOUT

- Support for pre-mature publishing. [Christian Kliche]

  New configuration parameter num_files_premature_publish to define
  a number of received files after that an event will be published
  although there are still some missing files. After publishing such
  event, the segment gatherer still waits for further file messages
  for this timeslot.

- Close files after saving. [Martin Raspaud]
- Fix the temporary file permissions. [Martin Raspaud]
- Save files through a temporary name first. [Martin Raspaud]
- Bugfix segment_gatherer in case of delayed files. [Martin Raspaud]
- Cleanup trollstalker2. [Martin Raspaud]
- Make trollstalker more robust when end_time is missing. [Martin
  Raspaud]
- Bugfix. [Martin Raspaud]
- Add granule length capability to trollstalker. [Martin Raspaud]

  This way we can specify an end time that was implicit, and remove hardcoded
  ugliness

- Make gatherer crash when the trigger crashes. [Martin Raspaud]

  It happens that the trigger crashes now and then. Unfortunately, the main
  gatherer process won't die in this case, and would just do nothing. This
  patch should address this issue through checking that the triggers are
  alive.

- Avoid crash in xml product-list reading when an env is missing.
  [Martin Raspaud]
- Move publish/subscribe topics and station to config file.
  [Adam.Dybbroe]
- Take care of smaller passes using min_length in cat. [Martin Raspaud]
- Merge branch 'develop' of github.com:pytroll/trollduction into
  develop. [Adam.Dybbroe]
- Add the min_length config option for catter. [Martin Raspaud]
- Handle files that don't match the used pattern. [Panu Lahtinen]
- Fix incorrect python path. [Panu Lahtinen]
- Use metadata parsed from the filename (UID) as basis. [SatMan]
- Fix consistency in orbit number. [Adam.Dybbroe]

  The orbit number in the outgoing message now match the orbit
  number in the RDR (and later SDR) files

- Bugfix, pass on incoming message. [Adam.Dybbroe]
- Fixing bug - transfering message data from listener to publisher.
  [Adam.Dybbroe]
- Fix bug, missing variant field in msg. Carry on message from incoming
  msg. [Adam.Dybbroe]
- Bugfix. [Adam.Dybbroe]
- Bugfix; now reading the passlength_threshold param. [Adam.Dybbroe]
- Don't process very short passes, determined by config param.
  [Adam.Dybbroe]
- A bit more log info on NWP file consistency. [Adam.Dybbroe]
- Merge branch 'develop' of github.com:pytroll/trollduction into
  develop. [Adam.Dybbroe]
- Fix FakeMessage data from str to dict. [Panu Lahtinen]
- Add missing colon. [Panu Lahtinen]
- Prevent "ValueError: max() arg is an empty sequence" for equal sets,
  add more information on logging these occurences. [Panu Lahtinen]
- Merge branch 'develop' of https://github.com/pytroll/trollduction into
  develop. [Panu Lahtinen]
- Take into account filenames with variable fields (eg. production
  time), update example config. [Panu Lahtinen]
- Add a check of the NWP file content. [Adam.Dybbroe]
- Bugfix - filename. [Adam.Dybbroe]
- New sst tif output added. [Adam.Dybbroe]
- Bugfix for sst tiff file on euron1. [Adam.Dybbroe]
- Fix png image. [Adam.Dybbroe]
- Add some more output formats and variants. [Adam.Dybbroe]
- Remove old file info from pps runner messages. [Martin Raspaud]

  When passing over the metadata to new pps runner meesages, old file info
  wasn't removed. This is now fixed through removing collections and datasets
  from the input metadata.

- Make pps runner pass around input metadata. [Martin Raspaud]

  pps_runner would create a message from scratch, thereby leaving out the
  input metadata for later messages. We now copy the metadata over.

- Set time to UTC. [Panu Lahtinen]
- Add segment_collector to installed scripts. [Panu Lahtinen]
- Merge branch 'develop' of https://github.com/pytroll/trollduction into
  develop. [Panu Lahtinen]
- Revert back to 6 pool processes. [Adam.Dybbroe]
- Make it possible to turn on/off destriping via config. [Adam.Dybbroe]
- Lower the amount of pool processes to 4. [Adam.Dybbroe]
- Merge branch 'develop' of github.com:pytroll/trollduction into
  develop. [Adam.Dybbroe]
- Add more deubg info... [Adam.Dybbroe]
- Add example config for segment_gatherer.py. [Panu Lahtinen]
- Add more general gatherer for GEO segments and multifile polar
  granules (VIIRS, EARS-PPS, etc) [Panu Lahtinen]
- Add geo_gatherer to the list of installed scripts. [Panu Lahtinen]
- Fix bug. [Adam.Dybbroe]
- Merge branch 'develop' of github.com:pytroll/trollduction into
  develop. [Adam.Dybbroe]
- Add example how to collect EARS-PPS products together. [Panu Lahtinen]
- Merge branch 'develop' of https://github.com/pytroll/trollduction into
  develop. [Panu Lahtinen]
- If aliases are used, rename original metadata to 'orig_'+key. [Panu
  Lahtinen]
- Chmod +x. [Panu Lahtinen]
- Add destriping step. [Adam.Dybbroe]
- Allow None as a valid return code in geolocation processing.
  [Adam.Dybbroe]
- Use variant=DR. [Adam.Dybbroe]
- Fix to use correct path to default GBAD config file. [Adam.Dybbroe]
- Bugfix. [Adam.Dybbroe]
- Add support for Aqua processing. [Adam.Dybbroe]
- Use startnudge/endnudge from config and accepts returncode = 1 for
  geolocation. [Adam.Dybbroe]
- Fix bug. [Adam.Dybbroe]

  Only the three lvl1b files were send via posttroll,
  now the geo-file is included

- Add more debug info. [Adam.Dybbroe]
- Fix level: 1B instead of L1B. [Adam.Dybbroe]
- Add check if output files exists in working dir before moving them.
  [Adam.Dybbroe]
- Reset eos-files dict after completion/timeout of scene. [Adam.Dybbroe]
- Publish result messages. [Adam.Dybbroe]
- Fix bug in modis-lvl1b call. [Adam.Dybbroe]
- Removes the first and last 15 seconds of the data instead of just 5.
  [Adam.Dybbroe]
- Fix filenames and paths for geolocation and l1b generation.
  [Adam.Dybbroe]
- Fix bug. [Adam.Dybbroe]
- Fix bug. [Adam.Dybbroe]
- Exclude file path of the level-1 result file when calling modis_L1A.
  [Adam.Dybbroe]

  The Seadas modis_L1A doesn't work if you provide the full path

- Fix bug in scene dict and add more processing steps. [Adam.Dybbroe]
- Fix bug in scene dict. [Adam.Dybbroe]
- Add try-except clause around thread. [Adam.Dybbroe]
- Add more debug info. [Adam.Dybbroe]
- Add more debug info to terra processing and add level-1a command.
  [Adam.Dybbroe]
- Fix proper cleaning of job register and add ancillary data
  downloading. [Adam.Dybbroe]
- Fix installation of new seadas-modis runner. [Adam.Dybbroe]
- Add new modis runner using SeaDAS. [Adam.Dybbroe]
- Transfer message metadata thru aapp_runner. [Martin Raspaud]

  AAPP runner was recreating new messages for publishing, thereby dropping
  the incomming messages's metadata. Instead we now initialize the outgoing
  message with the incomming mda, so that the whole mda is available at later
  stages.

- Add params info on save error. [Martin Raspaud]

  when saving crashes, we now print out the params info

- Rename source to variant. [Martin Raspaud]
- Pop 'regions' from metadata. [Martin Raspaud]

  Since last update, 'regions' would be included in the message. This was not
  desireable, so it has now been removed.

- Add source info in scisys receiver. [Martin Raspaud]

  The scisys-receiver is now providing a source info in it's messages.

- Allow gatherer regions for each config item. [Martin Raspaud]

  The regions to gather on were until now defined globally only, in a
  'default' section. By upcasing this to 'DEFAULT', this allows us to use the
  global value as a default, and to have locally defined 'regions'
  parameters.

- Fix the message check in gatherer. [Martin Raspaud]

  Gatherer is checking the resulting message before sending. Until now, the
  uri had to be there. However this is not valid for dataset messages, so
  we check this case now also.

- Fix intendation error. [Panu Lahtinen]
- Add a function that checks swath completeness, clearer log messages.
  [Panu Lahtinen]
- Check metadata for URI, use stdout logging even when logging to file.
  [Panu Lahtinen]
- Prevent ZeroDivisionError, when scenes have start_time = end_time.
  [Adam.Dybbroe]
- Fall back to environment variable if config doesn't have
  pps_statistics_dir. [Adam.Dybbroe]
- Using pps_statistics_dir from pps_config. [Adam.Dybbroe]
- Merge branch 'develop' of github.com:pytroll/trollduction into
  develop. [Adam.Dybbroe]
- Cleanup. [Martin Raspaud]
- Hardfix: Attempt running AAPP with all instruments, no exceptions for
  NOAA-15. [Adam.Dybbroe]
- Cleanup registry. [Adam.Dybbroe]
- Merge branch 'develop' of github.com:pytroll/trollduction into
  develop. [Adam.Dybbroe]
- Bugfix gc. [Martin Raspaud]
- Fix is_uri_on_server. [Martin Raspaud]
- Fix uri checking for scisys receiver. [Martin Raspaud]
- Remove install section in setup.cfg, and add netcdf4-python as a
  dependency. [Martin Raspaud]
- Cleaning up in sst-runner. [Adam.Dybbroe]
- Merge branch 'develop' of github.com:pytroll/trollduction into
  develop. [Adam.Dybbroe]
- Merge branch 'develop' of https://github.com/pytroll/trollduction into
  develop. [Panu Lahtinen]
- Add watchdog as a dependency to trollduction. [Martin Raspaud]
- Gatherer can now be parametrized as to which streams to watch. [Martin
  Raspaud]
- Example config for GEO satellite segment gatherer. [Panu Lahtinen]
- Gatherer for GEO satellite segments. [Panu Lahtinen]
- More debug info on NWP files found. [Adam.Dybbroe]
- Introduce new config param locktime_before_rerun. [Adam.Dybbroe]
- Fix the checking of same scene_id using time overlap determination.
  [Adam.Dybbroe]
- Merge branch 'develop' of github.com:pytroll/trollduction into
  develop. [Adam.Dybbroe]
- Retry saving file once in case of an IOError. [Martin Raspaud]
- Add some debug info. [Martin Raspaud]
- Retry when copying fails with IOError. [Martin Raspaud]
- Allow for Metop lvl0 instrument files with slightly (more than a
  minute) different start and end times. [Adam.Dybbroe]
- Removed buggy log-message. [Adam.Dybbroe]
- Allow for no hostname in message: url.hostname may be None.
  [Adam.Dybbroe]
- Merge branch 'develop' of github.com:pytroll/trollduction into
  develop. [Adam.Dybbroe]

  Conflicts:
  	trollduction/scisys.py

- Avoid key errors in scisys.py. [Martin Raspaud]
- Bugfix. [Adam.Dybbroe]
- Bugfix. [Adam.Dybbroe]
- More debug info. [Adam.Dybbroe]
- Clean up and pep8. [Adam.Dybbroe]
- 2met receiver checks that that message is for the current host only.
  [Adam.Dybbroe]
- Bug in region collector printout. [Martin Raspaud]
- Be more explicit in debug when the product can't be created. [Martin
  Raspaud]
- Change timeout in gatherer when last granule is not arriving last.
  [Martin Raspaud]
- Remove use of servername from config. [Adam.Dybbroe]
- Dynamic checking of hostname. [Adam.Dybbroe]
- Merge branch 'develop' of https://github.com/mraspaud/trollduction
  into develop. [Panu Lahtinen]

  Conflicts:
  	trollduction/collectors/trigger.py
  	trollduction/producer.py

- More debug info and check return code after cat command.
  [Adam.Dybbroe]
- Merge branch 'develop' of github.com:pytroll/trollduction into
  develop. [Adam.Dybbroe]
- Cleanup local_data before going on to the next area. [Martin Raspaud]
- Bugfix, use os.system for cat-command. [Adam.Dybbroe]
- Change the way system commands are called and logged, using Popen.
  [Adam.Dybbroe]
- Listens to AAPP-HRPT. [Adam.Dybbroe]
- Add some optional memory-leak detection. [Martin Raspaud]
- Bugfix flag for hirs in aapp runner. [Martin Raspaud]
- Listen to SDR/1B and not segment/SDR/1B. [Adam.Dybbroe]
- Don't crash if message doesn't have a uri. [Martin Raspaud]
- Adding the orbit number to the aapp call for metop. [Martin Raspaud]
- Create a new message in cat instead of recycling the old one. [Martin
  Raspaud]

  Otherwise sender and time don't get updated.

- Sort files before decompression for the cat. [Martin Raspaud]
- Fix the last fix to work when the netloc is empty. [Martin Raspaud]
- Fix hostname checking to dynamic instead of config-based. [Martin
  Raspaud]
- Allow only one sensor for ears metop. [Martin Raspaud]
- Merge branch 'develop' of github.com:pytroll/trollduction into
  develop. [Adam.Dybbroe]
- Add alias capability to cat. [Martin Raspaud]
- Make cat.py available as a script. [Martin Raspaud]
- Add cat script. [Martin Raspaud]
- Change the format for the xml output to PPS-XML, so that the
  l2processors will ignore these files/messages. [Adam.Dybbroe]
- Merge branch 'my-new-aapp-runner' into develop. [Adam.Dybbroe]
- Log stderr as info. [Adam.Dybbroe]
- Fix log reading. [Adam.Dybbroe]
- Merge branch 'my-new-aapp-runner' into develop. [Adam.Dybbroe]
- Subscribe to Segmen/SDR... [Adam.Dybbroe]
- Bugfix. publish_topic added as a keyword argument to WatchDogTrigger.
  [Adam.Dybbroe]
- Merge branch 'develop' into my-new-aapp-runner. [Adam.Dybbroe]

  Conflicts:
  	trollduction/collectors/trigger.py

- Debugging... [Adam.Dybbroe]
- Avhrr/3 name in call to mpop instead of avhrr. [Adam.Dybbroe]
- Avhrr instead of avhrr/3 in mpop call. [Adam.Dybbroe]
- Support for avhrr. [Adam.Dybbroe]
- Date/time bugfix. [Adam.Dybbroe]
- Bugfix. [Adam.Dybbroe]
- Developing sst_runner. [Adam.Dybbroe]
- Typo/bugfix. [Adam.Dybbroe]
- Adding osisaf sst runner. [Adam.Dybbroe]
- Bugfix. [Adam.Dybbroe]
- Install trollstalker2.py. [Adam.Dybbroe]
- Merge branch 'feature-trollstalker2' into my-new-aapp-runner.
  [Adam.Dybbroe]

  Conflicts:
  	trollduction/collectors/trigger.py

- New code checking if host matches message is commented out.
  [Adam.Dybbroe]
- Handle PpsRunError from pps. [Adam.Dybbroe]
- Only run if message is from the same server! [Adam.Dybbroe]
- Put back the update_nwp call at start up. [Adam.Dybbroe]
- Making a try, except clause around run function, and remove p.wait()
  call. [Adam.Dybbroe]
- Bugfix - orbit. [Adam.Dybbroe]
- Using platform_name consistently. [Adam.Dybbroe]
- Check for pps-script. [Adam.Dybbroe]
- Debugging and catching exceptions in pps_worker. [Adam.Dybbroe]
- More debug info in case of prepare_nwp crach. [Adam.Dybbroe]
- AAPP-PPS is the avhrr lvl1 publish format. [Adam.Dybbroe]
- Bugfix - data level. [Adam.Dybbroe]
- Install under /usr instead of /usr/local. [Adam.Dybbroe]
- Debug info added. [Adam.Dybbroe]
- Handle situation where no log file is given in env. [Adam.Dybbroe]
- Bugfix. [Adam.Dybbroe]
- Adding pps_runner.py to package and add the shell script.
  [Adam.Dybbroe]
- Merge branch 'new-pps-runner' into my-new-aapp-runner. [Adam.Dybbroe]
- Editorial. [Adam.Dybbroe@smhi.se]
- More debug info. [Adam.Dybbroe@smhi.se]
- Syncing with smhi-develop branch. [Adam.Dybbroe@smhi.se]
- Complete restructure of pps_runner. [Adam.Dybbroe@smhi.se]
- Rewrite pps-runner. [Adam.Dybbroe@smhi.se]
- Use smove function also for metop. [Adam.Dybbroe]
- Temporarily take away the cleaning of workdir. [Adam.Dybbroe]
- Print environment variables... [Adam.Dybbroe]
- Perform tleing also after each aapp run. [Adam.Dybbroe]
- Fixes for tleing. [Adam.Dybbroe]
- Adding support for new config variables. [Adam.Dybbroe]
- Add support for running tle-ingest etc from the runner. [Adam.Dybbroe]
- Put back the cleaning of the working dir after run. [Adam.Dybbroe]
- Bugfix. [Adam.Dybbroe]
- Fix satellite name for output-dir. [Adam.Dybbroe]
- More debug info. [Adam.Dybbroe]
- Bugfix. [Adam.Dybbroe]
- Bugfix. [Adam.Dybbroe]
- Call AAPP-script with orbit number + debugging (do not clean up after
  AAPP) [Adam.Dybbroe]
- Bugfix in printout. [Adam.Dybbroe]
- Bugfix. [Adam.Dybbroe]
- Remove pdb entries. [Adam.Dybbroe]
- Fix subscribe topics. [Adam.Dybbroe]
- Fixing the logging. [Adam.Dybbroe]
- Cleaning setup.py and adding setup.cfg. [Adam.Dybbroe]
- Bypassing host server checking. [Adam.Dybbroe]
- Bugfix. [Adam.Dybbroe]
- Making it merge with smhi branch. [Adam.Dybbroe]
- Cosmetics. [Adam.Dybbroe]
- Rename aapp_runner to aapp_dr_runner. [Adam.Dybbroe]
- Bugfix in import. [Adam.Dybbroe]
- Adding support for smhi station. [Adam.Dybbroe]
- Bug fixes. [jkotro]
- Fixing. [jkotro]
- Making a packge out of aapp_runner. [Adam.Dybbroe]
- Restructure of aapp_runner.py. [jkotro]
- Make sure that l2processor doesn't hang on crash. [Panu Lahtinen]
- Fixed incorrect function names in PostTrollTrigger. [Panu Lahtinen]
- Add handling for separate start_date + start_time, end_date and
  end_time (Suomi-NPP files) [Panu Lahtinen]
- Use UTC, not local time. [Panu Lahtinen]
- Fixed parsing of check_coverage from product config. [Panu Lahtinen]
- "continue" to next area item in collection instead of return, add
  handling for struct.error (raised in mipp) [Panu Lahtinen]
- Better handling of "run only once" history. [Panu Lahtinen]
- Merge branch 'feature-trollstalker2' into develop. [Adam.Dybbroe]

  Conflicts:
  	trollduction/collectors/trigger.py

- Merge branch 'develop' into feature-trollstalker2. [Adam.Dybbroe]

  Conflicts:
  	trollduction/collectors/trigger.py

- First iteration of the trollstalker rewrite. [Martin Raspaud]
- Retry failed processing once, workaround for mipp import error. [Panu
  Lahtinen]
- Some error handling for broken input data. [Panu Lahtinen]
- Add "history" to trollstalker, update config templates. [Panu
  Lahtinen]
- Possibility to stop reprocessing of the previous file with
  configuration option process_only_once=True. [Panu Lahtinen]
- For published message, collect also sub-dictionary keys/values for
  trollsift.compose. [Panu Lahtinen]
- Missing self added. [Panu Lahtinen]
- Added possibility to set publish_topic in l2processor_config.ini, with
  trollsift formating. [Panu Lahtinen]
- Check if file is local (workaround for file:// "protocol") [Panu
  Lahtinen]
- Removed forgotten obsolete argument. [Panu Lahtinen]
- Removed obsolete variable. [Panu Lahtinen]
- Merge branch 'feature_area_msg' into develop. [Panu Lahtinen]

  Conflicts:
  	trollduction/collectors/region_collector.py
  	trollduction/producer.py
  	trollduction/xml_read.py

- Fixes for logging (PEP8) [Panu Lahtinen]
- Style changes to logging. [Panu Lahtinen]
- Fixed a test after renaming a class member. [Panu Lahtinen]
- For inbound messages where type is collection, check if the area
  matches to the configured area. Also some cleanup for PEP8. [Panu
  Lahtinen]
- Added config option for using external calibration coefficients for
  channels 1, 2 and 3a. [Panu Lahtinen]
- Fix and re-enable checking valid and invalid satellites. [Panu
  Lahtinen]
- Merge documentation updates from branch 'zero_coverage' into develop.
  [Panu Lahtinen]

  Conflicts:
  	doc/source/index.rst
  	doc/source/usage.rst

- Updated documentation. [Panu Lahtinen]
- Fixed instrument -> sensor, clarified product config templates. [Panu
  Lahtinen]
- Making landscape happier. [Panu Lahtinen]
- Fix for having <dump> in the product config. [Panu Lahtinen]
- Removed as obsolete. [Panu Lahtinen]
- Update to gatherer usage. [Panu Lahtinen]
- Changed instrument -> sensor. [Panu Lahtinen]
- Fixed links. [Panu Lahtinen]
- Removed redundat documentation, added a link to readthedocs to README.
  [Panu Lahtinen]
- Updated configuration. [Panu Lahtinen]
- Merge branch 'develop' of https://github.com/mraspaud/trollduction
  into develop. [Panu Lahtinen]
- Merge pull request #4 from mraspaud/gatherer_publish_topic. [Panu
  Lahtinen]

  Gatherer publish topic
- Fixed test for PostTrollTrigger. [Panu Lahtinen]
- Changed logging to info from warning in case no topic has been given.
  [Panu Lahtinen]
- Config option "publish_topic" for setting custom topic for published
  messages by gatherer. [Panu Lahtinen]
- Small updates. [Panu Lahtinen]
- Removed obsolete config file. [Panu Lahtinen]
- Consistent template filenames and updates to examples. [Panu Lahtinen]
- Sync prepare_nwp from smhi-develop. [Adam.Dybbroe@smhi.se]
- Activate nwp update for testing. [Adam.Dybbroe@smhi.se]
- Adding nwp-stuff in pps-config template. [Adam.Dybbroe@smhi.se]
- More verbose. [Adam.Dybbroe]
- Bugfix. [Adam.Dybbroe]
- Bugfix. [Adam.Dybbroe]
- Add support for pps time statistics. [Adam.Dybbroe]
- Needs level in upper case. Warns if level is right but in lower case.
  [Adam.Dybbroe]
- Use upper case for level (1C instead of 1c) [Adam.Dybbroe]
- Listen to all levels of AAPP-HRPT (needs 1B and 1C) [Adam.Dybbroe]
- Use Upper case for processing level: "1B" instead of "1b"
  [Adam.Dybbroe]
- Change data proc level from 1b to 1B. [Adam.Dybbroe]
- Subscribing to 1B data only. [Adam.Dybbroe]
- Allow for different paths for hdf5/netcdf and xml output.
  [Adam.Dybbroe]
- Merge branch 'develop' of github.com:mraspaud/trollduction into
  develop. [Adam.Dybbroe]
- Don't listen to the SDR_compact (EARS-VIIRS) data. PPS is not
  compatible with this format. [Adam.Dybbroe]
- Uses socket.gethostname to get the server name, in case it is not
  provided in config. [Adam.Dybbroe]
- Also publish netCDF and XML output. [Adam.Dybbroe]
- Do not take aliases from the product list to replace metadata in
  incomming msg. [Martin Raspaud]
- Viirs-runner: get hostname from system, not from config file. [Martin
  Raspaud]
- Gatherer doesn't crash anymore when "pattern" is missing, it uses
  posttroll. [Martin Raspaud]
- Merge branch 'develop' of github.com:mraspaud/trollduction into
  develop. [Martin Raspaud]
- Typo. [Panu Lahtinen]
- Added new configuration options (nprocs, proj_method, precompute).
  [Panu Lahtinen]
- Added excecute file access bits. [Panu Lahtinen]
- Added new config options (nprocs, proj_method, precompute). [Panu
  Lahtinen]
- Restructuring. [Panu Lahtinen]
- Merge branch 'zero_coverage' into develop. [Panu Lahtinen]
- Use aliases also to replace the data in incoming messages (eg. MSG3 ->
  Meteosat-10) [Panu Lahtinen]
- Removed satnumber to reflect the coming changes in satellite naming.
  [Panu Lahtinen]
- Possibility to skip all area coverage calculations, skip area coverage
  calculation if min_coverage is zero, satnumber parameter returned to
  create_scene() call, cleaned log message formating, some syntactic
  cleanup (row lengths) [Panu Lahtinen]
- Added configuration option for skipping area coverage checks. [Panu
  Lahtinen]
- Merge pull request #1 from mraspaud/stalker_times. [Panu Lahtinen]

  Add "start_time" and "end_time" to messages if they are not present.
- Add "start_time" and "end_time" to messages if they are not present.
  The value "end_time" will be nominal_time (or "time", or
  "nominal_time") plus 15 minutes. [Panu Lahtinen]
- Make the uid unique for the different copies. [Martin Raspaud]
- Fix data processing level for cloud products. [Martin Raspaud]
- Fixing type/formats for output products. [Martin Raspaud]
- Fix format and type fields of output messages. [Martin Raspaud]
- Mock h5py and netcdf for documentation. [Martin Raspaud]
- Mock mpop for building documentation. [Martin Raspaud]


v0.2.0 (2015-02-19)
-------------------

Fix
~~~
- Bugfix: error message in image generation was buggy. [Martin Raspaud]
- Bugfix: variable substitution. [Martin Raspaud]
- Bugfix: unload after channel names. [Martin Raspaud]
- Bugfix: the unloading doesn't work on a list, * it. [Martin Raspaud]
- Bugfix: Error was shutil.Error. [Martin Raspaud]
- Bugfix: instrument is now called sensor. [Martin Raspaud]
- Bugfix: add missing dependency. [Martin Raspaud]
- Bugfix: don't check host for local files. [Martin Raspaud]
- Bugfix: remove last traces of minion. [Martin Raspaud]
- Bugfix: sleep forever in trollstalker now... [Martin Raspaud]

Other
~~~~~
- Update changelog. [Martin Raspaud]
- Bump version: 0.1.0 → 0.2.0. [Martin Raspaud]
- Change version numbering. [Martin Raspaud]
- Some more documentation. [Martin Raspaud]
- Update the documentation a bit. [Martin Raspaud]
- Merge branch 'feature-aapp-and-npp' of
  github.com:mraspaud/trollduction into feature-aapp-and-npp. [Martin
  Raspaud]
- Simplified the code. [Adam Dybbroe]
- Really kill the idle process. [Adam Dybbroe]
- Replace the corner estimation in region_collector with trollsched's
  routines. [Martin Raspaud]
- Install mock for travis. [Martin Raspaud]
- Change publisher name for gatherer to "gatherer". [Martin Raspaud]
- L2processor: print out reason when trollduction dies. [Martin Raspaud]
- Install hdf5 and netcdf on travis before testing. [Martin Raspaud]
- Add missing dependencies. [Martin Raspaud]
- Add pytroll-schedule as dependency. [Martin Raspaud]
- Handling IOError excpetion in copy file ("Stale file handle") [Adam
  Dybbroe]
- Try fixing a bug in an exception. [Adam Dybbroe]
- Bugfix. [Adam Dybbroe]
- Identifying pps jobs by time as well, and don't do repeated processing
  on scenes that are close in time. [Adam Dybbroe]
- More debug info. [Adam Dybbroe]
- Fixing Metop names for tle files. [Adam Dybbroe]
- More debug info. [Adam Dybbroe]
- Moving common function from aapp_runner to helper_functions. [Adam
  Dybbroe]
- More log info. [Adam Dybbroe]
- Merge branch 'feature-aapp-and-npp' of
  github.com:mraspaud/trollduction into feature-aapp-and-npp. [Adam
  Dybbroe]

  Conflicts:
  	bin/trollstalker.py

- Merge branch 'feature-aapp-and-npp' of
  github.com:mraspaud/trollduction into feature-aapp-and-npp. [Martin
  Raspaud]

  Conflicts:
  	bin/trollstalker.py

- Add orbit style flag for have consistent orbit numbers in the system.
  [Martin Raspaud]
- Derive orbit number in aapp runner. [Adam Dybbroe]
- Handling more than one instrument in config file. [Adam Dybbroe]
- Bugfix and more debug info. [Adam Dybbroe]
- Bugfix. [Adam Dybbroe]
- Bugfix. [Adam Dybbroe]
- More debug info. [Adam Dybbroe]
- Bugfix again... [Adam Dybbroe]
- Bugfix. [Adam Dybbroe]
- Bugfixing and cleaning up a bit in aapp-runner. [Adam Dybbroe]
- Adding template for pps-run script. [Adam Dybbroe]
- Adapting to new pps bash script, where no date/time is provided for
  other satellites than EOS. [Adam Dybbroe]
- Allowing aapp to run also on DMI data. [Adam Dybbroe]
- Fix thumbnail_size type when generating error message. [Martin
  Raspaud]
- Pps_runner now publishes h5 files instead. [Martin Raspaud]
- Try bug fixing debug printout... [Adam Dybbroe]
- Remove all shell=True from Popen calls. [Adam Dybbroe]
- Bugfix... [Adam Dybbroe]
- Bugfix. [Adam Dybbroe]
- Bugfix... [Adam Dybbroe]
- Popen tests... [Adam Dybbroe]
- Using shlex to construct Popen arguments. [Adam Dybbroe]
- Changing Popen calls... [Adam Dybbroe]
- Shell=True (going back, since shell=False didn't work) [Adam Dybbroe]
- Set working dir for Aqua gbad processing! [Adam Dybbroe]
- Check the status code from the MODIS lvl1 processing and only continue
  if it is equal "0" [Adam Dybbroe]
- Add more log info. [Adam Dybbroe]
- Restructure modis runner for standardised logging. [Adam Dybbroe]
- Remove unnecessary tle handling. [Martin Raspaud]
- Remove unnecessary hardcoded global variables and config items.
  [Martin Raspaud]
- Print out viirs config file on running. [Martin Raspaud]
- Merge branch 'feature-aapp-and-npp' of
  github.com:mraspaud/trollduction into feature-aapp-and-npp. [Martin
  Raspaud]
- Add more debug info. [Adam Dybbroe]
- Adding module name to log. [Adam Dybbroe]
- Merge branch 'feature-aapp-and-npp' of
  github.com:mraspaud/trollduction into feature-aapp-and-npp. [Adam
  Dybbroe]
- Changed logging format for modis, and fixed symlink bug. [Adam
  Dybbroe]
- Use command-line parameters for viirs_dr_runner. [Martin Raspaud]
- On linking error, tell which files are failing. [Martin Raspaud]
- Allow reading configuration for pycoast. [Martin Raspaud]
- Updating the documentation. [Martin Raspaud]
- Add coverage functionality for geostationary data. [Martin Raspaud]
- Gatherer: add the possibility to choose which observer is being used.
  [Martin Raspaud]
- Merge branch 'feature-aapp-and-npp' of
  github.com:mraspaud/trollduction into feature-aapp-and-npp. [Martin
  Raspaud]
- Revert "Go back to 'old' modis_dr_runner from mid November" [Adam
  Dybbroe]

  This reverts commit c6e1f0e5047eb780b71af56364446000c755507e.

- Go back to 'old' modis_dr_runner from mid November. [Adam Dybbroe]
- Change the subscription. [Adam Dybbroe]
- Remove modis script from bin. [Adam Dybbroe]
- Update documentation. [Martin Raspaud]
- Remove area coverage computation if no overpass attribute is present.
  [Martin Raspaud]
- Bugfix trollstalker: the file parsing is now occuring on the basename.
  [Martin Raspaud]
- Merge branch 'feature-aapp-and-npp' of
  github.com:mraspaud/trollduction into feature-aapp-and-npp. [Martin
  Raspaud]
- Debug info added. [Adam Dybbroe]
- Adapted to modis_runner. [Adam Dybbroe]
- Fixing modis_runner. [Adam Dybbroe]
- Trollstalker improvements to avoid wrong error catching. [Martin
  Raspaud]
- Check for local ips with netifaces (should be more robust) [Martin
  Raspaud]
- Receive RDRs from any publisher. [Martin Raspaud]
- Add a working dir for modis gbad processing. [Martin Raspaud]
- Fix trollstalker to new metadata. [Martin Raspaud]
- Report error on KeyError for product_config_file. [Martin Raspaud]
- Add modis_runner.py. [Martin Raspaud]
- "variables" now accepts environment variables to check against.
  [Martin Raspaud]
- Allow specifying overlay="#<color>" in xml product list. [Martin
  Raspaud]
- Bugfix thumbnailing. [Martin Raspaud]
- Merge branch 'feature-aapp-and-npp' of
  github.com:mraspaud/trollduction into feature-aapp-and-npp. [Martin
  Raspaud]
- Merge branch 'feature-aapp-and-npp' of
  github.com:mraspaud/trollduction into feature-aapp-and-npp. [Adam
  Dybbroe]
- Allow to listen for everything publishing level 1 files. [Adam
  Dybbroe]
- Add thumbnailing functionality. [Martin Raspaud]
- Add a time_interval load argument if possible. [Martin Raspaud]
- Do not create satellite scenes with multiple sensors. [Martin Raspaud]
- Allow multiple sensors in message. [Martin Raspaud]
- Coverage computation is now done at the group level to avoid unloading
  if possible. [Martin Raspaud]
- Print out linking exceptions. [Martin Raspaud]
- Merge branch 'feature-aapp-and-npp' of
  github.com:mraspaud/trollduction into feature-aapp-and-npp. [Martin
  Raspaud]
- Bugfix, for metop. [Adam Dybbroe]
- More debug info in aapp runner. [Adam Dybbroe]
- Merge branch 'feature-aapp-and-npp' of
  github.com:mraspaud/trollduction into feature-aapp-and-npp. [Adam
  Dybbroe]
- Correcting the name of the runner publishing. [Adam Dybbroe]
- Fix multiple Thread inheritance. [Martin Raspaud]
- Groups can now have "unload" and "resolution" parameters. [Martin
  Raspaud]
- Do not crash when copying goes wrong. [Martin Raspaud]
- Scale coverages to the same magnitude order. [Martin Raspaud]
- Add coverage computation. [Martin Raspaud]
- Fix copy to itself. [Martin Raspaud]
- Make orbit number an int when sending out messages. [Martin Raspaud]
- Comments added. [Martin Raspaud]
- Merge branch 'feature-aapp-and-npp' of
  github.com:mraspaud/trollduction into feature-aapp-and-npp. [Martin
  Raspaud]
- Merge branch 'feature-aapp-and-npp' of
  github.com:mraspaud/trollduction into feature-aapp-and-npp. [Adam
  Dybbroe]
- Level 1 data dir is set outside PPS. [Adam Dybbroe]
- Add aliases possibility in the product list and copy files when
  already saved. [Martin Raspaud]
- Merge branch 'feature-aapp-and-npp' of
  github.com:mraspaud/trollduction into feature-aapp-and-npp. [Martin
  Raspaud]
- Adapting PPS for collections. [Adam Dybbroe]
- Remove platform name translation. [Martin Raspaud]
- Move check_uri out of the dataprocessor class. [Martin Raspaud]
- Mock out entire watchdogtrigger on importerror. [Martin Raspaud]
- Mock watchdog if not present. [Martin Raspaud]
- Catch importerrors when watchdog is imported. [Martin Raspaud]
- Add collectors in setup.py. [Martin Raspaud]
- Add the collector __init__.py. [Martin Raspaud]
- Move gatherer to bin. [Martin Raspaud]
- Merge branch 'feature-aapp-and-npp' of
  github.com:mraspaud/trollduction into feature-aapp-and-npp. [Martin
  Raspaud]
- Bugfix, sensor naming. [Adam Dybbroe]
- Bugfix. [Adam Dybbroe]
- Bugfix. [Adam Dybbroe]
- Bugfix. [Adam Dybbroe]
- Bugfix. [Adam Dybbroe]
- More consistency in platform name handling. [Adam Dybbroe]
- Bugfix - published satellite name. [Adam Dybbroe]
- Bugfix. [Adam Dybbroe]
- Fix metadata in output messages from pps. [Adam Dybbroe]
- Handle collections in producer. [Martin Raspaud]
- Fix gatherer and regioncollector for new metadata and npp granules.
  [Martin Raspaud]
- Add PostTrollTrigger to triggers. [Martin Raspaud]
- Switch SDR to level 1b (instead of 1) [Martin Raspaud]
- Log exception in case of incomplete or corrupted data. [Martin
  Raspaud]
- Merge branch 'feature-aapp-and-npp' of
  github.com:mraspaud/trollduction into feature-aapp-and-npp. [Martin
  Raspaud]
- Merge branch 'feature-aapp-and-npp' of
  github.com:mraspaud/trollduction into feature-aapp-and-npp. [Adam
  Dybbroe]
- Bugfix sensor naming. [Adam Dybbroe]
- Do not publish messages if no sdr files are created. [Martin Raspaud]
- Merge branch 'feature-aapp-and-npp' of
  github.com:mraspaud/trollduction into feature-aapp-and-npp. [Martin
  Raspaud]
- Bugfix. [Adam Dybbroe]
- Change viirs_dr_runner to send batch of files as datasets. [Martin
  Raspaud]
- Remove non-existant scripts from setup. [Martin Raspaud]
- Add some debugging messages around data loading. [Martin Raspaud]
- Remove smhi scripts. [Martin Raspaud]
- Merge branch 'feature-aapp-and-npp' of
  github.com:mraspaud/trollduction into feature-aapp-and-npp. [Martin
  Raspaud]
- Installs aapp runner. [Adam Dybbroe]
- Merge branch 'feature-aapp-and-npp' of
  github.com:mraspaud/trollduction into feature-aapp-and-npp. [Martin
  Raspaud]
- Merge branch 'feature-aapp-and-npp' of
  github.com:mraspaud/trollduction into feature-aapp-and-npp. [Adam
  Dybbroe]
- Aapp config template (from smhi) [Adam Dybbroe]
- Add the (smhi) aapp_runner.py. [Adam Dybbroe]
- Consistent metop/noaa sensor names. [Adam Dybbroe]
- Smoother crashing of producer.py. [Martin Raspaud]
- Merge branch 'feature-aapp-and-npp' of
  github.com:mraspaud/trollduction into feature-aapp-and-npp. [Martin
  Raspaud]
- Bugfix - orbit. [Adam Dybbroe]
- Bugfix - instrument->sensor. [Adam Dybbroe]
- Bugfix. [Adam Dybbroe]
- Install pps scripts. [Adam Dybbroe]
- Adding pps runner. [Adam Dybbroe]
- Fix sensor=modis in published messages. [Adam Dybbroe]
- Bugfix! Arggghh! [Adam Dybbroe]
- Adding helper function to create (aqua) messages from receiver log for
  later ingestion. [Adam Dybbroe]
- More debugging. [Adam Dybbroe]
- Add debug info. [Adam Dybbroe]
- Bugfix EOS-Aqua name... [Adam Dybbroe]
- Bugfix. [Adam Dybbroe]
- Debug info and pep8. [Adam Dybbroe]
- Renamed modis_runner function not to be confused with modulename.
  [Adam Dybbroe]
- More deug info - message creation is at error! [Adam Dybbroe]
- Bugfix. [Adam Dybbroe]
- Less verbose. [Adam Dybbroe]
- Adapt to new message format. [Adam Dybbroe]
- GPL header added. [Adam Dybbroe]
- Npp/viirs bugfixes. [Adam Dybbroe]
- Producer adaptation to "dataset" messages. [Martin Raspaud]
- Allow to run l2proc on several topics. [Martin Raspaud]
- Bugfix modis. [Martin Raspaud]
- Fix instrument->sensor. [Martin Raspaud]
- Merge branch 'feature-aapp-and-npp' of
  github.com:mraspaud/trollduction into feature-aapp-and-npp. [Martin
  Raspaud]
- Fix installation of npp-stuff. [Adam Dybbroe]
- Merge branch 'feature-aapp-and-npp' of
  github.com:mraspaud/trollduction into feature-aapp-and-npp. [Adam
  Dybbroe]
- Adding template for viirs. [Adam Dybbroe]
- Adding S-NPP VIIRS runner. [Adam Dybbroe]
- Send datasets for modis l1b files. [Martin Raspaud]
- Merge branch 'feature-aapp-and-npp' of
  github.com:mraspaud/trollduction into feature-aapp-and-npp. [Martin
  Raspaud]
- Bugfix. [Adam Dybbroe]
- Moving smhi'fied script to a template/example. [Adam Dybbroe]
- Remove smhi stuff. [Adam Dybbroe]
- Merge branch 'feature-aapp-and-npp' of
  github.com:mraspaud/trollduction into feature-aapp-and-npp. [Adam
  Dybbroe]
- Merge branch 'smhi-develop' of /data/proj/SAF/GIT/trollduction into
  feature-aapp-and-npp. [Adam Dybbroe]
- Merge branch 'feature-aapp-and-npp' into smhi-develop. [Martin
  Raspaud]

  Conflicts:
  	setup.py

- Add pyinotify to the list of dependencies. [Martin Raspaud]
- Fixing setup for SMHI. [Martin Raspaud]
- Change modis runner to accept new metadata standard. [Martin Raspaud]
- Merge branch 'feature-aapp-and-npp' of
  github.com:mraspaud/trollduction into feature-aapp-and-npp. [Martin
  Raspaud]
- Bugfix, and comment away broken tests! [Adam Dybbroe]
- Adding the modis-dr-runner from smhi. [Adam Dybbroe]
- Add orbit_number for NPP rdrs. [Martin Raspaud]
- Bugfix scisys: satellite is not always defined for npp rdrs. [Martin
  Raspaud]
- Add the scisys library. [Martin Raspaud]
- Add scisys_receiver.py to scripts. [Martin Raspaud]
- Update producer for new metadata standard. [Martin Raspaud]
- Add scisys test to test bench. [Martin Raspaud]
- Change description in setup.py. [Martin Raspaud]
- Add scisys receiver. [Martin Raspaud]
- Implement area groups. [Martin Raspaud]
- Metadata adjustments. [Martin Raspaud]
- Import AreaNotFound error. [Martin Raspaud]
- Don't crash on area not found. [Martin Raspaud]
- Set orbit number as string. [Martin Raspaud]
- Various fixes. [Martin Raspaud]
- Fix unittest. [Martin Raspaud]
- Do not crash when composite is not available for satellite. [Martin
  Raspaud]
- Cleanup. [Martin Raspaud]
- Logging and argparsing in catter. [Martin Raspaud]
- Add example files for gatherer and catter. [Martin Raspaud]
- Remove hardcoded link to configuration files. [Martin Raspaud]
- Accept collections in producer. [Martin Raspaud]
- Granule handling, first commit. [Martin Raspaud]

  * Region collection is implemented.
  * catter cats the low level data.

- Implemented variable substitution in xml product lists. [Martin
  Raspaud]
- Try to fix unittest. [Martin Raspaud]
- Add publishing of generated files. [Martin Raspaud]
- Refactoring to allow multiple files per product, among other things.
  [Martin Raspaud]
- Bugfix for integer satellite numbers. [Martin Raspaud]
- Orbit is now orbit_number in config files. [Martin Raspaud]
- Test mock nc/cf. [Martin Raspaud]
- Mock trollsift in test. [Martin Raspaud]
- Producer refactoring and netcdf revamping to avoid race condition.
  [Martin Raspaud]
- Change 'orbit' to 'orbit_number' [Martin Raspaud]
- Add trollsift to the list of dependencies. [Martin Raspaud]
- Add pyorbital to the list of dependencies. [Martin Raspaud]
- Add pykdtree and trollimage to the list of dependencies. [Martin
  Raspaud]
- Add pyresample to the list of dependencies. [Martin Raspaud]
- Add posttroll to the list of dependencies. [Martin Raspaud]
- Add mpop to the list of dependencies. [Martin Raspaud]
- First test for run should be complete. [Martin Raspaud]
- Rename orbit parameter to orbit_number. [Martin Raspaud]
- Add trollduction unittest skeleton. [Martin Raspaud]
- New xml format. [Martin Raspaud]
- Rename trollduction.py to producer.py to avoid confusion with package
  name. [Martin Raspaud]
- Merge remote branch 'origin/develop' into feature-aapp-and-npp.
  [Martin Raspaud]

  Conflicts:
  	trollduction/trollduction.py

- Renamed config item "service" to "topic" [Panu Lahtinen]
- Added try/except blocks to make the production more robust, changed
  config item "service" to "topic" [Panu Lahtinen]
- Removed references to lxml which is not used anymore. [Panu Lahtinen]
- Removed the need for lxml, use the standard lib xml.etree.ElementTree
  instead. [Panu Lahtinen]
- Fixed errors in example configs, updated the message for reading
  product config. [Panu Lahtinen]
- Merge branch 'feature-aapp-and-npp' of
  github.com:mraspaud/trollduction into feature-aapp-and-npp. [Martin
  Raspaud]

  Conflicts:
  	trollduction/trollduction.py

- Support messages with satellite instead of platform and number.
  [Martin Raspaud]
- Support messages with satellite instead of platform and number.
  [Martin Raspaud]
- Get the time from different possible tags. [Martin Raspaud]
- Remove annoying Minion parent, doesn't make sense with supervisord.
  [Martin Raspaud]
- Pep8 style corrections. [Martin Raspaud]
- Load the filename provided in the message if possible. [Martin
  Raspaud]
- Check if file is on the localhost before running. [Martin Raspaud]
- Add pyinotify to the install dependencies. [Martin Raspaud]
- Added "aliases" for replacing values in messages. [Panu Lahtinen]
- Requirements file for Read the Docs. [Panu Lahtinen]
- Fixed a type in "Sun too low night-only product" [Panu Lahtinen]
- Escape a part that ReST interpreted as a target (link) [Panu Lahtinen]
- Moved also template product configs to *_template filenames. [Panu
  Lahtinen]
- Possibility to change timezone for log timestamps (default: UTC),
  updated/fixed documentation, install bin/*.py, moved config templates
  to examples/, config files to *.ini_template, config files with
  _template ending can't be used. [Panu Lahtinen]
- Updated documentation. [Panu Lahtinen]
- Removed log_dir config item, which is not used. [Panu Lahtinen]
- Few updates to documentation. [Panu Lahtinen]
- Use unified configuration file for trollstalker and l2processor,
  removed deprecated files and added example/master_config.ini to show
  two examples how the configuration is made. [Panu Lahtinen]
- Deleted depracated config for filepatterns. [Panu Lahtinen]
- Changed to use posttroll NSSubscriber keyword 'service' instead of old
  data_type_list. [Panu Lahtinen]
- Reorganized and added missing keywords. [Panu Lahtinen]
- Reorganized items and added missing keywords. [Panu Lahtinen]
- Added config_item keyword. [Panu Lahtinen]
- Added 'instrument' config option and propagate this info to message.
  [Panu Lahtinen]
- Moved to examples/procuct_config_hrit.xml. [Panu Lahtinen]
- Example product configs for NOAA/AVHRR HRPT/AAPP/l1b and MSG/HRIT.
  [Panu Lahtinen]
- Removed deprecated config file. [Panu Lahtinen]
- Trollduction config in config.ini format. [Panu Lahtinen]
- Use trollsift.Parser to generate filenames. [Panu Lahtinen]
- Added a possibility to read config.ini format. [Panu Lahtinen]
- Fixes to syntax. [Panu Lahtinen]
- Merge remote-tracking branch 'origin/feature_parser_stalker' into
  develop. [Panu Lahtinen]

  Conflicts:
  	bin/main.py
  	bin/trollstalker.py

  Conflicts resolved.

- Syntactical cleanup. [Panu Lahtinen]
- Log config for trollstalker. [Panu Lahtinen]
- File pattern and logging.cfg. [Panu Lahtinen]
- Deleted empty file. [Panu Lahtinen]
- Deleted obsolete xml-config. [Panu Lahtinen]
- Changed to use trollsift.Parser for getting information from files,
  changed to config.ini format. TODO: using config doesn't work! [Panu
  Lahtinen]
- Example configuration file for trollstalker in config.ini format.
  [Panu Lahtinen]
- Merge remote-tracking branch 'origin/feature_xrit_extent' into
  develop. [Panu Lahtinen]

  Conflicts:
  	trollduction/custom_handler.py
  	trollduction/trollduction.py

  Conflicts resolved.

- Converted to use area extent calculations based on the area definition
  borders instead of lonlat corner points. [Panu Lahtinen]
- Removed disable_data_reduce config keyword. [Panu Lahtinen]
- Removed handling of disable_data_reduce config keyword. [Panu
  Lahtinen]
- GEO extent calculations moved to mpop, data reduction (for swath data)
  moved to mpop. [Panu Lahtinen]
- Added get_maximum_ll_borders() [Panu Lahtinen]
- Added <disable_data_reduce> [Panu Lahtinen]
- Moved OldTrollduction to own file old_trollduction.py. [Panu Lahtinen]
- Adjusted to use old_trollduction.OldTrollduction. [Panu Lahtinen]
- Moved older version of trollduction to own file. Also, implemented
  area extent for any area definition (regardless of projection) for
  MSG, and data reduction for polar satellites. [Panu Lahtinen]
- Moved common functions to own file. [Panu Lahtinen]
- Syntactical cleanup. [Panu Lahtinen]
- Syntactic cleanup. [Panu Lahtinen]
- Removed obsolete publisher/logger. [Panu Lahtinen]
- Support for getting maximum extent in lon/lat. Useable with MSG(3),
  and shouldn't break polar satellite production. [Panu Lahtinen]
- Merge remote-tracking branch 'origin/feature-duke' into develop. [Panu
  Lahtinen]

  Conflicts:
  	bin/trollstalker.py

  Conflict fixed.

- Tweaks for get_lan_ip() [Panu Lahtinen]
- Working version to test-run OldTrollduction. [Panu Lahtinen]
- Add poking. [Martin Raspaud]
- Work on dungeon keeper. [Martin Raspaud]
- Refactor trollduction. [Martin Raspaud]
- Removed deprecated publisher/logger. [Panu Lahtinen]
- More notation cleanup. [Panu Lahtinen]
- Notation cleanup. [Panu Lahtinen]
- Merge branch 'feature_config' into develop. [Martin Raspaud]
- Added IN_MOVED_TO and a commandline switch for enabling debug
  messages. [Panu Lahtinen]
- Remove old print messages. [Martin Raspaud]
- Panu's custom handler. [Martin Raspaud]
- Cleanup. [Martin Raspaud]
- Logging now uses a standard config file. [Martin Raspaud]
- Cleanup. [Martin Raspaud]
- Switch to standard logging with a pytroll handler. [Martin Raspaud]
- Removed debug print IN_CLOSE_WRITE. [Panu Lahtinen]
- Removed unneeded events. [Panu Lahtinen]
- Changed has_key to "in" [Panu Lahtinen]
- Removed unnecessary import of sys. [Panu Lahtinen]
- Changed has_key() to in. [Panu Lahtinen]
- Fix for conflicting member names. [Panu Lahtinen]
- Possibility to use select local or UTC time (default) for logging in
  trollduction_config.xml (<use_local_time>1</use_local_time>) [Panu
  Lahtinen]
- Fixed incorrect event IN_MOVED_IN to IN_MOVED_TO. [Panu Lahtinen]
- Changed to use Queue.Queue instead of mutliprocessing.Pipe for message
  passing, and made the program cleanly stoppable by ctrl+c. [Panu
  Lahtinen]
- Changed to use Queue.Queue instead of multiprocessing.Pipe for
  handling message passing. [Panu Lahtinen]
- Added clean stopping for Publisher. [Panu Lahtinen]
- Better event masking using bit-wise or. [Panu Lahtinen]
- Fixed --monitored_dirs commandline switch. [Panu Lahtinen]
- Removed old logger. [Panu Lahtinen]
- Example config for trollstalker. [Panu Lahtinen]
- Now using new logger/publisher with 60 s heartbeat. [Panu Lahtinen]
- New logger/publisher. [Panu Lahtinen]
- Removed references to old logger. [Panu Lahtinen]
- In trollstalker, command line args take precedence. Missing config
  file doesn't crash. [Martin Raspaud]
- Log&publish listener readiness. [Panu Lahtinen]
- Removed unnecessary print. [Panu Lahtinen]
- Logging and placeholder for message publishing. [Panu Lahtinen]
- Clarifications to check_sunzen() [Panu Lahtinen]
- Sun zenith-angle limits can be checked with pixel location given in
  product configuration file. [Panu Lahtinen]
- Sun zenith angle limits can be checked against configured location
  (lon, lat) [Panu Lahtinen]
- Empty line removed. [Panu Lahtinen]
- Possibility to add integer to xml value. [Panu Lahtinen]
- Check for orbit=None. [Panu Lahtinen]
- Separated MSG2 (Meteosat 9) and MSG3 (Meteosat 10) [Panu Lahtinen]
- Template for trollduction file info parsing and filename matching.
  HRIT and HRPT l1b filepatterns are implemented. [Panu Lahtinen]
- Added a function that reads filepattern template xml for trollstalker.
  [Panu Lahtinen]
- Install etc/ directory. [Panu Lahtinen]
- Possibility to use configuration files. File info parsing based on xml
  template. [Panu Lahtinen]
- Moved to examples/ [Panu Lahtinen]
- Moved to examples. [Panu Lahtinen]
- Moved to examples. [Panu Lahtinen]
- Moved to examples/ [Panu Lahtinen]
- Adapted to new message format from trollstalker. [Panu Lahtinen]
- Refactored zenith angle and satellite checks to methods, minor
  cleanup. [Panu Lahtinen]
- Added comment on Sun zenith angle limits. [Panu Lahtinen]
- Sun zenith angle limitations relative to image center. [Panu Lahtinen]
- Step-by-step instructions. [Panu Lahtinen]
- Old stuff. [Panu Lahtinen]
- Old stuff. [Panu Lahtinen]
- Old stuff. [Panu Lahtinen]
- Fixed product_config_file tag. [Panu Lahtinen]
- Execution bit set. [Panu Lahtinen]
- Moved to trollduction/bin/ [Panu Lahtinen]
- Moved to trollduction/bin/ [Panu Lahtinen]
- Moved to trollduction/bin/ [Panu Lahtinen]
- Moved to trollduction/bin/ [Panu Lahtinen]
- Fixed imports, moved to bin/ [Panu Lahtinen]
- Fixed imports. [Panu Lahtinen]
- Sunzen tags renamed. [Panu Lahtinen]
- Imports fixed. [Panu Lahtinen]
- Fixed channel data load/unload. [Panu Lahtinen]
- More configuration items used. Also better channel load/unload
  function. [Panu Lahtinen]
- Delete unneeded files. [Panu Lahtinen]
- Working example config. [Panu Lahtinen]
- Couple of semantic changes. [Panu Lahtinen]
- XML reader/parser adapted for Trollduction. [Panu Lahtinen]
- Partly adapted to use configuration files. [Panu Lahtinen]
- Updated configuration file. [Panu Lahtinen]
- Typo. [Panu Lahtinen]
- First guess of product config file. [Panu Lahtinen]
- Typo. [Panu Lahtinen]
- Reorganize and plans for class member structuring. [Panu Lahtinen]
- Adjusted to use ListenerContainer class. [Panu Lahtinen]
- Container class added. [Panu Lahtinen]
- Grouped satellite information to dictionary, and removed duplicate
  time_slot parameter from draw_images. [Panu Lahtinen]
- Satellite information to Trollduction attributes. [Panu Lahtinen]
- Updated listener restart to new posttroll version. [Panu Lahtinen]
- Removed white space from listener inits. [Panu Lahtinen]
- Removed white spaces from file types. [Panu Lahtinen]
- File types changed and a small cleanup. [Panu Lahtinen]
- Merge branch 'feature_new_posttroll' into develop. [Martin Raspaud]

  Conflicts:
  	trollduction/trollduction.py

- Merge branch 'feature_new_posttroll' of
  github.com:mraspaud/trollduction into feature_new_posttroll. [Martin
  Raspaud]
- Working filemask. [Panu Lahtinen]
- Adapt to the new posttroll, and cleanup a few things. [Martin Raspaud]
- Merge branch 'develop' of https://github.com/mraspaud/trollduction
  into develop. [Panu Lahtinen]
- Change the copyright year... [Martin Raspaud]
- Minor fixes and updates to docstrings. [Panu Lahtinen]
- Member functions. [Panu Lahtinen]
- Added a line in the documentation. [Martin Raspaud]
- Added documentation template. [Martin Raspaud]
- Add support for travis, add the test framework structure. [Martin
  Raspaud]
- Merge branch 'master' into develop. [Martin Raspaud]

  Conflicts:
  	trollduction/listener.py

- Outdated parallel functions. [Panu Lahtinen]
- Main for testing without config file. [Panu Lahtinen]
- Main for testing without config file. [Panu Lahtinen]
- Testable version with serial processing. [Panu Lahtinen]
- Added fileinfo parsing to message. [Panu Lahtinen]
- Minor updates for better usability. [Panu Lahtinen]
- Main() for testing trollduction. [Panu Lahtinen]
- First runnable version. [Panu Lahtinen]
- Pyinotify with messaging for trollduction. [Panu Lahtinen]
- Example main for completed system. [Panu Lahtinen]
- Skeleton version of trollduction.py and a working listener.py. [Panu
  Lahtinen]
- Better handling of thread pool and some error handling. [Martin
  Raspaud]

   * semaphore is now acquired before thread creation
   * unknown format error doesn't crash thread
   * generate_composites now accepts hooks

- Remove relative imports and added a setup.py and version.py. [Martin
  Raspaud]
- Semaphore to avoid fork bombs. [Martin Raspaud]
- Add overlay dynamically. [Martin Raspaud]
- Changed orbit to orbit_number in messages. [Martin Raspaud]
- Merge branch 'develop' of github.com:mraspaud/trollduction into
  develop. [Martin Raspaud]
- Renamed dirstalker_sat.py to dirstalker.py. [karjaljo]
- Sample xml product list. [Martin Raspaud]
- WIP Producer. Creates images now :) [Martin Raspaud]
- Added a few more info items in dirstalker_sat.py and an example
  message. [Martin Raspaud]
- Adding the __init__.py file to make trollduction a package. [Martin
  Raspaud]
- Rename postroll_listener to producer.py. [Martin Raspaud]
- Merge branch 'develop' of https://github.com/mraspaud/trollduction
  into develop. [karjaljo]
- Added self.subscriber to class members. [Panu Lahtinen]
- Listener class and a simple publisher for testing. [Panu Lahtinen]
- Added logger configuration file and logger init function. [karjaljo]
- Initial code commit. [Martin Raspaud]
- Add ~ files to .gitignore. [Martin Raspaud]
- Initial commit. [Martin Raspaud]


