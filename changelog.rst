Changelog
=========

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


