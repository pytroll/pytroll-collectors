"""Tests for trollstalker."""
import os
import time
from threading import Thread

from posttroll.message import Message
from pytroll_collectors.trollstalker import main, stop


def create_config_file(dir_to_watch, tmp_path):
    """Create a config file for trollstalker."""
    config = """# This config is used in Trollstalker.

[noaa_hrpt]
topic=/HRPT/l1b/dev/mystation
directory=""" + os.fspath(dir_to_watch) + """
filepattern={path}hrpt_{platform_name}_{start_time:%Y%m%d_%H%M}_{orbit_number:05d}.l1b
instruments=avhrr/3,mhs,amsu-b,amsu-a,hirs/3,hirs/4
#stalker_log_config=/usr/local/etc/pytroll/trollstalker_logging.ini
loglevel=DEBUG
event_names=IN_CLOSE_WRITE,IN_MOVED_TO
posttroll_port=0
alias_platform_name = noaa18:NOAA-18|noaa19:NOAA-19
history=10"""
    config_file = tmp_path / "config.ini"
    with open(config_file, "w") as fd:
        fd.write(config)
    return config_file


def test_trollstalker(tmp_path, caplog):
    """Test trollstalker functionality."""
    dir_to_watch = tmp_path / "to_watch"
    os.makedirs(dir_to_watch)

    config_file = create_config_file(dir_to_watch, tmp_path)

    thread = Thread(target=main, args=[["-c", os.fspath(config_file), "-C", "noaa_hrpt"]])
    thread.start()
    time.sleep(.5)
    trigger_file = dir_to_watch / "hrpt_noaa18_20230524_1017_10101.l1b"
    with open(trigger_file, "w") as fd:
        fd.write("hej")
    time.sleep(.5)
    stop()
    thread.join()
    assert "Publishing message pytroll://HRPT/l1b/dev/mystation file " in caplog.text
    for line in caplog.text.split("\n"):
        if "Publishing message" in line:
            message = Message(rawstr=line.split("Publishing message ")[1])

    assert message.data['platform_name'] == "NOAA-18"
    assert message.data['uri'] == os.fspath(trigger_file)


# def test_trollstalker_directory_does_not_exist(tmp_path, caplog):
#     dir_to_watch = tmp_path / "to_watch"
#
#     config_file = create_config_file(dir_to_watch, tmp_path)
#
#     thread = Thread(target=main, args=[["-c", os.fspath(config_file), "-C", "noaa_hrpt"]])
#     thread.start()
#     time.sleep(.5)
#     with open(dir_to_watch / "hrpt_noaa18_20230524_1017_10101.l1b", "w") as fd:
#         fd.write("hej")
#     time.sleep(.5)
#     stop()
#     thread.join()
#     assert "Publishing message pytroll://HRPT/l1b/dev/mystation file " in caplog.text
