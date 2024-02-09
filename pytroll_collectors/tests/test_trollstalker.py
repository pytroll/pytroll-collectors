"""Tests for trollstalker."""
import os
import time
import pytest

from posttroll.message import Message
from pytroll_collectors.trollstalker import start_observer, stop_observer


LAG_SECONDS = 0.02


@pytest.fixture
def dir_to_watch(tmp_path):
    """Define a dir to watch."""
    dir_to_watch = tmp_path / "to_watch"
    return dir_to_watch


@pytest.fixture
def config_file(tmp_path, dir_to_watch):
    """Create a config file for trollstalker."""
    config = """# This config is used in Trollstalker.

[noaa_hrpt]
topic=/HRPT/l1b/dev/mystation
directory=""" + os.fspath(dir_to_watch) + """
filepattern={path}hrpt_{platform_name}_{start_time:%Y%m%d_%H%M}_{orbit_number:05d}.l1b
instruments=avhrr/3,mhs,amsu-b,amsu-a,hirs/3,hirs/4
loglevel=WARNING
posttroll_port=12234
nameservers=false
alias_platform_name = noaa18:NOAA-18|noaa19:NOAA-19
history=10"""
    config_file = tmp_path / "config.ini"
    with open(config_file, "w") as fd:
        fd.write(config)
    return config_file


@pytest.fixture
def messages_from_observer(config_file):
    """Create an observer and yield the messages it published."""
    from posttroll.testing import patched_publisher
    with patched_publisher() as messages:
        obs = start_observer(["-c", os.fspath(config_file), "-C", "noaa_hrpt"])
        time.sleep(LAG_SECONDS)
        yield messages
        stop_observer(obs)


def test_trollstalker(messages_from_observer, dir_to_watch):
    """Test trollstalker functionality."""
    subdir_to_watch = dir_to_watch / "new_dir"
    os.mkdir(subdir_to_watch)

    trigger_file = subdir_to_watch / "hrpt_noaa18_20230524_1017_10101.l1b"
    with open(trigger_file, "w") as fd:
        fd.write("hej")
    time.sleep(LAG_SECONDS)
    message = messages_from_observer[0]
    assert message.startswith("pytroll://HRPT/l1b/dev/mystation file ")
    message = Message(rawstr=message)

    assert message.data['platform_name'] == "NOAA-18"
    assert message.data['uri'] == os.fspath(trigger_file)


def test_trollstalker_monitored_directory_is_created(messages_from_observer, dir_to_watch):
    """Test that monitored directories are created."""
    trigger_file = dir_to_watch / "hrpt_noaa18_20230524_1017_10101.l1b"
    with open(trigger_file, "w") as fd:
        fd.write("hej")
    time.sleep(LAG_SECONDS)
    assert os.path.exists(dir_to_watch)


def test_trollstalker_handles_moved_files(messages_from_observer, dir_to_watch, tmp_path):
    """Test that trollstalker detects moved files."""
    filename = "hrpt_noaa18_20230524_1017_10101.l1b"
    trigger_file = tmp_path / filename
    with open(trigger_file, "w") as fd:
        fd.write("hej")
    os.rename(trigger_file, dir_to_watch / filename)
    time.sleep(LAG_SECONDS)
    assert len(messages_from_observer) == 1
    assert messages_from_observer[0].startswith("pytroll://HRPT/l1b/dev/mystation file ")


def test_event_names_are_deprecated(config_file):
    """Test that trollstalker detects moved files."""
    with open(config_file, "a") as fd:
        fd.write("\nevent_names=IN_CLOSE_WRITE,IN_MOVED_TO,IN_CREATE\n")
    with pytest.deprecated_call():
        obs = start_observer(["-c", os.fspath(config_file), "-C", "noaa_hrpt"])
        stop_observer(obs)
