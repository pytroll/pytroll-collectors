"""Fixtures for unittests."""

import pytest


TEST_YAML_CONFIG_CONTENT_SCISYS_RECEIVER = """
# Publish topic
publish_topic_pattern: '/{sensor}/{format}/{data_processing_level}/{platform_name}'

# It is possible to here add a static postfix topic if needed:
topic_postfix: "my/cool/postfix/topic"

host: merlin
port: 10600
station: nrk
environment: dev

excluded_satellites:
  - fy3d

"""


@pytest.fixture
def fake_yamlconfig_file_for_scisys_receiver(tmp_path):
    """Write fake yaml config file for the SCISYS receiver."""
    file_path = tmp_path / 'test_scisys_receiver_config.yaml'
    with open(file_path, 'w') as fpt:
        fpt.write(TEST_YAML_CONFIG_CONTENT_SCISYS_RECEIVER)

    yield file_path
