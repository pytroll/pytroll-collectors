"""Test getting the yaml configurations from file."""

from pytroll_collectors.config import read_config


def test_get_yaml_configuration(fake_yamlconfig_file_for_scisys_receiver):
    """Test read and get the yaml configuration for the scisys receiver from file."""
    config = read_config(fake_yamlconfig_file_for_scisys_receiver)

    assert config['publish_topic_pattern'] == '/{sensor}/{format}/{data_processing_level}/{platform_name}'
    assert config['topic_postfix'] == 'my/cool/postfix/topic'
    assert config['host'] == 'merlin'
    assert isinstance(config['port'], int)
    assert config['port'] == 10600
    assert config['station'] == 'nrk'
    assert config['environment'] == 'dev'
    assert len(config['excluded_satellites']) == 1
    assert config['excluded_satellites'][0] == 'fy3d'
