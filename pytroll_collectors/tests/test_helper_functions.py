"""Unit testing some general purpose helper functions."""

from datetime import datetime

from pytroll_collectors.helper_functions import create_aligned_datetime_var


def test_create_aligned_datetime_var():
    """Test the create_aligned_datetime_var function."""
    # Run
    filepattern = "{start_time:%Y%m%d%H%M%S|align(5)}"
    result = create_aligned_datetime_var(filepattern,
                                         {'start_time':
                                          datetime(2015, 1, 9, 17, 10, 48)})
    # Assert
    assert result == datetime(2015, 1, 9, 17, 10, 0)

    # Run
    result = create_aligned_datetime_var(filepattern,
                                         {'start_time':
                                          datetime(2015, 1, 9, 17, 3, 0)})
    # Assert
    assert result == datetime(2015, 1, 9, 17, 0, 0)

    # Run
    result = create_aligned_datetime_var(filepattern,
                                         {'start_time':
                                          datetime(2015, 1, 9, 16, 59, 0)})
    # Assert
    assert result == datetime(2015, 1, 9, 16, 55, 0)

    # Run
    filepattern = "{start_time:%Y%m%d%H%M%S|align(15)}"

    result = create_aligned_datetime_var(filepattern,
                                         {'start_time':
                                          datetime(2015, 1, 9, 16, 59, 0)})
    # Assert
    assert result == datetime(2015, 1, 9, 16, 45, 0)


def test_create_aligned_datetime_var_offsets():
    """Test the create_aligned_datetime_var function."""
    # Run
    filepattern = "{start_time:%Y%m%d%H%M%S|align(15,-2)}"
    result = create_aligned_datetime_var(filepattern,
                                         {'start_time':
                                          datetime(2015, 1, 9, 16, 59, 0)})
    # Assert
    assert result == datetime(2015, 1, 9, 17, 0, 0)

    filepattern = "{start_time:%Y%m%d%H%M%S|align(15,2)}"
    result = create_aligned_datetime_var(filepattern,
                                         {'start_time':
                                          datetime(2015, 1, 9, 17, 16, 0)})
    # Assert
    assert result == datetime(2015, 1, 9, 17, 0, 0)


def test_parse_time_with_timeslot_aligment_intervals_add():
    """Test the create_aligned_datetime_var function."""
    # Run
    filepattern = "{start_time:%Y%m%d%H%M%S|align(15,0,1)}"
    result = create_aligned_datetime_var(filepattern,
                                         {'start_time':
                                          datetime(2015, 1, 9, 16, 59, 0)})
    # Assert
    assert result == datetime(2015, 1, 9, 17, 0, 0)

    # Run
    filepattern = "{start_time:%Y%m%d%H%M%S|align(15,0,2)}"
    result = create_aligned_datetime_var(filepattern,
                                         {'start_time':
                                          datetime(2015, 1, 9, 16, 59, 0)})
    # Assert
    assert result == datetime(2015, 1, 9, 17, 15, 0)

    # Run
    filepattern = "{start_time:%Y%m%d%H%M%S|align(15,0,-1)}"
    result = create_aligned_datetime_var(filepattern,
                                         {'start_time':
                                          datetime(2015, 1, 9, 16, 59, 0)})
    # Assert
    assert result == datetime(2015, 1, 9, 16, 30, 0)

