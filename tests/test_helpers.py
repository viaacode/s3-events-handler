#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
#  tests/test_helpers.py
#
#  Copyleft 2018 VIAA vzw
#  <admin@viaa.be>
#
#  @author: https://github.com/maartends
#
#######################################################################
#
#  tests/test_helpers.py
#
#######################################################################

import json

# External imports
import pytest

# Internal imports
from meemoo.helpers import (
    try_to_find_md5,
    get_from_event,
    normalize_or_id,
    is_event_valid,
    InvalidEventException,
    get_destination_for_cp,
    make_url
)
from tests.resources import (
    S3_MOCK_ESSENCE_EVENT,
    S3_MOCK_INVALID_EVENT,
    S3_MOCK_ESSENCE_EVENT_WITHOUT_MD5,
)


def test_try_to_find_md5_valid_x_md5sum_meta():
    # Arrange
    metadata = {"x-md5sum-meta": "1234abcd1234abcd1234abcd1234abcd"}
    # Act
    md5 = try_to_find_md5(metadata)
    # Assert
    assert md5 == metadata["x-md5sum-meta"]


def test_try_to_find_md5_valid_md5sum():
    # Arrange
    metadata = {"md5sum": "1234abcd1234abcd1234abcd1234abcd"}
    # Act
    md5 = try_to_find_md5(metadata)
    # Assert
    assert md5 == metadata["md5sum"]


def test_try_to_find_md5_valid_x_amz_meta_md5sum():
    # Arrange
    metadata = {"x-amz-meta-md5sum": "1234abcd1234abcd1234abcd1234abcd"}
    # Act
    md5 = try_to_find_md5(metadata)
    # Assert
    assert md5 == metadata["x-amz-meta-md5sum"]


def test_get_from_event():
    event_dict = json.loads(S3_MOCK_ESSENCE_EVENT)
    bucket = get_from_event(event_dict, "bucket")
    assert bucket == "MAM_HighresVideo"
    event_name = get_from_event(event_dict, "event_name")
    assert event_name == "ObjectCreated:Put"
    or_id = get_from_event(event_dict, "tenant")
    assert or_id == "OR-rf5kf25"


def test_normalize_or_id_valid():
    # Arrange
    or_id = "or-a1b2c3d"
    # Act
    normalized_or_id = normalize_or_id(or_id)
    # Assert
    assert normalized_or_id == "OR-a1b2c3d"


def test_normalize_or_id_valid_random():
    # Arrange
    or_id = "OR-7p8tc89"
    # Act
    normalized_or_id = normalize_or_id(or_id)
    # Assert
    assert normalized_or_id == "OR-7p8tc89"


def test_normalize_or_id_valid_diff_prefix():
    # Arrange
    or_id = "mm-7p8tc89"
    # Act
    normalized_or_id = normalize_or_id(or_id)
    # Assert
    assert normalized_or_id == "MM-7p8tc89"


def test_normalize_or_id_invalid_noid_length_too_short():
    # Arrange
    or_id = "or-a1b2c3"
    # Act and Assert
    with pytest.raises(ValueError) as excinfo:
        normalize_or_id(or_id)
    assert "Invalid noid length" in str(excinfo.value)


def test_normalize_or_id_invalid_noid_length_too_long():
    # Arrange
    or_id = "or-a1b2c3da1b2c3d"
    # Act and Assert
    with pytest.raises(ValueError) as excinfo:
        normalize_or_id(or_id)
    assert "Invalid noid length" in str(excinfo.value)


def test_normalize_or_id_invalid_no_hyphen_seperator():
    # Arrange
    or_id = "or_a1b2c3d"
    # Act and Assert
    with pytest.raises(ValueError) as excinfo:
        normalize_or_id(or_id)
        assert "Could not split" in str(excinfo.value)


def test_is_event_valid_with_valid_event():
    # Arrange
    event = json.loads(S3_MOCK_ESSENCE_EVENT)
    # Act and Assert
    is_event_valid(event)


def test_is_event_valid_with_valid_event_no_md5():
    # Arrange
    event = json.loads(S3_MOCK_ESSENCE_EVENT_WITHOUT_MD5)
    # Act and Assert
    is_event_valid(event)


def test_is_event_valid_with_invalid_event():
    # Arrange
    event = json.loads(S3_MOCK_INVALID_EVENT)
    # Act and Assert
    with pytest.raises(InvalidEventException):
        is_event_valid(event)


def test_is_event_valid_missing_field():
    # Arrange
    event_dict = json.loads(S3_MOCK_ESSENCE_EVENT)
    event_dict["Records"][0]["s3"].pop("bucket")
    # Act and Assert
    with pytest.raises(InvalidEventException) as excinfo:
        is_event_valid(event_dict)
    assert "Not all fields are present" in str(excinfo.value)


def test_is_event_valid_extra_fields():
    # Arrange
    event_dict = json.loads(S3_MOCK_ESSENCE_EVENT)
    event_dict["extra_field_1"] = "should"
    event_dict["Records"][0]["extra_field_2"] = "be"
    event_dict["Records"][0]["s3"]["extra_field_3"] = "ignored"
    # Act
    event_valid = is_event_valid(event_dict)
    # Assert
    assert event_valid is None


@pytest.mark.parametrize(
    "environment, cp, file_type, expected_destination",
    [
        ("production", "vrt", "essence", "TAPE-SHARE-EVENTS"),
        ("production", "vrt", "collateral", "DISK-SHARE-EVENTS"),
        ("production", "testbeeld", "essence", "DISK-SHARE-EVENTS"),
        ("qas", "vrt", "essence", "DISK-SHARE-EVENTS"),
        ("qas", "testbeeld", "essence", "DISK-SHARE-EVENTS"),
        ("xxxxx", "vrt", "essence", "DISK-SHARE-EVENTS"),
        ("production", "meemoo", "essence", "DISK-SHARE-EVENTS"),
        ("production", "vrt", "xxxxx", "DISK-SHARE-EVENTS"),
    ],
)
def test_file_destination(environment, cp, file_type, expected_destination):
    # Act
    destination = get_destination_for_cp(environment, cp, file_type)
    # Assert
    assert destination == expected_destination


def test_make_url_with_trailing_slash():
    # Arrange
    base_url = 'https://example-url/'
    # Act
    url = make_url(base_url, 'org', 'test-org-id')
    # Assert
    assert url == 'https://example-url/org/test-org-id'


def test_make_url_multiple_trailing_slashes():
    # Arrange
    base_url = 'https://example-url////'
    # Act
    url = make_url(base_url, 'org', 'test-org-id')
    # Assert
    assert url == 'https://example-url/org/test-org-id'


def test_make_url_missing_trailing_slash():
    # Arrange
    base_url = 'https://example-url'
    # Act
    url = make_url(base_url, 'org', 'test-org-id')
    # Assert
    assert url == 'https://example-url/org/test-org-id'


def test_make_url_long_path():
    # Arrange
    base_url = 'https://example-url'
    # Act
    url = make_url(base_url, 'api', 'deeply', 'nested', 'test', 'path')
    # Assert
    assert url == 'https://example-url/api/deeply/nested/test/path'


def test_make_url_none_base_url():
    # Arrange
    base_url = None
    # Act and assert
    with pytest.raises(TypeError) as excinfo:
        make_url(base_url, 'org', 'test-org-id')
    assert 'base_url is required' in str(excinfo.value)


def test_make_url_no_path():
    # Arrange
    base_url = 'https://example-url'
    # Act
    url = make_url(base_url)
    # Assert
    assert url == 'https://example-url'

# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4 smartindent
