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
import unittest
from meemoo.helpers import (
    try_to_find_md5,
    get_from_event,
    normalize_or_id,
    is_event_valid,
    InvalidEventException,
)
from tests.resources import S3_MOCK_ESSENCE_EVENT


class TestHelperFunctions(unittest.TestCase):
    def test_try_to_find_md5_valid_x_md5sum_meta(self):
        # Arrange
        metadata = {'x-md5sum-meta': '1234abcd1234abcd1234abcd1234abcd'}
        # Act
        md5 = try_to_find_md5(metadata)
        # Assert
        self.assertEqual(md5, metadata['x-md5sum-meta'])

    def test_try_to_find_md5_valid_md5sum(self):
        # Arrange
        metadata = {'md5sum': '1234abcd1234abcd1234abcd1234abcd'}
        # Act
        md5 = try_to_find_md5(metadata)
        # Assert
        self.assertEqual(md5, metadata['md5sum'])

    def test_try_to_find_md5_valid_x_amz_meta_md5sum(self):
        # Arrange
        metadata = {'x-amz-meta-md5sum': '1234abcd1234abcd1234abcd1234abcd'}
        # Act
        md5 = try_to_find_md5(metadata)
        # Assert
        self.assertEqual(md5, metadata['x-amz-meta-md5sum'])

    def test_try_to_find_md5_invalid_missing(self):
        # Arrange
        metadata = {}
        # Act and assert
        with self.assertRaises(InvalidEventException) as error:
            md5 = try_to_find_md5(metadata)
        self.assertTrue('not found or syntactically incorrect' in str(error.exception))

    def test_try_to_find_md5_invalid_md5sum_too_short(self):
        # Arrange
        metadata = {'md5sum': '1234abcd'}
        # Act and assert
        with self.assertRaises(InvalidEventException) as error:
            md5 = try_to_find_md5(metadata)
        self.assertTrue('not found or syntactically incorrect' in str(error.exception))

    def test_try_to_find_md5_invalid_md5sum_too_long(self):
        # Arrange
        metadata = {'md5sum': '1234abcd1234abcd1234abcd1234abcd1234abcd'}
        # Act and assert
        with self.assertRaises(InvalidEventException) as error:
            md5 = try_to_find_md5(metadata)
        self.assertTrue('not found or syntactically incorrect' in str(error.exception))

    def test_try_to_find_md5_invalid_md5sum_invalid_characters(self):
        # Arrange
        metadata = {'md5sum': '1234efgh1234efgh1234efgh1234efgh'}
        # Act and assert
        with self.assertRaises(InvalidEventException) as error:
            md5 = try_to_find_md5(metadata)
        self.assertTrue('not found or syntactically incorrect' in str(error.exception))

    def test_get_from_event(self):
        event_dict = json.loads(S3_MOCK_ESSENCE_EVENT)
        bucket = get_from_event(event_dict, "bucket")
        self.assertEqual(bucket, "MAM_HighresVideo")
        event_name = get_from_event(event_dict, "event_name")
        self.assertEqual(event_name, "ObjectCreated:Put")
        or_id = get_from_event(event_dict, "tenant")
        self.assertEqual(or_id, "OR-rf5kf25")

    def test_normalize_or_id_valid(self):
        # Arrange
        or_id = 'or-a1b2c3d'
        # Act
        normalized_or_id = normalize_or_id(or_id)
        # Assert
        self.assertEqual(normalized_or_id, 'OR-a1b2c3d')

    def test_normalize_or_id_valid_random(self):
        # Arrange
        or_id = 'OR-7p8tc89'
        # Act
        normalized_or_id = normalize_or_id(or_id)
        # Assert
        self.assertEqual(normalized_or_id, 'OR-7p8tc89')

    def test_normalize_or_id_valid_diff_prefix(self):
        # Arrange
        or_id = 'mm-7p8tc89'
        # Act
        normalized_or_id = normalize_or_id(or_id)
        # Assert
        self.assertEqual(normalized_or_id, 'MM-7p8tc89')

    def test_normalize_or_id_invalid_noid_length_too_short(self):
        # Arrange
        or_id = 'or-a1b2c3'
        # Act and Assert
        with self.assertRaises(ValueError) as error:
            normalized_or_id = normalize_or_id(or_id)
        self.assertTrue('Invalid noid length' in str(error.exception))

    def test_normalize_or_id_invalid_noid_length_too_long(self):
        # Arrange
        or_id = 'or-a1b2c3da1b2c3d'
        # Act and Assert
        with self.assertRaises(ValueError) as error:
            normalized_or_id = normalize_or_id(or_id)
        self.assertTrue('Invalid noid length' in str(error.exception))

    def test_normalize_or_id_invalid_no_hyphen_seperator(self):
        # Arrange
        or_id = 'or_a1b2c3d'
        # Act and Assert
        with self.assertRaises(ValueError) as error:
            normalized_or_id = normalize_or_id(or_id)
        self.assertTrue('Could not split' in str(error.exception))

    def test_is_event_valid_valid(self):
        # Arrange
        event_dict = json.loads(S3_MOCK_ESSENCE_EVENT)
        # Act
        event_valid = is_event_valid(event_dict)
        # Assert
        self.assertIsNone(event_valid)

    def test_is_event_valid_missing_field(self):
        # Arrange
        event_dict = json.loads(S3_MOCK_ESSENCE_EVENT)
        event_dict['Records'][0]['s3'].pop('bucket')
        # Act and assert
        with self.assertRaises(InvalidEventException) as error:
            event_valid = is_event_valid(event_dict)
        self.assertTrue('Not all fields are present' in str(error.exception))

    def test_is_event_valid_extra_fields(self):
        # Arrange
        event_dict = json.loads(S3_MOCK_ESSENCE_EVENT)
        event_dict['extra_field_1'] = 'should'
        event_dict['Records'][0]['extra_field_2'] = 'be'
        event_dict['Records'][0]['s3']['extra_field_3'] = 'ignored'
        # Act
        event_valid = is_event_valid(event_dict)
        # Assert
        self.assertIsNone(event_valid)

if __name__ == "__main__":
    unittest.main()


# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4 smartindent
