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
from meemoo.helpers import get_from_event, normalize_or_id, is_event_valid, InvalidEventException
from tests.resources import S3_MOCK_ESSENCE_EVENT, S3_MOCK_INVALID_EVENT, S3_MOCK_ESSENCE_EVENT_WITHOUT_MD5


class TestHelperFunctions(unittest.TestCase):
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

    def test_is_event_valid_with_valid_event(self):
        # Arrange
        event = json.loads(S3_MOCK_ESSENCE_EVENT)
        # Act and Assert
        is_event_valid(event)

    def test_is_event_valid_with_valid_event_no_md5(self):
        # Arrange
        event = json.loads(S3_MOCK_ESSENCE_EVENT_WITHOUT_MD5)
        # Act and Assert
        is_event_valid(event)

    def test_is_event_valid_with_invalid_event(self):
        # Arrange
        event = json.loads(S3_MOCK_INVALID_EVENT)
        # Act and Assert
        with self.assertRaises(InvalidEventException) as error:
            is_event_valid(event)

if __name__ == "__main__":
    unittest.main()


# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4 smartindent
