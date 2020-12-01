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
from meemoo.helpers import get_from_event, normalize_or_id
from tests.resources import S3_MOCK_ESSENCE_EVENT


class TestHelperFunctions(unittest.TestCase):
    def test_get_from_event(self):
        event_dict = json.loads(S3_MOCK_ESSENCE_EVENT)
        bucket = get_from_event(event_dict, "bucket")
        self.assertEqual(bucket, "MAM_HighresVideo")
        event_name = get_from_event(event_dict, "event_name")
        self.assertEqual(event_name, "ObjectCreated:Put")

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


if __name__ == "__main__":
    unittest.main()


# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4 smartindent
