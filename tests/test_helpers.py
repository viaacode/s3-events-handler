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
from meemoo.helpers import get_from_event
from tests.resources import S3_MOCK_ESSENCE_EVENT


class TestHelperFunctions(unittest.TestCase):
    def test_get_from_event(self):
        event_dict = json.loads(S3_MOCK_ESSENCE_EVENT)
        bucket = get_from_event(event_dict, "bucket")
        self.assertEqual(bucket, "MAM_HighresVideo")
        event_name = get_from_event(event_dict, "event_name")
        self.assertEqual(event_name, "ObjectCreated:Put")


if __name__ == "__main__":
    unittest.main()


# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4 smartindent
