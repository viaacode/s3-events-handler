#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
#  meemoo/context.py
#
#  Copyleft 2020 meemoo
#
#  @author: Maarten De Schrijver
#

# System imports
import os

# Third-party imports

# Local imports


class Context(object):
    """The base Context object
    """

    def __init__(self, cfg=None):
        self.correlation_id = "abc123"
        self.dryrun = cfg.app_cfg.get("environment", "development") != "production"
        self.config = cfg


# vim modeline
# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4
