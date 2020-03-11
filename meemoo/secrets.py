#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
#  meemoo/secrets.py
#  
#  Copyleft 2020 meemoo
#  
#  @author: Maarten De Schrijver
#  

# System imports
import os
# Third-party imports

# Local imports
from .vault import vault

class Secrets(object):
    def __init__(self):
        pass

    def get(self, key):
        if vault.is_available():
            try:
                value = vault.get_secret(key)
            except KeyError as e:
                raise KeyError(f'Key {key} not available in Vault')
            else:
                return value
        else:
            try:
                value = os.environ[key]
            except KeyError as e:
                raise KeyError(f'Key {key} not available in the environment')
            else:
                return value

secrets = Secrets()


# vim modeline
# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4
