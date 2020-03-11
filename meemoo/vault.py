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

# Third-party imports

# Local imports

SECRETS = {
    'LDAP_PASSWD': 'abc123_fromvault'
}

class Vault(object):
    def __init__(self):
        pass
    
    def is_available(self):
        return True

    def get_secret(self, key):
        return SECRETS[key]

vault = Vault()


# vim modeline
# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4
