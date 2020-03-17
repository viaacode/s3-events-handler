#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
#  meemoo/services.py
#  
#  Copyleft 2020 meemoo
#  
#  @author: Maarten De Schrijver
#  

# System imports
import logging
import json
# Third-party imports
import requests
# Local imports


# Get logger
log = logging.getLogger('nano-bd')


class Service(object):
    """The base Service object
    TODO: use the factory pattern for service creation
    """
    def __init__(self, ctx):
        self.name   = 'noname' if not self.name else self.name
        self.ctx    = ctx
        self.host   = self._get_service_host() 

    def _get_service_host(self):
        host = None
        try:
            host = self.ctx.config[self.name]['host']
        except KeyError as e:
            log.warn('Oeps')
        else:
            log.debug(f'Host: {host}')
        return host
    
class PIDService(Service):
    """Abstraction to the pid-generating service.

    See: https://github.com/viaacode/pid_webservice

    The service returns a JSON as such:
    ```json
    [
        {
            "id": "j96059k22s", 
            "number": 1
        }
    ]
    ```
    """
    # TODO: implement PID validation:
    # regex: [a-z0-9]{10}
    # See: issue in Jira
    def __init__(self, ctx):
        self.name   = 'pid-service'
        super().__init__(ctx)
    
    def get_pid(self) -> str:
        if self.ctx.dryrun:
            pid = 'a1b2c3d4e5'
        else:
            resp = requests.get(self.host)
            log.debug(f'Response is: {resp.json()}')
            pid = resp.json()[0]["id"]
        return pid


class FileTransferService(Service):
    """Abstraction to the generic file transfer service.
    
    See: <repo-url>
    """
    def __init__(self, ctx):
        self.name   = 'file-transfer-service'
        super().__init__(ctx)

    def validate_params(self, param_dict):
        """Check if the request conforms to the service's API."""
        return True

    def send_transfer_request(self, param_dict):
        try:
            self.validate_params(param_dict)
        except ValidationError as e:
            log.error('ValidationError')
            raise
        else:
            #resp = requests.post(self.host, json.dumps(param_dict))
            log.debug(f'Posting to {self.host}')
        return True


class OrganisationsService(Service):
    """ Abstraction for the organisations-api. """
    def __init__(self, ctx):
        self.name   = 'organisations-api'
        super().__init__(ctx)

    def get_organisation(self, or_id):
            resp = requests.get(f"{self.host}/{or_id}")

            organisation = resp.json()["data"]


# vim modeline
# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4
