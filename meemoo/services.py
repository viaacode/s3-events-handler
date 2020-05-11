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
from viaa.configuration import ConfigParser
from viaa.observability import logging

# Local imports

# Get logger
config = ConfigParser()
log = logging.get_logger(__name__, config=config)


class Service(object):
    """The base Service object
    TODO: use the factory pattern for service creation
    """

    def __init__(self, ctx):
        self.name = "noname" if not self.name else self.name
        self.ctx = ctx
        self.config = ctx.config.app_cfg
        self.host = self._get_service_host()

    def _get_service_host(self):
        host = None
        try:
            host = self.config[self.name]["host"]
        except KeyError as e:
            log.warning("Oeps")
        else:
            log.debug(f"Host: {host}")
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
        self.name = "pid-service"
        super().__init__(ctx)

    def get_pid(self) -> str:
        if self.ctx.dryrun:
            pid = "a1b2c3d4e5"
        else:
            resp = requests.get(self.host)
            log.debug(f"Response is: {resp.raw}")
            pid = resp.json()[0]["id"]
        return pid


class OrganisationsService(Service):
    """ Abstraction for the organisations-api. """

    def __init__(self, ctx):
        self.name = "organisations-api"
        super().__init__(ctx)

    def get_organisation(self, or_id):
        # Make sure the OR is uppercase. Organisation api needs it.
        or_id = or_id[:2].upper() + or_id[2:]

        response = requests.get(f"{self.host}org/{or_id}")
        organisation = response.json()["data"]
        return organisation


# vim modeline
# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4
