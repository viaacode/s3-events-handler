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
import functools
import json
import logging
import urllib
from typing import List, Tuple

# Third-party imports
import requests
from requests.auth import HTTPBasicAuth
from requests.exceptions import RequestException
from viaa.configuration import ConfigParser
from viaa.observability import logging

# Local imports

# Get logger
config = ConfigParser()
log = logging.get_logger(__name__, config=config)


class AuthenticationException(Exception):
    """Exception raised when authentication fails."""

    pass


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
        except KeyError:
            log.warning(
                f"The key 'host' not found in the config for service: {self.name}"
            )
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
            log.debug(f"Response is: {resp.json()}")
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


class MediahavenService(Service):
    """ Abstraction for the mediahaven-api. """

    def __init__(self, ctx):
        self.name = "mediahaven-api"
        self.token_info = None
        super().__init__(ctx)

    def __authenticate(function):
        @functools.wraps(function)
        def wrapper_authenticate(self, *args, **kwargs):
            if not self.token_info:
                self.token_info = self.__get_token()
            try:
                return function(self, *args, **kwargs)
            except AuthenticationException:
                self.token_info = self.__get_token()
            return function(self, *args, **kwargs)

        return wrapper_authenticate

    def __get_token(self) -> str:
        """Gets an OAuth token that can be used in mediahaven requests to authenticate."""
        user: str = self.config[self.name]["user"]
        password: str = self.config[self.name]["passwd"]
        url: str = f"{self.host}oauth/access_token"
        payload = {"grant_type": "password"}

        try:
            r = requests.post(
                url,
                auth=HTTPBasicAuth(user.encode("utf-8"), password.encode("utf-8")),
                data=payload,
            )

            if r.status_code != 201:
                raise RequestException(
                    f"Failed to get a token. Status: {r.status_code}"
                )
            token_info = r.json()
        except RequestException as e:
            raise e
        return token_info

    def _construct_headers(self) -> dict:
        return {
            "Authorization": f"Bearer {self.token_info['access_token']}",
            "Accept": "application/vnd.mediahaven.v2+json",
        }

    @__authenticate
    def get_fragment(self, query_params: List[Tuple[str, str]]) -> dict:
        headers: dict = self._construct_headers()
        url: str = f"{self.host}media"

        # Construct URL query parameters as "+(k1:v1 k2:v2)"
        query: str = f"+({' '.join([':'.join(k_v) for k_v in query_params])})"

        params_dict: dict = {
            "q": query,
            "nrOfResults": 1,
        }
        
        # Encode the spaces in the query parameters as %20 and not +
        params = urllib.parse.urlencode(params_dict, quote_via=urllib.parse.quote)

        # Send the GET request
        response = requests.get(url, headers=headers, params=params,)

        if response.status_code == 401:
            # AuthenticationException triggers a retry with a new token
            raise AuthenticationException(response.text)

        # If there is an HTTP error, raise it
        response.raise_for_status()

        return response.json()

    @__authenticate
    def update_metadata(self, fragment_id: str, sidecar: str) -> dict:
        headers: dict = self._construct_headers()

        # Construct the URL to POST to
        url: str = f"{self.host}media/{fragment_id}"

        data: dict = {"metadata": sidecar, "reason": "metadataUpdated"}

        # Send the POST request, as multipart/form-data
        response = requests.post(url, headers=headers, files=data)

        if response.status_code == 401:
            # AuthenticationException triggers a retry with a new token
            raise AuthenticationException(response.text)

        # If there is an HTTP error, raise it
        response.raise_for_status()

        return True


# vim modeline
# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4
