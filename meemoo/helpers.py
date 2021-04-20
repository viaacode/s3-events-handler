#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
#  meemoo/helpers.py
#
#  Copyleft 2020 meemoo
#
#  @author: Maarten De Schrijver
#

# System imports
import os
from io import BytesIO
from ftplib import FTP as BuiltinFTP
from urllib.parse import urlparse

# Third-party imports
from viaa.configuration import ConfigParser
from viaa.observability import logging
from lxml import etree
import yaml

# Local imports


# Get logger
config = ConfigParser()
log = logging.get_logger(__name__, config=config)

# Constants
BASE_DOMAIN = "viaa.be"
S3_FIELDS = ["bucket", "object_key", "domain", "tenant", "user", "md5", "event_name"] 
REQUIRED_S3_FIELDS = ["bucket", "object_key", "domain", "tenant", "user", "event_name"] 

class InvalidEventException(Exception):
    """ Exception raised when not all required fields are present
    """

    def __init__(self, message, **kwargs):
        self.message = message
        self.kwargs = kwargs

def get_destination_for_cp(environment: str, cp_name: str, file_type: str):
    # Default location
    destination = "DISK-SHARE-EVENTS"

    with open(os.getcwd() + "/destination_config.yml", "r") as ymlfile:
        destinations = yaml.load(ymlfile, Loader=yaml.FullLoader)
    
    try:
        destination = destinations[environment][cp_name][file_type]
    except KeyError as ke:
        log.info(f"No destination configured for content partner '{cp_name}' with type '{file_type}' in '{environment}'")

    return destination

def try_to_find_md5(object_metadata):
    """Simple convenience function that allows to be able to try different
    possible md5 field names (keys) and return it's value.
    Args:
        object_metadata (dict): The object metadata sub-section of the S3-event.
    Returns:
        str: The md5sum when found.
    Raises:
        KeyError: The md5sum was not found or is syntactically incorrect.
    """
    POSSIBLE_KEYS = ["x-md5sum-meta", "md5sum", "x-amz-meta-md5sum"]
    md5 = ""
    for key in POSSIBLE_KEYS:
        if key in object_metadata.keys():
            log.debug(f"md5sum located in metadata-field: '{key}'")
            md5 = object_metadata[key]
    return md5


def get_from_event(event, name):
    assert name in S3_FIELDS, f'Unknown key: "{name}"'
    record = event["Records"][0]
    if name == "bucket":
        return record["s3"]["bucket"]["name"]
    elif name == "domain":
        return record["s3"]["domain"]["name"]
    elif name == "object_key":
        return record["s3"]["object"]["key"]
    elif name == "tenant":
        return normalize_or_id(record["s3"]["bucket"]["metadata"]["tenant"])
    elif name == "user":
        return record["userIdentity"]["principalId"]
    elif name == "md5":
        return try_to_find_md5(record["s3"]["object"]["metadata"])
    elif name == "event_name":
        return record["eventName"]

def normalize_or_id(or_id):
    """Return a "normalized" version of the `OR-id`. This means:
    - a literal uppercase `OR`, followed by
    - a "hyphen-minus": `-` (U+002D), followed by
    - a random lowercase 7 charachter alfanumeric string.
    Eg., `OR-a1b2c3d`.

    Raises `ValueError` upon invalid input.
    """
    NOID_LENGTH = 7
    try:
        prefix, noid = or_id.split('-')
    except ValueError as e:
        raise ValueError(f'Could not split "{or_id}" by "-".')
    if len(noid) != NOID_LENGTH:
        raise ValueError(f'Invalid noid length for "{or_id}": != {NOID_LENGTH}: found {len(noid)}')
    return '-'.join((prefix.upper(), noid.lower()))

def is_event_valid(event):
    try:
        fields_present_in_event = [field for field in S3_FIELDS if get_from_event(event, field)]
        
        assert set(REQUIRED_S3_FIELDS).issubset(set(fields_present_in_event))
    except (AssertionError, KeyError, ValueError, InvalidEventException) as error:
        raise InvalidEventException("Not all fields are present in the event.", event=event, error=error)


class SidecarBuilder(object):
    """SidecarBuilder constructs an XML sidecar compliant to the MediaHaven
    metadata model. The resulting XML (as a string) can obtained via a call to
    `to_string`.
    MediaHaven's documentation: https://mediahaven.atlassian.net/wiki/spaces/CS/pages/488964146/Metadata+Sidecar
    """

    ALLOWED_NODES = ["Dynamic", "Technical"]
    XML_ENCODING = "UTF-8"
    MHS_VERSION = "19.2"
    MH_NAMESPACES = {
        "mhs": f"https://zeticon.mediahaven.com/metadata/{MHS_VERSION}/mhs/",
        "mh": f"https://zeticon.mediahaven.com/metadata/{MHS_VERSION}/mh/",
    }

    #
    def __init__(self, ctx=None):
        self.sidecar = None
        self.ctx = ctx

    #
    def check_metadata_dict(self, metadata_dict) -> bool:
        """"""
        for k in metadata_dict:
            assert k in self.ALLOWED_NODES, f'Unknown sidecar node: "{k}"'

    #
    def build(self, metadata_dict) -> None:
        """"""
        self.check_metadata_dict(metadata_dict)
        # Create the root element: Sidecar
        root = etree.Element(
            "{%s}%s" % (self.MH_NAMESPACES["mhs"], "Sidecar"),
            version=self.MHS_VERSION,
            nsmap=self.MH_NAMESPACES,
        )
        # Make a new document tree
        doc = etree.ElementTree(root)  # NEEDED?
        # Add the subelements
        for top in metadata_dict:
            # Can't we use f-strings? With curly braces?
            node = etree.SubElement(root, "{%s}%s" % (self.MH_NAMESPACES["mhs"], top))
            # TODO: the subnodes under 'Dynamic' should not be namespaced!
            for sub, val in metadata_dict[top].items():
                if top == "Technical":
                    etree.SubElement(
                        node, "{%s}%s" % (self.MH_NAMESPACES["mh"], sub)
                    ).text = val
                if top == "Dynamic":
                    etree.SubElement(node, "%s" % sub).text = val

        self.sidecar = doc

    #
    def to_bytes(self, pretty=False) -> bytes:
        return etree.tostring(
            self.sidecar,
            pretty_print=pretty,
            encoding=self.XML_ENCODING,
            xml_declaration=True,
        )

    #
    def to_string(self, pretty=False) -> str:
        return etree.tostring(
            self.sidecar,
            pretty_print=pretty,
            encoding=self.XML_ENCODING,
            xml_declaration=True,
        ).decode("utf-8")


class FTP(object):
    """Abstraction for FTP"""

    def __init__(self, ctx=None):
        self.ctx = ctx
        self.host = self.__set_host()
        self.conn = self.__connect()

    #

    def __set_host(self):
        """"""
        host = self.ctx.config.app_cfg["mediahaven"]["ftp"]["host"]
        parts = urlparse(host)
        log.debug(f"FTP: scheme={parts.scheme}, host={parts.netloc}")
        return parts.netloc

    def __connect(self):
        config = self.ctx.config.app_cfg
        ftp_user = config["mediahaven"]["ftp"]["user"]
        ftp_passwd = config["mediahaven"]["ftp"]["passwd"]

        try:
            conn = BuiltinFTP(host=self.host, user=ftp_user, passwd=ftp_passwd)
        except Exception as e:
            log.error(e)
            raise e
        else:
            log.debug(f"Succesfully established connection to {self.host}")
            return conn

    def put(self, content_bytes, destination_path, destination_filename):
        log.debug(
            f"Putting {destination_filename} to {destination_path} on {self.host}"
        )
        with self.conn as conn:
            try:
                conn.cwd(destination_path)
                stor_cmd = f"STOR {destination_filename}"
                conn.storbinary(stor_cmd, BytesIO(content_bytes))
            except Exception as e:
                log.critical(f"Failed to put sidecar on {self.host} {destination_path}")
                raise e


def make_url(base_url, *path):
    """Creates a valid url.

    Args:
        base_url (string): The base url.
        *path (variable length argument list, optional):
            The path to be appended to the base url.

    Returns:
        url (string): The created url.

    Raises a TypeError if base_url is None.
    """

    if base_url is None:
        raise TypeError('base_url is required')
    url = base_url.rstrip('/')  # Remove optional trailing slash
    for item in path:
        url = f"{url}/{item}"
    return url

# vim modeline
# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4
