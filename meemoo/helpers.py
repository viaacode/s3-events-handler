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
# Local imports


# Get logger
config = ConfigParser()
log = logging.get_logger(__name__, config=config)

# Constants
BASE_DOMAIN = 'viaa.be'

def get_from_event(event, name):
    keys = ['bucket', 'object_key', 'domain', 'tenant', 'user', 'md5']
    assert name in keys, f'Unknown key: "{name}"'
    if name == 'bucket':
        return event['Records'][0]['s3']['bucket']['name']
    elif name == 'domain':
        return event['Records'][0]['s3']['domain']['name']
    elif name == 'object_key':
        return event['Records'][0]['s3']['object']['key']
    elif name =='tenant':
        return event['Records'][0]['s3']['bucket']['metadata']['tenant']
    elif name =='user':
        return event['Records'][0]['userIdentity']['principalId']
    elif name =='md5':
        return event["Records"][0]["s3"]["object"]["metadata"]["x-md5sum-meta"]


class SidecarBuilder(object):
    """SidecarBuilder constructs an XML sidecar compliant to the MediaHaven
    metadata model. The resulting XML (as a string) can obtained via a call to
    `to_string`.
    MediaHaven's documentation: https://mediahaven.atlassian.net/wiki/spaces/CS/pages/488964146/Metadata+Sidecar
    """
    ALLOWED_NODES = ['Dynamic', 'Technical']
    XML_ENCODING  = 'UTF-8'
    MHS_VERSION   = '19.2'
    MH_NAMESPACES = {
        "mhs": f"https://zeticon.mediahaven.com/metadata/{MHS_VERSION}/mhs/",
        "mh":  f"https://zeticon.mediahaven.com/metadata/{MHS_VERSION}/mh/"
    }
    #
    def __init__(self, ctx=None):
        self.sidecar    = None
        self.ctx        = ctx
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
        root = etree.Element("{%s}%s" % (self.MH_NAMESPACES['mhs'], 'Sidecar'),
                             version=self.MHS_VERSION,
                             nsmap=self.MH_NAMESPACES)
        # Make a new document tree
        doc = etree.ElementTree(root)   # NEEDED?
        # Add the subelements
        for top in metadata_dict:
            # Can't we use f-strings? With curly braces?
            node = etree.SubElement(root, "{%s}%s" % (self.MH_NAMESPACES['mhs'], top))
            # TODO: the subnodes under 'Dynamic' should not be namespaced!
            for sub, val in metadata_dict[top].items():
                if top == 'Technical':
                    etree.SubElement(node, "{%s}%s" % (self.MH_NAMESPACES['mh'], sub)).text = val
                if top == 'Dynamic':
                    etree.SubElement(node, "%s" % sub).text = val

        self.sidecar = doc
    #
    def to_bytes(self, pretty=False) -> bytes:
        return etree.tostring(self.sidecar, pretty_print=pretty,
                           encoding=self.XML_ENCODING,
                           xml_declaration=True)
    #
    def to_string(self, pretty=False) -> str:
        return etree.tostring(self.sidecar, pretty_print=pretty,
                           encoding=self.XML_ENCODING,
                           xml_declaration=True).decode('utf-8')

class FTP(object):
    """Abstraction for FTP"""
    def __init__(self, ctx=None):
        self.ctx        = ctx
        self.host       = self.__set_host()
        self.conn       = self.__connect()
    #

    def __set_host(self):
        """"""
        host = self.ctx.config.app_cfg['mediahaven']['ftp']['host']
        parts = urlparse(host)
        log.debug(f'FTP: scheme={parts.scheme}, host={parts.netloc}')
        return parts.netloc


    def __connect(self):
        config = self.ctx.config.app_cfg
        ftp_user = config['mediahaven']['ftp']['user']
        ftp_passwd = config['mediahaven']['ftp']['passwd']

        try:
            conn = BuiltinFTP(host=self.host, user=ftp_user, passwd=ftp_passwd)
        except Exception as e:
            log.error(e)
            raise
        else:
            log.debug(f'Succesfully established connection to {self.host}')
            return conn

    def put(self, content_bytes, destination_path, destination_filename):
        log.debug(f'Putting {destination_filename} to {destination_path} on {self.host}')
        with self.conn as conn:
            try:
                conn.cwd(destination_path)
                stor_cmd = f'STOR {destination_filename}'
                conn.storbinary(stor_cmd, BytesIO(content_bytes))
            except Exception as exception:
                log.critical(f"Failed to put sidecar on {self.host} {destination_path}")




# vim modeline
# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4
