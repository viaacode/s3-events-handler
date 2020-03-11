#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
#  unit_tests.py
#
#  Copyleft 2018 VIAA vzw
#  <admin@viaa.be>
#
#  @author: https://github.com/maartends
#
#######################################################################
#
#  ./tests/unit_tests.py
#
#######################################################################

import json
from meemoo.helpers import get_from_event
from tests.resources import S3_MOCK_EVENT

event = json.loads(S3_MOCK_EVENT)

get_from_event(event, 'host')
get_from_event(event, 'object_key')
get_from_event(event, 'tenant')
get_from_event(event, 'bucket')


ALLOWED_NODES = ['Dynamic', 'Technical']
XML_ENCODING  = 'UTF-8'
MHS_VERSION   = '19.4'
MH_NAMESPACES = {
    "mhs": f"https://zeticon.mediahaven.com/metadata/{MHS_VERSION}/mhs/",
    "mh":  f"https://zeticon.mediahaven.com/metadata/{MHS_VERSION}/mh/"
}

for k in metadata_dict:
    assert k in ALLOWED_NODES, f'Unknown sidecar node: "{k}"'

from lxml import etree

# Create the root element
root = etree.Element("{%s}%s" % (MH_NAMESPACES['mhs'], 'Sidecar'),
                     version=MHS_VERSION,
                     nsmap=MH_NAMESPACES)
# Make a new document tree
doc = etree.ElementTree(root)
# Add the subelements
for top in metadata_dict:
    # Can't we use f-strings? With curly braces?
    node = etree.SubElement(root, "{%s}%s" % (MH_NAMESPACES['mhs'], top))
    for sub, val in metadata_dict[top].items():
        etree.SubElement(node, "{%s}%s" % (MH_NAMESPACES['mh'], sub)).text = val

print(etree.tostring(doc, pretty_print=True,
                           encoding=XML_ENCODING,
                           xml_declaration=True).decode('utf-8'))


# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4 smartindent
