#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
#  main.py
#
#  Copyleft 2018 VIAA vzw
#  <admin@viaa.be>
#
#  @author: https://github.com/maartends
#
#######################################################################
#
#  ./main.py
#
#  See README.md
#  
#######################################################################

import os
import sys
import json
import yaml
import logging
# 3d party imports
import pika
# Local imports
from meemoo.services import PIDService
from meemoo.services import FileTransferService
from meemoo.events import Events
from meemoo.helpers import SidecarBuilder, FTP, get_from_event
from meemoo import Context

# Get logger
log = logging.getLogger('nano-bd')
log.setLevel(logging.DEBUG)
# create handler and set level
ch = logging.StreamHandler(stream=sys.stdout)
#~ ch = logging.FileHandler('./logging.log', mode='a')
ch.setLevel(logging.DEBUG)
# create formatter
formatter = logging.Formatter('%(asctime)s - %(name)s - %(lineno)d - %(levelname)s - %(message)s')
# add formatter to ch
ch.setFormatter(formatter)
# add ch to logger
log.addHandler(ch)

# CONSTANTS
CFGFILE = 'config.yaml'


def construct_destination_path():
    #return '/export/home/OR-rf5kf25/incoming/borndigital'
    return '/tmp'   

def construct_fts_params_dict(event, dest_filename, dest_path, ctx):
    """"""
    return {
        "source": {
            "host": get_from_event(event, 'host'),
            "user": get_from_event(event, 'tenant'),
            "password": "password",
            "file": get_from_event(event, 'tenant'),
            "path": get_from_event(event, 'object_key'),
        }, 
        "destination": {
           "host": ctx.config['mediahaven']['ftp']['host'],
           "user": get_from_event(event, 'tenant'),
           "password": "password",
           "file": dest_filename,
           "path": dest_path
        },
        "move": False
    }


def get_event_from_bbody(body):
    """Event body is bytes. Return a dict."""
    return json.loads(body)

def event_handler(event, context):
    """Main event handler function"""
    # Get the bucket (which conveniently also is the organisation ID)
    bucket = get_from_event(event, 'bucket')

class MsgHandler(object):
    """"""
    def __init__(self, ctx):
        self.ctx    = ctx
    #

def callback(ch, method, properties, body, ctx):
    log.debug('Callback called')
    #~ print(" [x] Received %r" % body)
    #~ event = get_event_from_bbody(body)

    from tests.resources import S3_MOCK_EVENT
    event = json.loads(S3_MOCK_EVENT)
    # Debug the correlation_id
    log.debug(f'Correlation ID: {ctx.correlation_id}')
    # Get a pid from the PIDService
    pid_service     = PIDService(ctx)
    pid = pid_service.get_pid()
    log.info(f'PID received: {pid}')

    # Build the sidecar
    sidecar_builder = SidecarBuilder(ctx)
    from tests.resources import METADATA_DICT as metadata_dict
    sidecar_builder.build(metadata_dict)

    # Send the sidecar to TRA-server
    # Get the sidecar XML representation as bytes
    sidecar_xml = sidecar_builder.to_bytes(pretty=True)
    log.debug(sidecar_xml.decode('utf-8'))
    dest_path = construct_destination_path()
    dest_filename = f'{pid}.xml'
    log.debug(f'Destination: path={dest_path}, file_name={dest_filename}')
    ftp_host = ctx.config['mediahaven']['ftp']['host']
    ftp = FTP(ftp_host, ctx)
    ftp.put(sidecar_xml, dest_path, dest_filename)

    # Request file transfer
    dest_filename = 'a1b2c3d4e5.mxf'
    fts = FileTransferService(ctx)
    param_dict = construct_fts_params_dict(event, dest_filename, dest_path, ctx)
    log.debug(param_dict)
    result = fts.send_transfer_request(param_dict)


def main(ctx):
    events = Events('s3_events', ctx)
    channel = events.get_channel()
    channel.basic_consume(
        queue=events.queue,
        # Adapt callback fn to add in the ctx parameter
        on_message_callback=lambda ch, method, properties, body: callback(ch, method, properties, body, ctx),
        # Ack the message when the callback fn returns
        auto_ack=True
    )
    print(' [*] Waiting for messages. To exit press CTRL+C')
    channel.start_consuming()
    # TODO: override auto_ack 
    #~ events.basic_consume(
        #~ on_message_callback=callback
    #~ )


if __name__ == '__main__':
    with open(CFGFILE, 'r') as yamlfile:
        cfg = yaml.load(yamlfile, Loader=yaml.FullLoader)
    log.debug(f'Config read from {CFGFILE}')
    ctx = Context(cfg)
    main(ctx)


# vim modeline
# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4 smartindent
