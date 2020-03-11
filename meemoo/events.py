#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
#  meemoo/events.py
#  
#  Copyleft 2020 meemoo
#  
#  @author: Maarten De Schrijver
#  

# System imports
import os
import logging
# Third-party imports
import pika
# Local imports

# Get logger
log = logging.getLogger('nano-bd')

# Constants
EVENT_SERVICES = {
    's3_events': {
        'queue_name': 's3-events'
    }
}
    
# Get some credentials from .env.secrets
log_fmt_str = 'Environment variable "%s" not present. Exiting.'
try:
    RABBIT_MQ_USER  = os.environ['RABBIT_MQ_USER']
except KeyError as e:
    log.error(log_fmt_str % 'RABBIT_MQ_USER')
    exit(1)
try:
    RABBIT_MQ_PASSWD = os.environ['RABBIT_MQ_PASSWD']
except KeyError as e:
    log.error(log_fmt_str % 'RABBIT_MQ_PASSWD')
    exit(1)


class Events(object):
    def __init__(self, name, ctx):
        self.name           = name
        self.ctx            = ctx
        self.queue          = self._determine_queue()
        self.credentials    = self._init_credentials()
        self.connection     = self._init_connection()
        self.channel        = self._init_channel()
        self._declare_queue()

    def _determine_queue(self):
        """"""
        assert self.name in EVENT_SERVICES, f'{self.name} not known as a event service'
        queue = EVENT_SERVICES[self.name]['queue_name']
        return queue
        
    def _init_credentials(self):
        """"""
        credentials = pika.PlainCredentials(RABBIT_MQ_USER, RABBIT_MQ_PASSWD)
        return credentials
        
    def _init_connection(self):
        """"""
        host = self.ctx.config['rabbitmq']['host']
        port = self.ctx.config['rabbitmq']['port']
        vhost = '/'
        parameters = pika.ConnectionParameters(host, port, vhost, self.credentials)
        connection = pika.BlockingConnection(parameters)
        return connection

    def _init_channel(self):
        """"""
        channel = self.connection.channel()
        channel.basic_qos(prefetch_count=1)
        return channel

    def _declare_queue(self):
        self.channel.queue_declare(queue=self.queue)
        # Turn on delivery confirmations if need be
        #~ channel.confirm_delivery()

    def basic_consume(self):
        return self.channel.basic_consume(
            queue=self.queue,
            # TODO
        )
        
    def get_channel(self):
        return self.channel




# vim modeline
# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4
