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
# Third-party imports
import pika
from viaa.configuration import ConfigParser
from viaa.observability import logging
# Local imports

# Get logger
config = ConfigParser()
log = logging.get_logger(__name__, config=config)

# Constants
EVENT_SERVICES = {
    'incoming_s3_events': {
        'exchange': 's3.ingest',
        'queue_name': 's3.ingest'
    },
    'outgoing_filetransfer': {
        'queue_name': 's3_to_remotefs'
    }
}
    
class Events(object):
    def __init__(self, name, ctx):
        self.name           = name
        self.ctx            = ctx
        self.queue          = self._determine_queue()
        self.exchange       = self._determine_exchange()
        self.credentials    = self._init_credentials()
        self.connection     = self._init_connection()
        self.channel        = self._init_channel()
        self._declare_queue()

    def _determine_queue(self):
        """"""
        assert self.name in EVENT_SERVICES, f'{self.name} not known as a event service'
        queue = EVENT_SERVICES[self.name]['queue_name']
        return queue

    def _determine_exchange(self):
        """"""
        assert self.name in EVENT_SERVICES, f'{self.name} not known as a event service'
        exchange = EVENT_SERVICES[self.name].get('exchange', '')
        return exchange
        
    def _init_credentials(self):
        """"""
        config = self.ctx.config.app_cfg["rabbitmq"]
        credentials = pika.PlainCredentials(config["user"], config["passwd"])
        return credentials
        
    def _init_connection(self):
        """"""
        config = self.ctx.config.app_cfg["rabbitmq"]
        host = config['host']
        port = config['port']
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
        self.channel.queue_declare(queue=self.queue, durable=True)

        if (self.exchange):        
            self.channel.exchange_declare(exchange=self.exchange, exchange_type='direct')
            self.channel.queue_bind(exchange=self.exchange, queue=self.queue, routing_key=self.queue)

        # Turn on delivery confirmations if need be
        #~ channel.confirm_delivery()

    def basic_consume(self):
        return self.channel.basic_consume(
            queue=self.queue,
            # TODO
        )

    def publish(self, message):
        log.debug(f"message to filetransfer {self.exchange}: {message}")
        self.channel.basic_publish(
            exchange=self.exchange, 
            routing_key=self.queue,
            body=message,
            properties=pika.BasicProperties(
                delivery_mode=2,  # make message persistent
            )
        )
        
    def get_channel(self):
        return self.channel




# vim modeline
# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4
