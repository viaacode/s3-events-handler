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

# 3d party imports
import pika
from viaa.configuration import ConfigParser
from viaa.observability import logging
from lxml import etree

# Local imports
from meemoo.services import PIDService
from meemoo.services import OrganisationsService
from meemoo.events import Events
from meemoo.helpers import SidecarBuilder, FTP, get_from_event
from meemoo import Context

config = ConfigParser()
log = logging.get_logger(__name__, config=config)


def construct_destination_path(cp_name, ctx):
    return f"/{cp_name}/{ctx.config.app_cfg['destination-folder']}"


def construct_fts_params_dict(event, pid, file_extension, dest_path, ctx):
    """"""
    return {
        "source": {
            "domain": {"name": get_from_event(event, "domain")},
            "bucket": {"name": get_from_event(event, "bucket")},
            "object": {"key": get_from_event(event, "object_key")},
        },
        "destination": {
            "path": f"/mnt/STORAGE/INGEST/SIDECAR{dest_path}/{pid}{file_extension}",
            "host": ctx.config.app_cfg["mediahaven"]["ftp"]["host"],
        },
    }


def callback(ch, method, properties, body, ctx):
    try:
        event = json.loads(body)
    except json.JSONDecodeError as error:
        log.warning("Bad s3 event.", error=str(error))
        return

    # Get a pid from the PIDService
    pid_service = PIDService(ctx)
    pid = pid_service.get_pid()
    log.info(f"PID received: {pid}")

    # # Build the sidecar
    # sidecar_builder = SidecarBuilder(ctx)
    # log.debug(f"Item md5: {event['Records'][0]['s3']['object']['metadata']['x-md5sum-meta']}")
    # metadata_dict = {
    #     "Dynamic": {
    #         "s3_object_key": event["Records"][0]["s3"]["object"]["key"],
    #         "s3_bucket": event["Records"][0]["s3"]["bucket"]["name"],
    #         "PID": pid
    #     },
    #     "Technical": {
    #         "Md5": event["Records"][0]["s3"]["object"]["metadata"]["x-md5sum-meta"]
    #     }
    # }

    # sidecar_builder.build(metadata_dict)

    # # Send the sidecar to TRA-server
    # # Get the sidecar XML representation as bytes
    # sidecar_xml = sidecar_builder.to_bytes(pretty=True)
    # log.debug(sidecar_xml.decode('utf-8'))

    ###############################
    # DIRTY MEDIAHAVEN WORKAROUND #
    ###############################
    root = etree.Element("MediaHAVEN_external_metadata")
    etree.SubElement(root, "title").text = f"s3-test {pid}"

    mdprops = etree.SubElement(root, "MDProperties")

    etree.SubElement(mdprops, "CP").text = "VRT"
    etree.SubElement(mdprops, "CP_id").text = "OR-rf5kf25"
    etree.SubElement(mdprops, "PID").text = pid
    etree.SubElement(mdprops, "md5").text = get_from_event(event, "md5")
    etree.SubElement(mdprops, "s3_domain").text = get_from_event(event, "domain")
    etree.SubElement(mdprops, "s3_bucket").text = get_from_event(event, "bucket")
    etree.SubElement(mdprops, "s3_object_key").text = get_from_event(
        event, "object_key"
    )
    etree.SubElement(mdprops, "s3_object_owner").text = get_from_event(event, "user")

    tree = etree.ElementTree(root)
    sidecar_xml = etree.tostring(
        root, pretty_print=True, encoding="UTF-8", xml_declaration=True
    )

    ###############################

    or_id = get_from_event(event, "tenant")
    org_service = OrganisationsService(ctx)
    cp_name = org_service.get_organisation(or_id)["cp_name_mam"]

    dest_path = construct_destination_path(cp_name, ctx)
    dest_filename = f"{pid}.xml"
    log.debug(f"Destination: path={dest_path}, file_name={dest_filename}")

    ftp = FTP(ctx)
    ftp.put(sidecar_xml, dest_path, dest_filename)

    # Request file transfer
    file_extension = os.path.splitext(event["Records"][0]["s3"]["object"]["key"])[1]
    param_dict = construct_fts_params_dict(event, pid, file_extension, dest_path, ctx)

    events = Events(ctx.config.app_cfg["rabbitmq"]["outgoing"], ctx)
    events.publish(json.dumps(param_dict), properties.correlation_id)

    return True


def main(ctx):
    events = Events(ctx.config.app_cfg["rabbitmq"]["incoming"], ctx)
    channel = events.get_channel()

    channel.basic_consume(
        queue=events.queue,
        # Adapt callback fn to add in the ctx parameter
        on_message_callback=lambda ch, method, properties, body: callback(
            ch, method, properties, body, ctx
        ),
        # Ack the message when the callback fn returns
        auto_ack=True,
    )
    channel.start_consuming()


if __name__ == "__main__":
    ctx = Context(config)

    main(ctx)


# vim modeline
# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4 smartindent
