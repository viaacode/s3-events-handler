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

import json
import os
import sys

import yaml

# 3d party imports
import pika
from lxml import etree
from meemoo import Context
from meemoo.events import Events
from meemoo.helpers import FTP, SidecarBuilder, get_from_event

# Local imports
from meemoo.services import MediahavenService, OrganisationsService, PIDService
from viaa.configuration import ConfigParser
from viaa.observability import logging

config = ConfigParser()
log = logging.get_logger(__name__, config=config)


def construct_destination_path(cp_name, folder):
    return f"/{cp_name}/{folder}"


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


def construct_essence_sidecar(event, pid):
    s3_object_key = get_from_event(event, "object_key")

    root = etree.Element("MediaHAVEN_external_metadata")
    etree.SubElement(root, "title").text = f"Essence: pid: {pid}"

    description = f"""Main fragment for essence:
    - filename: {s3_object_key}
    - CP: VRT
    """
    etree.SubElement(root, "description").text = description

    mdprops = etree.SubElement(root, "MDProperties")
    etree.SubElement(mdprops, "CP").text = "VRT"
    etree.SubElement(mdprops, "CP_id").text = "OR-rf5kf25"
    etree.SubElement(mdprops, "sp_name").text = "s3"
    etree.SubElement(mdprops, "PID").text = pid
    etree.SubElement(mdprops, "md5").text = get_from_event(event, "md5")
    etree.SubElement(mdprops, "s3_domain").text = get_from_event(event, "domain")
    etree.SubElement(mdprops, "s3_bucket").text = get_from_event(event, "bucket")
    etree.SubElement(mdprops, "s3_object_key").text = s3_object_key
    etree.SubElement(mdprops, "dc_source").text = s3_object_key
    etree.SubElement(mdprops, "s3_object_owner").text = get_from_event(event, "user")

    tree = etree.ElementTree(root)
    return etree.tostring(
        root, pretty_print=True, encoding="UTF-8", xml_declaration=True
    )

def construct_collateral_sidecar(event, pid, media_id):
    s3_object_key = get_from_event(event, "object_key")

    root = etree.Element("MediaHAVEN_external_metadata")
    etree.SubElement(root, "title").text = f"Collateral: pid: {pid}"

    description = f"""Subtitles for essence:
    - filename: {s3_object_key}
    - CP: VRT
    """
    etree.SubElement(root, "description").text = description

    mdprops = etree.SubElement(root, "MDProperties")
    etree.SubElement(mdprops, "CP").text = "VRT"
    etree.SubElement(mdprops, "CP_id").text = "OR-rf5kf25"
    etree.SubElement(mdprops, "sp_name").text = "s3"
    etree.SubElement(mdprops, "PID").text = pid
    etree.SubElement(mdprops, "md5").text = get_from_event(event, "md5")
    etree.SubElement(mdprops, "s3_domain").text = get_from_event(event, "domain")
    etree.SubElement(mdprops, "s3_bucket").text = get_from_event(event, "bucket")
    etree.SubElement(mdprops, "s3_object_key").text = s3_object_key
    etree.SubElement(mdprops, "dc_source").text = s3_object_key
    etree.SubElement(mdprops, "s3_object_owner").text = get_from_event(event, "user")
    etree.SubElement(mdprops, "dc_identifier_localid").text = media_id

    relations = etree.SubElement(mdprops, "dc_relations")
    etree.SubElement(relations, "is_verwant_aan").text = pid

    tree = etree.ElementTree(root)
    return etree.tostring(
        root, pretty_print=True, encoding="UTF-8", xml_declaration=True
    )

def construct_essence_update_sidecar(pid):
    root = etree.Element("MediaHAVEN_external_metadata")
    mdprops = etree.SubElement(root, "MDProperties")
    relations = etree.SubElement(mdprops, "dc_relations")
    etree.SubElement(relations, "is_verwant_aan").text = pid

    tree = etree.ElementTree(root)
    return etree.tostring(
        root, pretty_print=True, encoding="UTF-8", xml_declaration=True
    )

def callback(ch, method, properties, body, ctx):
    try:
        event = json.loads(body)
    except json.JSONDecodeError as error:
        log.warning("Bad s3 event.", error=str(error))
        return

    # Check if item already in mediahaven based on key and md5
    mediahaven_service = MediahavenService(ctx)
    query_params = [
        ("s3_object_key", get_from_event(event, "object_key")),
        ("md5", get_from_event(event, "md5")),
    ]
    result = mediahaven_service.get_fragment(query_params)

    if result["MediaDataList"]:
        log.warning(
            "Item already archived", s3_object_key=get_from_event(event, "object_key")
        )
        return

    # Check if we are dealing with essence or collateral
    bucket = get_from_event(event, "bucket")
    if bucket == "mam-collaterals":
        object_key = get_from_event(event, "object_key")
        collateral_type = object_key.split("/")[0]
        media_id = object_key.split("/")[1]

        query_params = [
            ("dc_identifier_localid", media_id),
        ]
        result = mediahaven_service.get_fragment(query_params)
        essence_pid = result["MediaDataList"][0]["Dynamic"]["PID"]
        essence_fragment_id = result["MediaDataList"][0]["Internal"]["FragmentId"]
        
        pid = f"{essence_pid}_{collateral_type}"
        dest_path = construct_destination_path(cp_name, ctx.config.app_cfg['collateral-destination-folder'])
        dest_filename = f"{pid}.xml"

        sidecar_xml = construct_collateral_sidecar(event, pid, media_id)

        essence_update_sidecar = construct_essence_update_sidecar(pid)
        mediahaven_service.update_metadata(essence_fragment_id, essence_update_sidecar)

    else:
        pid_service = PIDService(ctx)
        pid = pid_service.get_pid()
        log.info(f"PID received: {pid}")

        dest_path = construct_destination_path(cp_name, ctx.config.app_cfg['essence-destination-folder'])
        dest_filename = f"{pid}.xml"

        sidecar_xml = construct_essence_sidecar(event, pid)

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

    or_id = get_from_event(event, "tenant")
    org_service = OrganisationsService(ctx)
    cp_name = org_service.get_organisation(or_id)["cp_name_mam"]

    log.debug(f"Destination: path={dest_path}, file_name={dest_filename}")

    ftp = FTP(ctx)
    ftp.put(sidecar_xml, dest_path, dest_filename)

    # Request file transfer
    file_extension = os.path.splitext(get_from_event(event, "object_key"))[1]
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
