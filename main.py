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
import time

# 3d party imports
from lxml import etree
from meemoo import Context
from meemoo.events import Events
from meemoo.helpers import FTP, get_from_event
from requests.exceptions import HTTPError, RequestException

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

    return etree.tostring(
        root, pretty_print=True, encoding="UTF-8", xml_declaration=True
    )


def construct_fragment_update_sidecar(pid):
    root = etree.Element("MediaHAVEN_external_metadata")
    mdprops = etree.SubElement(root, "MDProperties")
    relations = etree.SubElement(mdprops, "dc_relations")
    etree.SubElement(relations, "is_verwant_aan").text = pid

    return etree.tostring(
        root, pretty_print=True, encoding="UTF-8", xml_declaration=True
    )


def create_handler(event: dict, properties, ctx: Context) -> bool:
    """Handler for s3 create events"""
    # Check if item already in mediahaven based on key and md5
    mediahaven_service = MediahavenService(ctx)
    query_params = [
        ("s3_object_key", f'"{get_from_event(event, "object_key")}"'),
        ("md5", get_from_event(event, "md5")),
    ]
    result = mediahaven_service.get_fragment(query_params)

    if result["MediaDataList"]:
        log.warning(
            "Item already archived", s3_object_key=get_from_event(event, "object_key")
        )
        return True

    or_id = get_from_event(event, "tenant")
    org_service = OrganisationsService(ctx)
    cp_name = org_service.get_organisation(or_id)["cp_name_mam"]

    # Check if we are dealing with essence or collateral
    bucket = get_from_event(event, "bucket")
    if bucket == "mam-collaterals":
        object_key = get_from_event(event, "object_key")
        collateral_type = object_key.split("/")[0]
        media_id = object_key.split("/")[1]

        log.debug(f"Received a {collateral_type} for media id: {media_id}")

        query_params = [
            ("dc_identifier_localid", media_id),
        ]
        result = mediahaven_service.get_fragment(query_params)
        item_pid = result["MediaDataList"][0]["Dynamic"]["PID"]
        item_fragment_id = result["MediaDataList"][0]["Internal"]["FragmentId"]

        log.debug(f"Found pid: {item_pid} for media id: {media_id}")

        pid = f"{item_pid}_{collateral_type}"
        dest_path = construct_destination_path(cp_name, ctx.config.app_cfg['collateral-destination-folder'])
        dest_filename = f"{pid}.xml"

        sidecar_xml = construct_collateral_sidecar(event, item_pid, media_id)

        essence_update_sidecar = construct_fragment_update_sidecar(pid)
        mediahaven_service.update_metadata(item_fragment_id, essence_update_sidecar)
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

    log.debug(f"Destination: path={dest_path}, file_name={dest_filename}")

    ftp = FTP(ctx)
    ftp.put(sidecar_xml, dest_path, dest_filename)

    # Request file transfer
    file_extension = os.path.splitext(get_from_event(event, "object_key"))[1]
    param_dict = construct_fts_params_dict(event, pid, file_extension, dest_path, ctx)

    events = Events(ctx.config.app_cfg["rabbitmq"]["outgoing"], ctx)
    events.publish(json.dumps(param_dict), properties.correlation_id)
    return True


def delete_media_object(mediahaven_service, fragment_id: str) -> bool:
    log.info(
        f"Deleting fragment for object with fragment id: {fragment_id}",
        fragment_id=fragment_id,
    )
    try:
        mediahaven_service.delete_media_object(fragment_id)
    except (HTTPError, RequestException) as error:
        log.error(
            f"Error when deleting MH fragment with fragment ID: {fragment_id}",
            error=error,
        )
        return False
    return True


def remove_handler(event: dict, properties, ctx: Context) -> bool:
    """Handler for s3 removed events

    First we query MH with the s3_object_key and s3_bucket

    This results in the main object (= essence) and its real fragments.
    The real fragments potentially have collaterals linked that need to be deleted.
    So we query all the objects with the media ID of the fragments.
    Of those we filter out the real fragments. We should end up with only the
    collaterals. These will be deleted one by one. Finally, delete the essence.
    """

    mediahaven_service = MediahavenService(ctx)

    # Query MH with the s3_bucket en s3_object_key
    s3_bucket = get_from_event(event, "bucket")
    s3_object_key = get_from_event(event, "object_key")
    query_params = [
        ("s3_object_key", f'"{s3_object_key}"'),
        ("s3_bucket", f'"{s3_bucket}"'),
    ]
    try:
        result = mediahaven_service.get_fragment(query_params, or_params=False)
    except (HTTPError, RequestException) as error:
        log.error(
            f"Error when querying MH fragment with s3 bucket: {s3_bucket} and object key: {s3_object_key}",
            error=error,
        )
        return False

    if not result["MediaDataList"]:
        log.info(f"No media object found with s3 bucket: {s3_bucket} and object key: {s3_object_key}")
        return False

    log.info(f"Removing media object with s3 bucket: {s3_bucket} and object key: {s3_object_key}")
    items = result["MediaDataList"]

    # Collect the media IDs of the fragments. These will be used to remove the collaterals.
    media_id_fragments = [
        item["Dynamic"]["dc_identifier_localid"]
        for item in items
        if item["Internal"]["IsFragment"]
    ]
    # Get the umid of the essence to delete
    umid = next(
        item["Internal"]["MediaObjectId"]
        for item in items
        if not item["Internal"]["IsFragment"]
    )

    if media_id_fragments:
        # Query all the objects with the media IDs of the fragments
        query_params_media_ids = [
            ("dc_identifier_localid", f"{media_id}") for media_id in media_id_fragments
        ]
        response = mediahaven_service.get_fragment(query_params_media_ids)
        # Collect the umids of the collaterals
        umids_collateral = [
            item["Internal"]["MediaObjectId"]
            for item in response["MediaDataList"]
            if not item["Internal"]["IsFragment"]
        ]

        # Delete the collaterals
        for umid_collateral in umids_collateral:
            result = delete_media_object(mediahaven_service, umid_collateral)
            if not result:
                return False
            time.sleep(0.2)  # Sleep to not hit rate limit

    # Delete the essence
    return delete_media_object(mediahaven_service, umid)


def get_handler(event: dict):
    """ Factory method to return correct handler """
    event_name = get_from_event(event, "event_name")
    base_type = event_name.split(":")[0]
    if base_type == "ObjectCreated":
        return create_handler
    elif base_type == "ObjectRemoved":
        return remove_handler
    else:
        log.error("Unknown type of s3 event: {event_name}", s3_event=event)
        raise ValueError(event_name)


def callback(ch, method, properties, body, ctx):
    try:
        event = json.loads(body)
    except json.JSONDecodeError as error:
        log.warning("Bad s3 event.", error=str(error))
        return False

    try:
        handler = get_handler(event)
    except ValueError:
        return False

    result = handler(event, properties, ctx)
    return result


def main(ctx):
    events = Events(ctx.config.app_cfg["rabbitmq"]["incoming"], ctx)
    channel = events.get_channel()

    consumer_tag = channel.basic_consume(
        queue=events.queue,
        # Adapt callback fn to add in the ctx parameter
        on_message_callback=lambda ch, method, properties, body: callback(
            ch, method, properties, body, ctx
        ),
        # Ack the message when the callback fn returns
        auto_ack=True,
    )

    log.info(f"Starting: listening for messages on q:{events.queue}.")
    log.info(f"Starting: consumer tag is: {consumer_tag}.")
    channel.start_consuming()


if __name__ == "__main__":
    ctx = Context(config)

    main(ctx)


# vim modeline
# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4 smartindent
