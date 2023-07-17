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
import re
from typing import List, Tuple

# 3d party imports
from lxml import etree
from mediahaven import MediaHaven
from mediahaven.oauth2 import RequestTokenError, ROPCGrant
from meemoo import Context
from meemoo.events import Events
from meemoo.helpers import (
    FTP,
    get_from_event,
    is_event_valid,
    InvalidEventException,
    get_destination_for_cp,
)
from requests.exceptions import HTTPError, RequestException

# Local imports
from meemoo.services import OrganisationsService, PIDService, OrgApiError
from viaa.configuration import ConfigParser
from viaa.observability import logging

config = ConfigParser()
log = logging.get_logger(__name__, config=config)
cp_names = {}


class NackException(Exception):
    """Exception raised when there is a situation in which handling
    of the event should be stopped.
    """

    def __init__(self, message, requeue=False, **kwargs):
        self.message = message
        self.requeue = requeue
        self.kwargs = kwargs


def handle_nack_exception(nack_exception, channel, delivery_tag):
    """Log an error and send a nack to rabbit"""
    log.error(nack_exception.message, **nack_exception.kwargs)
    if nack_exception.requeue:
        time.sleep(10)
    channel.basic_nack(delivery_tag=delivery_tag, requeue=nack_exception.requeue)


def construct_destination_path(environment, cp_name, file_type):
    destination_folder = get_destination_for_cp(environment, cp_name, file_type)
    return f"/{cp_name}/{destination_folder}"


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


def construct_essence_sidecar(event, pid, cp_name):
    s3_object_key = get_from_event(event, "object_key")

    root = etree.Element("MediaHAVEN_external_metadata")
    etree.SubElement(root, "title").text = f"Essence: pid: {pid}"

    description = f"""Main fragment for essence:
    - filename: {s3_object_key}
    - CP: {cp_name}
    """
    etree.SubElement(root, "description").text = description

    mdprops = etree.SubElement(root, "MDProperties")
    etree.SubElement(mdprops, "CP").text = cp_name
    etree.SubElement(mdprops, "CP_id").text = get_from_event(event, "tenant")
    etree.SubElement(mdprops, "sp_name").text = "s3"
    etree.SubElement(mdprops, "PID").text = pid
    etree.SubElement(mdprops, "s3_domain").text = get_from_event(event, "domain")
    etree.SubElement(mdprops, "s3_bucket").text = get_from_event(event, "bucket")
    etree.SubElement(mdprops, "s3_object_key").text = s3_object_key
    etree.SubElement(mdprops, "dc_source").text = s3_object_key
    etree.SubElement(mdprops, "s3_object_owner").text = get_from_event(event, "user")
    etree.SubElement(mdprops, "object_level").text = "file"
    etree.SubElement(mdprops, "object_use").text = "archive_master"
    etree.SubElement(mdprops, "ie_type").text = "n/a"

    # Only add md5 if valid and available from the event.
    if re.match("^[a-fA-F0-9]{32}$", get_from_event(event, "md5")):
        etree.SubElement(mdprops, "md5").text = get_from_event(event, "md5")

    return etree.tostring(
        root, pretty_print=True, encoding="UTF-8", xml_declaration=True
    )


def construct_collateral_sidecar(event, pid, media_id, cp_name, object_use):
    s3_object_key = get_from_event(event, "object_key")

    root = etree.Element("MediaHAVEN_external_metadata")
    etree.SubElement(root, "title").text = f"Collateral: pid: {pid}"

    description = f"""Subtitles for essence:
    - filename: {s3_object_key}
    - CP: {cp_name}
    """
    etree.SubElement(root, "description").text = description

    mdprops = etree.SubElement(root, "MDProperties")
    etree.SubElement(mdprops, "CP").text = cp_name
    etree.SubElement(mdprops, "CP_id").text = get_from_event(event, "tenant")
    etree.SubElement(mdprops, "sp_name").text = "s3"
    etree.SubElement(mdprops, "PID").text = pid
    etree.SubElement(mdprops, "s3_domain").text = get_from_event(event, "domain")
    etree.SubElement(mdprops, "s3_bucket").text = get_from_event(event, "bucket")
    etree.SubElement(mdprops, "s3_object_key").text = s3_object_key
    etree.SubElement(mdprops, "dc_source").text = s3_object_key
    etree.SubElement(mdprops, "s3_object_owner").text = get_from_event(event, "user")
    etree.SubElement(mdprops, "dc_identifier_localid").text = media_id
    etree.SubElement(mdprops, "object_level").text = "file"
    etree.SubElement(mdprops, "object_use").text = object_use
    etree.SubElement(mdprops, "ie_type").text = "n/a"

    # Only add md5 if valid and available from the event.
    if re.match("^[a-fA-F0-9]{32}$", get_from_event(event, "md5")):
        etree.SubElement(mdprops, "md5").text = get_from_event(event, "md5")

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


def get_cp_name(or_id: str, ctx: Context) -> str:
    """Retrieves the CP MAM Name for a given OR ID.

    The mapping between the OR ID and the CP MAM Name is cached.
    If the mapping is not found, The organisations API will be queried to retrieve
    that information. That information will be cached in a dictionary.

    Arguments:
        or_id {str} -- The OR ID
        ctx {Context} -- The context
    Returns:
        str -- The CP MAM Name
    """
    if or_id in cp_names:
        cp_name = cp_names[or_id]
    else:
        try:
            org_service = OrganisationsService(ctx)
            try:
                cp_name = org_service.get_mam_label(or_id)
            except OrgApiError as e:
                raise NackException(str(e))
            else:
                cp_names[or_id] = cp_name
        except RequestException as error:
            raise NackException(
                "Error connecting to Organisation API, retrying....",
                error=error,
                requeue=True,
            )
        except KeyError:
            raise NackException(f"Organisation not found with or_id: {or_id}")
    return cp_name


def query_params_item_ingested(event: dict, cp_name: str) -> List[Tuple[str, str]]:
    """Construct the query parameters to check if an item is already in the MAM.

    A check on S3 object key is always needed.

    A check on md5 is only executed if there is an md5 and
    the CP is not VRT unless the item is a collateral.

    Returns:
        List[Tuple[str, str]] -- The query params.
    """
    # Check based on the S3 object key
    query_params = [("s3_object_key", get_from_event(event, "object_key"))]

    # Check based on md5 if:
    #  - the md5 is available and
    #  - the CP is not VRT unless the item is a collateral
    md5 = get_from_event(event, "md5")
    if md5 and (cp_name.upper() not in ("VRT") or is_collateral(event)):
        query_params.append(("md5", md5))

    return query_params


def is_collateral(event: dict) -> bool:
    """Check if the event is a collateral."""
    return get_from_event(event, "bucket") == "mam-collaterals"


def handle_create_event(
    event: dict, properties, ctx: Context, mediahaven_client: MediaHaven
) -> bool:
    """Handler for s3 create events"""

    # Get cp_name for or_id
    or_id = get_from_event(event, "tenant")
    cp_name = get_cp_name(or_id, ctx)

    # Check if item already in mediahaven
    query_params = query_params_item_ingested(event, cp_name)
    query = get_mediahaven_query(query_params)
    try:
        result = mediahaven_client.records.search(q=query)
    except RequestException as error:
        raise NackException(
            "Error connecting to MediaHaven, retrying....",
            error=error,
            requeue=True,
        )
    except HTTPError as error:
        raise NackException(
            "Error occurred when querying MediaHaven",
            query_params=query_params,
            error=error,
            error_message=error.response.text,
        )

    if result.nr_of_results:
        log.warning(
            "Item already archived", s3_object_key=get_from_event(event, "object_key")
        )
        return

    # Check if we are dealing with essence or collateral
    if is_collateral(event):
        # Handle collateral
        object_key = get_from_event(event, "object_key")
        try:
            collateral_type = object_key.split("/")[0]
            media_id = object_key.split("/")[1]
        except IndexError as error:
            # Object key is not properly formatted as <collateral_type>/<media_id>/
            raise NackException(
                f"Non-compliant object key for collateral: {object_key}",
                error=error,
            )

        log.debug(f"Received a {collateral_type} for media id: {media_id}")

        query_params = [
            ("dc_identifier_localid", media_id),
        ]
        query = get_mediahaven_query(query_params)
        try:
            result = mediahaven_client.records.search(q=query)
        except RequestException as error:
            raise NackException(
                "Error connecting to MediaHaven, retrying....",
                error=error,
                requeue=True,
            )
        except HTTPError as error:
            raise NackException(
                "Error occurred when querying MediaHaven",
                query_params=query_params,
                error=error,
                error_message=error.response.text,
            )
        if not result.nr_of_results:
            raise NackException(
                f"Item not found in MediaHaven for dc_identifier_localid: {media_id}"
            )

        item_pid = result[0].Dynamic.PID
        item_fragment_id = result[0].Internal.FragmentId

        log.debug(f"Found pid: {item_pid} for media id: {media_id}")

        pid = f"{item_pid}_{collateral_type}"
        dest_path = construct_destination_path(
            ctx.config.app_cfg["environment"], cp_name, "collateral"
        )
        dest_filename = f"{pid}.xml"

        if collateral_type in ("openOt", "closedOt"):
            object_use = "subtitle"
        else:
            object_use = "collateral"

        sidecar_xml = construct_collateral_sidecar(
            event, item_pid, media_id, cp_name, object_use
        )

        essence_update_sidecar = construct_fragment_update_sidecar(pid)
        try:
            mediahaven_client.records.update(
                item_fragment_id, xml=essence_update_sidecar
            )
        except RequestException as error:
            raise NackException(
                "Error connecting to MediaHaven, retrying....",
                error=error,
                requeue=True,
            )
        except HTTPError as error:
            raise NackException(
                "Error occurred when updating metadata of collateral",
                fragment_id=item_fragment_id,
                sidecar=essence_update_sidecar,
                error=error,
                error_message=error.response.text,
            )
    else:
        # Handle essence
        try:
            pid_service = PIDService(ctx)
            pid = pid_service.get_pid()
        except (RequestException, IndexError, KeyError) as error:
            raise NackException(
                "Unable to get a PID, retrying...",
                error=error,
                requeue=True,
            )

        log.info(f"PID received: {pid}")

        dest_path = construct_destination_path(
            ctx.config.app_cfg["environment"], cp_name, "essence"
        )
        dest_filename = f"{pid}.xml"

        sidecar_xml = construct_essence_sidecar(event, pid, cp_name)

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

    # Transfer sidecar to FTP TRA
    try:
        ftp = FTP(ctx)
        ftp.put(sidecar_xml, dest_path, dest_filename)
    except Exception as error:
        # Potential destructive action has happened, allowed to requeue?
        raise NackException(
            "Error transferring sidecar via FTP", sidecar=sidecar_xml, error=error
        )

    # Request file transfer
    file_extension = os.path.splitext(get_from_event(event, "object_key"))[1]
    param_dict = construct_fts_params_dict(event, pid, file_extension, dest_path, ctx)

    events = Events(ctx.config.app_cfg["rabbitmq"]["outgoing"], ctx)
    events.publish(json.dumps(param_dict), properties.correlation_id)


def delete_media_object(
    mediahaven_client: MediaHaven, fragment_id: str, reason: str
) -> bool:
    log.info(
        f"Deleting fragment for object with fragment id: {fragment_id}",
        fragment_id=fragment_id,
    )
    try:
        mediahaven_client.records.delete(fragment_id, reason)
    except RequestException as error:
        raise NackException(
            "Error connecting to MediaHaven, retrying....",
            error=error,
            requeue=True,
        )
    except HTTPError as error:
        raise NackException(
            f"Error when deleting MH fragment with fragment ID: {fragment_id}",
            fragment_id=fragment_id,
            error=error,
            error_message=error.response.text,
        )


def get_mediahaven_query(query_params: List[Tuple[str, str]], or_params=True) -> str:
    query = (
        "+(" + " ".join([f'{k_v[0]}:"{k_v[1]}"' for k_v in query_params]) + ")"
        if or_params
        else " ".join([f'+({k_v[0]}: "{k_v[1]}")' for k_v in query_params])
    )
    return query


def handle_remove_event(
    event: dict, properties, ctx: Context, mediahaven_client: MediaHaven
) -> bool:
    """Handler for s3 removed events

    First we query MH with the s3_object_key and s3_bucket

    This results in the main object (= essence) and its real fragments.
    The real fragments potentially have collaterals linked that need to be deleted.
    So we query all the objects with the media ID of the fragments.
    Of those we filter out the real fragments. We should end up with only the
    collaterals. These will be deleted one by one. Finally, delete the essence.
    """

    # Get cp_name for or_id
    or_id = get_from_event(event, "tenant")
    cp_name = get_cp_name(or_id, ctx)

    # Query MH with the s3_bucket en s3_object_key
    s3_bucket = get_from_event(event, "bucket")
    s3_object_key = get_from_event(event, "object_key")
    query_params = [
        ("s3_object_key", s3_object_key),
        ("s3_bucket", s3_bucket),
    ]
    query = get_mediahaven_query(query_params, or_params=False)
    try:
        result = mediahaven_client.records.search(q=query)
    except RequestException as error:
        raise NackException(
            "Error connecting to MediaHaven, retrying....",
            error=error,
            requeue=True,
        )
    except HTTPError as error:
        raise NackException(
            "Error occurred when querying MediaHaven",
            query_params=query_params,
            error=error,
            error_message=error.response.text,
        )

    if not result.nr_of_results:
        log.info(
            f"No media object found with s3 bucket: {s3_bucket} and object key: {s3_object_key}"
        )
        return

    log.info(
        f"Removing media object with s3 bucket: {s3_bucket} and object key: {s3_object_key}"
    )

    # Collect the Media ID (and Fragment ID) of the fragments. These will be used to remove the collaterals.
    fragments = {}
    for item in result.as_generator():
        if item.Internal.IsFragment:
            fragments[item.Dynamic.dc_identifier_localid] = item.Internal.FragmentId

    try:
        # Get the Fragment ID of the essence to delete
        fragment_id_essence = next(
            item.Internal.FragmentId
            for item in result.as_generator()
            if not item.Internal.IsFragment
        )
    except StopIteration:
        # Should not occur
        return

    if fragments:
        # Query all the objects with the media IDs of the fragments
        query_params_media_ids = [
            ("dc_identifier_localid", media_id) for media_id in fragments.keys()
        ]
        query_media_ids = get_mediahaven_query(query_params_media_ids)
        try:
            response = mediahaven_client.records.search(q=query_media_ids)
        except RequestException as error:
            raise NackException(
                "Error connecting to MediaHaven, retrying....",
                error=error,
                requeue=True,
            )
        except HTTPError as error:
            raise NackException(
                "Error occurred when querying MediaHaven",
                query_params=query_params_media_ids,
                error=error,
                error_message=error.response.text,
            )

        # Collect the Fragment IDs of the collaterals. The Media ID is used in the delete reason.
        fragments_collateral = [
            (item.Internal.FragmentId, item.Dynamic.dc_identifier_localid)
            for item in response.as_generator()
            if not item.Internal.IsFragment
        ]

        # Delete the collaterals
        for fragment_collateral in fragments_collateral:
            # Get the Fragment ID of the fragment to which this collateral is linked to
            local_id = fragment_collateral[1]
            linked_fragment_id = fragments.get(local_id)
            result = delete_media_object(
                mediahaven_client,
                fragment_collateral[0],
                f'Deleted collateral with local_id: "{local_id}" linked to fragment with fragment_id: "{linked_fragment_id}". Essence was deleted via s3 delete-object.',
            )
            time.sleep(0.1)  # Sleep to not hit rate limit

    # Delete the essence
    delete_media_object(
        mediahaven_client,
        fragment_id_essence,
        f's3 delete-object for bucket: "{s3_bucket}" and key: "{s3_object_key}"',
    )


def calculate_handler(event: dict):
    """Factory method to return correct handler"""
    event_name = get_from_event(event, "event_name")
    base_type = event_name.split(":")[0]
    if base_type == "ObjectCreated":
        return handle_create_event
    elif base_type == "ObjectRemoved":
        return handle_remove_event
    else:
        raise NackException(f"Unknown type of s3 event: {event_name}", s3_event=event)


def callback(ch, method, properties, body, ctx, mediahaven_client):
    try:
        event = json.loads(body)
        is_event_valid(event)
    except (json.JSONDecodeError, InvalidEventException) as error:
        log.warning("Bad s3 event.", error=str(error))
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
        return

    try:
        handler = calculate_handler(event)
        handler(event, properties, ctx, mediahaven_client)
    except NackException as error:
        handle_nack_exception(error, ch, method.delivery_tag)
        return

    ch.basic_ack(delivery_tag=method.delivery_tag)


def main(ctx: Context):
    events = Events(ctx.config.app_cfg["rabbitmq"]["incoming"], ctx)
    channel = events.get_channel()
    mediahaven_config = ctx.config.app_cfg["mediahaven-api"]
    client_id = mediahaven_config["client_id"]
    client_secret = mediahaven_config["client_secret"]
    user = mediahaven_config["username"]
    password = mediahaven_config["passwd"]
    url = mediahaven_config["host"]
    grant = ROPCGrant(url, client_id, client_secret)
    try:
        grant.request_token(user, password)
    except RequestTokenError as e:
        log.error(e)
        raise e
    mediahaven_client = MediaHaven(url, grant)

    consumer_tag = channel.basic_consume(
        queue=events.queue,
        # Adapt callback fn to add in the ctx parameter
        on_message_callback=lambda ch, method, properties, body: callback(
            ch, method, properties, body, ctx, mediahaven_client
        ),
    )

    log.info(f"Starting: listening for messages on q:{events.queue}.")
    log.info(f"Starting: consumer tag is: {consumer_tag}.")
    channel.start_consuming()


if __name__ == "__main__":
    ctx = Context(config)

    main(ctx)


# vim modeline
# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4 smartindent
