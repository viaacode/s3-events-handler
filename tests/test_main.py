import json
from unittest.mock import MagicMock, patch

import pytest
from main import (
    callback,
    construct_collateral_sidecar,
    construct_essence_sidecar,
    construct_fragment_update_sidecar,
    cp_names,
    get_cp_name,
    handle_create_event,
    NackException,
    query_params_item_ingested,
)
from .mocks import mock_events, mock_ftp, mock_organisations_api, mock_mediahaven_api
from .resources import (
    S3_MOCK_ESSENCE_EVENT,
    S3_MOCK_COLLATERAL_EVENT,
    S3_MOCK_REMOVED_EVENT,
    S3_MOCK_UNKNOWN_EVENT,
    S3_MOCK_ESSENCE_EVENT_WITHOUT_MD5,
    MOCK_MEDIAHAVEN_EXTERNAL_METADATA,
    MOCK_MEDIAHAVEN_EXTERNAL_METADATA_COLLATERAL,
    MOCK_MEDIAHAVEN_FRAGMENT_UPDATE,
)
from mediahaven import MediaHaven
from mediahaven.oauth2 import ROPCGrant
from mediahaven.mocks.base_resource import MediaHavenPageObjectJSONMock


class Expando(object):
    pass


@pytest.fixture
def context():
    from viaa.configuration import ConfigParser
    from meemoo.context import Context

    config = ConfigParser()
    return Context(config)


@pytest.mark.parametrize(
    "body",
    [
        S3_MOCK_ESSENCE_EVENT,
        S3_MOCK_COLLATERAL_EVENT,
        S3_MOCK_REMOVED_EVENT,
    ],
    ids=[
        "s3 essence event",
        "s3 collateral event",
        "s3 removed event"
    ],
)
def test_callback(
    body, context, mock_ftp, mock_organisations_api, mock_events, mock_mediahaven_api
):
    ex = Expando()
    ex.correlation_id = "a1b2c3"
    channel_mock = MagicMock()
    callback(
        channel_mock,
        MagicMock(),
        ex,
        body,
        context,
        MediaHaven("", ROPCGrant("", "", "")),
    )
    assert channel_mock.basic_ack.call_count == 1
    assert not channel_mock.basic_nack.call_count


@patch("mediahaven.MediaHaven")
@patch("mediahaven.oauth2.ROPCGrant")
@patch("main.PIDService")
@patch("main.construct_essence_sidecar")
@patch("main.FTP")
@patch("main.OrganisationsService")
@patch("main.Events")
@patch("main.construct_collateral_sidecar")
def test_handle_create_event_essence(
    construct_collateral_sidecar_mock,
    events_mock,
    org_service_mock,
    ftp_mock,
    construct_essence_sidecar_mock,
    pid_service_mock,
    grant_mock,
    mediahaven_mock,
    context,
):
    ex = Expando()
    ex.correlation_id = "a1b2c3"

    mediahaven_mock.records.search.return_value = MediaHavenPageObjectJSONMock(
        [], nr_of_results=0
    )
    pid_service_mock().get_pid.return_value = "12345678"
    handle_create_event(json.loads(S3_MOCK_ESSENCE_EVENT), ex, context, mediahaven_mock)

    assert construct_essence_sidecar_mock.call_count == 1
    assert ftp_mock().put.call_count == 1
    assert events_mock().publish.call_count == 1
    assert pid_service_mock().get_pid.call_count == 1
    assert pid_service_mock().get_pid.return_value == "12345678"
    assert construct_collateral_sidecar_mock.call_count == 0


@patch("mediahaven.MediaHaven")
@patch("mediahaven.oauth2.ROPCGrant")
@patch("main.PIDService")
@patch("main.construct_collateral_sidecar")
@patch("main.FTP")
@patch("main.OrganisationsService")
@patch("main.Events")
@patch("main.construct_fragment_update_sidecar")
@patch("main.construct_essence_sidecar")
def test_handle_create_event_collateral(
    construct_essence_sidecar_mock,
    construct_fragment_update_sidecar_mock,
    events_mock,
    org_service_mock,
    ftp_mock,
    construct_collateral_sidecar_mock,
    pid_service_mock,
    grant_mock,
    mediahaven_mock,
    context,
):
    ex = Expando()
    ex.correlation_id = "a1b2c3"

    # Use side effect in order to mock
    mediahaven_mock.records.search.side_effect = [
        MediaHavenPageObjectJSONMock([], nr_of_results=0),
        MediaHavenPageObjectJSONMock(
            [
                {"Internal": {"FragmentId": 1}, "Dynamic": {"PID": "pid1"}},
                {"Internal": {"FragmentId": 2}, "Dynamic": {"PID": "pid2"}},
            ],
            nr_of_results=2,
        ),
    ]

    pid_service_mock().get_pid.return_value = "12345678"
    handle_create_event(
        json.loads(S3_MOCK_COLLATERAL_EVENT), ex, context, mediahaven_mock
    )

    assert mediahaven_mock.records.search.call_count == 2
    assert ftp_mock().put.call_count == 1
    assert events_mock().publish.call_count == 1
    assert pid_service_mock().get_pid.call_count == 0
    assert construct_fragment_update_sidecar_mock.call_count == 1
    assert construct_essence_sidecar_mock.call_count == 0


@pytest.mark.parametrize(
    "body",
    [
        S3_MOCK_UNKNOWN_EVENT,
    ],
    ids=[
        "unknown event"
    ],
)
def test_callback_unsuccessful(
    body, context, mock_ftp, mock_organisations_api, mock_events, mock_mediahaven_api
):
    ex = Expando()
    ex.correlation_id = "a1b2c3"
    channel_mock = MagicMock()
    callback(
        channel_mock,
        MagicMock(),
        ex,
        body,
        context,
        MediaHaven("", ROPCGrant("", "", "")),
    )
    assert channel_mock.basic_nack.call_count == 1
    assert not channel_mock.basic_ack.call_count


def test_construct_essence_sidecar():
    # ARRANGE
    event = json.loads(S3_MOCK_ESSENCE_EVENT)

    # ACT
    sidecar_xml = construct_essence_sidecar(event, "test_pid", "VRT")

    # ASSERT
    assert sidecar_xml.decode("utf-8") == MOCK_MEDIAHAVEN_EXTERNAL_METADATA


def test_construct_collateral_sidecar():
    # ARRANGE
    event = json.loads(S3_MOCK_COLLATERAL_EVENT)

    # ACT
    sidecar_xml = construct_collateral_sidecar(
        event, "test_pid", "media_id", "VRT", "metadata"
    )

    # ASSERT
    assert sidecar_xml.decode("utf-8") == MOCK_MEDIAHAVEN_EXTERNAL_METADATA_COLLATERAL


def test_construct_fragment_update_sidecar():
    # ACT
    sidecar_xml = construct_fragment_update_sidecar("test_pid")

    # ASSERT
    assert sidecar_xml.decode("utf-8") == MOCK_MEDIAHAVEN_FRAGMENT_UPDATE


def test_get_cp_name(context, mock_organisations_api):
    or_id = "or_id"
    assert or_id not in cp_names
    name = get_cp_name("or_id", context)
    assert name == "UNITTEST"
    assert cp_names["or_id"] == name


def test_get_cp_name_cached(context, mock_organisations_api):
    or_id = "or_id_cached"
    assert or_id not in cp_names
    cp_name = "CP MAM NAME"
    cp_names["or_id"] = cp_name
    name = get_cp_name("or_id", context)
    assert name != "UNITTEST"
    assert name == cp_name


def test_query_params_item_ingested():
    event = json.loads(S3_MOCK_ESSENCE_EVENT)
    params = query_params_item_ingested(event, "cp")
    assert params == [
        (
            "s3_object_key",
            "191213-VAN___statement_De_ideale_wereld___Don_12_December_2019-1983-d5be522e-3609-417a-a1f4-5922854620c8.MXF",
        ),
        ("md5", "1234abcd1234abcd1234abcd1234abcd"),
    ]


@pytest.mark.parametrize("cp", ["cp", "VRT"])
def test_query_params_item_ingested_collateral(cp):
    """The item is a collateral, so check on md5 regardless of CP"""
    event = json.loads(S3_MOCK_COLLATERAL_EVENT)
    params = query_params_item_ingested(event, cp)
    assert params == [
        (
            "s3_object_key",
            "TYPE/MEDIAID/blabla.xif",
        ),
        ("md5", "1234abcd1234abcd1234abcd1234abcd"),
    ]


@pytest.mark.parametrize(
    "cp, body",
    [
        ("cp", S3_MOCK_ESSENCE_EVENT_WITHOUT_MD5),
        ("VRT", S3_MOCK_ESSENCE_EVENT),
        ("VRT", S3_MOCK_ESSENCE_EVENT_WITHOUT_MD5),
    ],
    ids=[
        "cp event without md5",
        "vrt event",
        "vrt event without md5"
    ],
)
def test_query_params_item_ingested_no_md5(cp, body):
    event = json.loads(body)
    params = query_params_item_ingested(event, cp)
    assert params == [
        (
            "s3_object_key",
            "191213-VAN___statement_De_ideale_wereld___Don_12_December_2019-1983-d5be522e-3609-417a-a1f4-5922854620c8.MXF",
        )
    ]
