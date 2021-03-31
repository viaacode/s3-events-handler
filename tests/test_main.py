import json
from unittest.mock import MagicMock

import pytest
from main import callback, construct_essence_sidecar, cp_names, get_cp_name, NackException

from .mocks import mock_events, mock_ftp, mock_organisations_api, mock_mediahaven_api
from .resources import (
    S3_MOCK_ESSENCE_EVENT,
    S3_MOCK_COLLATERAL_EVENT,
    S3_MOCK_REMOVED_EVENT,
    S3_MOCK_UNKNOWN_EVENT,
    MOCK_MEDIAHAVEN_EXTERNAL_METADATA,
)


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
    ]
)
def test_callback(
    body,
    context,
    mock_ftp,
    mock_organisations_api,
    mock_events,
    mock_mediahaven_api
):
    ex = Expando()
    ex.correlation_id = "a1b2c3"
    channel_mock = MagicMock()
    callback(channel_mock, MagicMock(), ex, body, context)
    assert channel_mock.basic_ack.call_count == 1
    assert not channel_mock.basic_nack.call_count


@pytest.mark.parametrize("body", [S3_MOCK_UNKNOWN_EVENT, ])
def test_callback_unsuccessful(
    body,
    context,
    mock_ftp,
    mock_organisations_api,
    mock_events,
    mock_mediahaven_api
):
    ex = Expando()
    ex.correlation_id = "a1b2c3"
    channel_mock = MagicMock()
    callback(channel_mock, MagicMock(), ex, body, context)
    assert channel_mock.basic_nack.call_count == 1
    assert not channel_mock.basic_ack.call_count


def test_construct_essence_sidecar():
    # ARRANGE
    event = json.loads(S3_MOCK_ESSENCE_EVENT)

    # ACT
    sidecar_xml = construct_essence_sidecar(event, "test_pid", "VRT")

    # ASSERT
    assert sidecar_xml.decode("utf-8") == MOCK_MEDIAHAVEN_EXTERNAL_METADATA


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

def test_get_cp_name_none(context, mock_organisations_api):
    or_id = "or_id_none"
    assert or_id not in cp_names
    with pytest.raises(NackException) as exception_info:
        name = get_cp_name("or_id_none", context)