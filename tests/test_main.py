import json

import pytest
from main import callback, construct_essence_sidecar

from .mocks import mock_events, mock_ftp, mock_organisations_api, mock_mediahaven_api
from .resources import (
    S3_MOCK_ESSENCE_EVENT,
    S3_MOCK_COLLATERAL_EVENT,
    MOCK_MEDIAHAVEN_EXTERNAL_METADATA
)


class Expando(object):
    pass


@pytest.fixture
def context():
    from viaa.configuration import ConfigParser
    from meemoo.context import Context

    config = ConfigParser()
    return Context(config)


@pytest.mark.parametrize("body", [S3_MOCK_ESSENCE_EVENT, S3_MOCK_COLLATERAL_EVENT])
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
    callback(None, None, ex, body, context)


def test_construct_essence_sidecar():
    # ARRANGE
    event = json.loads(S3_MOCK_ESSENCE_EVENT)

    # ACT
    sidecar_xml = construct_essence_sidecar(event, "test_pid")

    # ASSERT
    assert sidecar_xml.decode("utf-8") == MOCK_MEDIAHAVEN_EXTERNAL_METADATA
