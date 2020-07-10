import json

import pytest
from main import callback, construct_mediahaven_external_metadata

from .mocks import mock_events, mock_ftp, mock_organisations_api
from .resources import S3_MOCK_EVENT, MOCK_MEDIAHAVEN_EXTERNAL_METADATA


class Expando(object):
    pass


@pytest.fixture
def context():
    from viaa.configuration import ConfigParser
    from meemoo.context import Context

    config = ConfigParser()
    return Context(config)


def test_callback(context, mock_ftp, mock_organisations_api, mock_events):
    ex = Expando()
    ex.correlation_id = "a1b2c3"
    callback(None, None, ex, S3_MOCK_EVENT, context)


def test_construct_mediahaven_external_metadata():
    # ARRANGE
    event = json.loads(S3_MOCK_EVENT)

    # ACT
    sidecar_xml = construct_mediahaven_external_metadata(event, "test_pid")

    # ASSERT
    assert sidecar_xml.decode("utf-8") == MOCK_MEDIAHAVEN_EXTERNAL_METADATA
