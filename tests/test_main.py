import pytest

from main import callback
from .resources import S3_MOCK_EVENT
from .mocks import mock_ftp, mock_organisations_api, mock_events


@pytest.fixture
def context():
    from viaa.configuration import ConfigParser
    from meemoo.context import Context

    config = ConfigParser()
    return Context(config)


def test_callback(context, mock_ftp, mock_organisations_api, mock_events):
    callback(None, None, None, S3_MOCK_EVENT, context)
