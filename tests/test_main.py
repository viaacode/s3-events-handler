import pytest

from main import callback
from .resources import S3_MOCK_EVENT
from .mocks import mock_ftp, mock_organisations_api, mock_events


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
