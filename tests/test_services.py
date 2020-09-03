import pytest

from meemoo.services import Service


class UnknownHostServiceTest(Service):
    def __init__(self, ctx):
        self.name = "unknown"
        super().__init__(ctx)


@pytest.fixture
def context():
    from viaa.configuration import ConfigParser
    from meemoo.context import Context

    config = ConfigParser()
    return Context(config)


def test_get_service_host_not_found(context, caplog):
    service = UnknownHostServiceTest(context)
    assert service.host is None
    log_record = caplog.records[0]
    assert log_record.levelname == "WARNING"
    assert log_record.message == (
        "The key 'host' not found in the config for service: unknown"
    )
