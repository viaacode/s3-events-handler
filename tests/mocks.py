import pytest

@pytest.fixture
def mock_ftp(monkeypatch):
    def mock_put(self, content_bytes, destination_path, destination_filename):
        print(f'Putting {destination_filename} to {destination_path} on {self.host}')
        pass
    from meemoo.helpers import FTP
    monkeypatch.setattr(FTP, "put", mock_put)

@pytest.fixture
def mock_organisations_api(monkeypatch):
    def mock_get_organisation(self, or_id):
        print(f'getting name for {or_id}')
        return {
            "cp_name_mam": "UNITTEST"
        }
    from meemoo.services import OrganisationsService
    monkeypatch.setattr(OrganisationsService, "get_organisation", mock_get_organisation)

@pytest.fixture
def mock_filetransfer_service(monkeypatch):
    def mock_send_transfer_request(self, param_dict):
        print(f'Putting message on the FXP service queue: {param_dict}')
        return True
    from meemoo.services import FileTransferService
    monkeypatch.setattr(FileTransferService, "send_transfer_request", mock_send_transfer_request)