import pytest

from mediahaven.mocks.base_resource import MediaHavenPageObjectJSONMock


@pytest.fixture
def mock_ftp(monkeypatch):
    def mock_connect(self):
        print("Connecting to host.")
        pass

    def mock_put(self, content_bytes, destination_path, destination_filename):
        print(f"Putting {destination_filename} to {destination_path} on {self.host}")
        pass

    from meemoo.helpers import FTP

    monkeypatch.setattr(FTP, "put", mock_put)
    monkeypatch.setattr(FTP, "_FTP__connect", mock_connect)


@pytest.fixture
def mock_organisations_api(monkeypatch):
    def mock_get_mam_label(self, or_id):
        print(f"getting name for {or_id}")
        if or_id == "or_id_none":
            return None
        return "UNITTEST"

    from meemoo.services import OrganisationsService

    monkeypatch.setattr(OrganisationsService, "get_mam_label", mock_get_mam_label)


@pytest.fixture
def mock_events(monkeypatch):
    def mock_init(self, name, ctx):
        print("Initiating Rabbit connection.")
        pass

    def mock_publish(self, message, correlation_id):
        print(f"Publish message: {message}")
        pass

    from meemoo.events import Events

    monkeypatch.setattr(Events, "__init__", mock_init)
    monkeypatch.setattr(Events, "publish", mock_publish)


@pytest.fixture
def mock_mediahaven_api(monkeypatch):
    def mock_init(self, url, grant):
        print("Initiating MH Client.")
        pass

    def mock_get_token(self):
        print("Getting token.")
        return {"access_token": "bearer, bear with me"}

    def mock_get_fragment(self, q: str = None) -> MediaHavenPageObjectJSONMock:
        print("Get fragment")

        return MediaHavenPageObjectJSONMock(
            [
                {
                    "Dynamic": {"PID": "pid", "dc_identifier_localid": "media_id"},
                    "Internal": {
                        "FragmentId": "ID",
                        "MediaObjectId": "umid",
                        "IsFragment": False,
                    },
                },  # Essence
                {
                    "Dynamic": {"PID": "pid2", "dc_identifier_localid": "media_id2"},
                    "Internal": {
                        "FragmentId": "ID2",
                        "MediaObjectId": "umid",
                        "IsFragment": True,
                    },
                },  # Fragment 1 of essence
                {
                    "Dynamic": {
                        "PID": "pid2_collateral",
                        "dc_identifier_localid": "media_id2",
                    },
                    "Internal": {
                        "FragmentId": "ID3",
                        "MediaObjectId": "umid_collateral",
                        "IsFragment": False,
                    },
                },  # Collateral of fragment 1
                {
                    "Dynamic": {
                        "PID": "pid2_metadata",
                        "dc_identifier_localid": "media_id2",
                    },
                    "Internal": {
                        "FragmentId": "ID4",
                        "MediaObjectId": "umid_metadata",
                        "IsFragment": False,
                    },
                },  # Metadata collateral of fragment 1
                {
                    "Dynamic": {"PID": "pid3", "dc_identifier_localid": "media_id3"},
                    "Internal": {
                        "FragmentId": "ID5",
                        "MediaObjectId": "umid",
                        "IsFragment": True,
                    },
                },  # Fragment 2 of essence
            ],
            total_nr_of_results=5,
            nr_of_results=5,
        )

    def mock_update_metadata(
        self, fragment_id: str, metadata: str = None, metadata_content_type: str = None
    ) -> bool:
        print("Update metadata")
        return True

    def mock_delete_media_object(self, fragment_id: str, reason: str = None) -> bool:
        print("Delete media object")
        return True

    def mock_request_token(self, user, password):
        print("Mocking getting a token.")
        pass

    monkeypatch.setattr(
        "mediahaven.resources.records.Records.search", mock_get_fragment
    )
    monkeypatch.setattr(
        "mediahaven.resources.records.Records.update", mock_update_metadata
    )
    monkeypatch.setattr(
        "mediahaven.resources.records.Records.delete", mock_delete_media_object
    )
    monkeypatch.setattr("mediahaven.oauth2.ROPCGrant.request_token", mock_request_token)
