from unittest.mock import MagicMock
from urllib.parse import urljoin

from airbyte_cdk.models import AirbyteConnectionStatus, Status

from destination_dhis2.constants import DATA_ELEMENTS_PATH
from destination_dhis2.destination import DestinationDhis2


def test_check_connection(
    config,
    requests_mock,
    oauth_configs,
    sample_access_token,
    base_url,
):
    source = DestinationDhis2()
    # fake refresh token
    requests_mock.post(
        oauth_configs["token_refresh_endpoint"],
        json={"access_token": sample_access_token, "expires_in": 43199},
    )
    # fake check
    requests_mock.get(urljoin(base_url, DATA_ELEMENTS_PATH), text="Success")
    assert source.check(MagicMock(), config) == AirbyteConnectionStatus(
        status=Status.SUCCEEDED
    )
