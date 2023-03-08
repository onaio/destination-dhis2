import pytest
from requests.exceptions import RequestException
from requests_mock import Mocker

from destination_dhis2 import Dhis2Authenticator


def test_dhis2_authenticator(
    requests_mock: Mocker,
    token_refresh_endpoint: str,
    oauth_configs: dict[str, str],
    sample_access_token: str,
) -> None:
    requests_mock.post(
        url=token_refresh_endpoint,
        json={"access_token": sample_access_token, "expires_in": 43199},
    )
    resp = Dhis2Authenticator(**oauth_configs).get_auth_header()
    assert resp == {"Authorization": f"Bearer {sample_access_token}"}


def test_dhis2_authenticator_fail(
    requests_mock: Mocker, token_refresh_endpoint: str, oauth_configs: dict[str, str]
) -> None:
    requests_mock.post(
        url=token_refresh_endpoint,
        exc=RequestException,
    )
    with pytest.raises(RequestException) as exc_info:
        Dhis2Authenticator(**oauth_configs).get_auth_header()
    assert "Error while refreshing access token" in str(exc_info.value)
    assert "RequestException" in str(exc_info)
