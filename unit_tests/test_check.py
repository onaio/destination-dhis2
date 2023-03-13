from unittest.mock import MagicMock

from airbyte_cdk.models import (  # type: ignore # see this https://github.com/airbytehq/airbyte/pull/22963
    AirbyteConnectionStatus,
    Status,
)
from requests.exceptions import ConnectTimeout, RequestException
from requests_mock import Mocker

from destination_dhis2 import DestinationDhis2


def test_check_connection(
    config: dict[str, str],
    requests_mock: Mocker,
    token_refresh_endpoint: str,
    sample_access_token: str,
    data_elements_url: str,
) -> None:
    destination = DestinationDhis2()
    # fake refresh_token success
    requests_mock.post(
        url=token_refresh_endpoint,
        json={"access_token": sample_access_token, "expires_in": 43199},
    )
    # fake check success
    requests_mock.get(url=data_elements_url, text="Success")

    resp = destination.check(MagicMock(), config)
    assert resp == AirbyteConnectionStatus(status=Status.SUCCEEDED)


def test_check_connection_authenticator_fail(
    config: dict[str, str],
    requests_mock: Mocker,
    token_refresh_endpoint: str,
    data_elements_url: str,
) -> None:
    destination = DestinationDhis2()
    # fake refresh_token RequestException
    requests_mock.post(url=token_refresh_endpoint, exc=RequestException)
    # fake check success
    requests_mock.get(url=data_elements_url, status_code=200)

    resp = destination.check(MagicMock(), config)
    assert resp.status == AirbyteConnectionStatus(status=Status.FAILED).status
    assert "Exception in check command" in str(resp.message)
    assert "Error while refreshing access token" in str(resp.message)
    assert "RequestException" in str(resp.message)


def test_check_connection_raise_for_status(
    config: dict[str, str],
    requests_mock: Mocker,
    token_refresh_endpoint: str,
    sample_access_token: str,
    data_elements_url: str,
) -> None:
    destination = DestinationDhis2()
    # fake refresh_token success
    requests_mock.post(
        url=token_refresh_endpoint,
        json={"access_token": sample_access_token, "expires_in": 43199},
    )
    # fake check raise_for_status
    requests_mock.get(url=data_elements_url, status_code=500)

    resp = destination.check(MagicMock(), config)
    assert resp.status == AirbyteConnectionStatus(status=Status.FAILED).status
    assert "Exception in check command" in str(resp.message)
    assert "500 Server Error" in str(resp.message)


def test_check_connection_fail(
    config: dict[str, str],
    requests_mock: Mocker,
    token_refresh_endpoint: str,
    sample_access_token: str,
    data_elements_url: str,
) -> None:
    destination = DestinationDhis2()
    # fake refresh_token success
    requests_mock.post(
        url=token_refresh_endpoint,
        json={"access_token": sample_access_token, "expires_in": 43199},
    )
    # fake check ConnectTimeout
    requests_mock.get(url=data_elements_url, exc=ConnectTimeout)

    resp = destination.check(MagicMock(), config)
    assert resp.status == AirbyteConnectionStatus(status=Status.FAILED).status
    assert "Exception in check command" in str(resp.message)
    assert "ConnectTimeout" in str(resp.message)


def test_check_connection_fail_with_message(
    config: dict[str, str],
    requests_mock: Mocker,
    token_refresh_endpoint: str,
    sample_access_token: str,
    data_elements_url: str,
) -> None:
    destination = DestinationDhis2()
    # fake refresh_token success
    requests_mock.post(
        url=token_refresh_endpoint,
        json={"access_token": sample_access_token, "expires_in": 43199},
    )
    # fake check ConnectTimeout
    requests_mock.get(
        url=data_elements_url,
        status_code=400,
        json={"message": "max-results cannot be negative"},
    )

    resp = destination.check(MagicMock(), config)
    assert resp.status == AirbyteConnectionStatus(status=Status.FAILED).status
    assert "Exception in check command" in str(resp.message)
    assert "max-results cannot be negative" in str(resp.message)
