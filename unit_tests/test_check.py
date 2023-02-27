from unittest.mock import MagicMock

from airbyte_cdk.models import AirbyteConnectionStatus, Status
from requests.exceptions import ConnectTimeout, RequestException
from requests_mock import Mocker

from destination_dhis2.destination import DestinationDhis2


def test_check_connection(
    config,
    requests_mock: Mocker,
    token_refresh_endpoint,
    sample_access_token,
    data_elements_url,
):
    source = DestinationDhis2()
    # fake refresh_token success
    requests_mock.post(
        url=token_refresh_endpoint,
        json={"access_token": sample_access_token, "expires_in": 43199},
    )
    # fake check success
    requests_mock.get(url=data_elements_url, text="Success")

    resp = source.check(MagicMock(), config)
    assert resp == AirbyteConnectionStatus(status=Status.SUCCEEDED)


def test_check_connection_authenticator_fail(
    config,
    requests_mock: Mocker,
    token_refresh_endpoint,
    data_elements_url,
):
    source = DestinationDhis2()
    # fake refresh_token RequestException
    requests_mock.post(url=token_refresh_endpoint, exc=RequestException)
    # fake check success
    requests_mock.get(url=data_elements_url, status_code=200)

    resp = source.check(MagicMock(), config)
    assert resp.status == AirbyteConnectionStatus(status=Status.FAILED).status
    assert "Exception in check command" in str(resp.message)
    assert "Error while refreshing access token" in str(resp.message)
    assert "RequestException" in str(resp.message)


def test_check_connection_raise_for_status(
    config,
    requests_mock: Mocker,
    token_refresh_endpoint,
    sample_access_token,
    data_elements_url,
):
    source = DestinationDhis2()
    # fake refresh_token success
    requests_mock.post(
        url=token_refresh_endpoint,
        json={"access_token": sample_access_token, "expires_in": 43199},
    )
    # fake check raise_for_status
    requests_mock.get(url=data_elements_url, status_code=500)

    resp = source.check(MagicMock(), config)
    assert resp.status == AirbyteConnectionStatus(status=Status.FAILED).status
    assert "Exception in check command" in str(resp.message)
    assert "500 Server Error" in str(resp.message)


def test_check_connection_fail(
    config,
    requests_mock: Mocker,
    token_refresh_endpoint,
    sample_access_token,
    data_elements_url,
):
    source = DestinationDhis2()
    # fake refresh_token success
    requests_mock.post(
        url=token_refresh_endpoint,
        json={"access_token": sample_access_token, "expires_in": 43199},
    )
    # fake check ConnectTimeout
    requests_mock.get(url=data_elements_url, exc=ConnectTimeout)

    resp = source.check(MagicMock(), config)
    assert resp.status == AirbyteConnectionStatus(status=Status.FAILED).status
    assert "Exception in check command" in str(resp.message)
    assert "ConnectTimeout" in str(resp.message)
