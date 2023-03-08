from requests_mock import Mocker

from destination_dhis2 import DataValues, Dhis2Client
from destination_dhis2.constants import DATA_VALUE_SETS_PATH


def test_dhis2_client(
    config: dict[str, str],
    base_url: str,
    requests_mock: Mocker,
    token_refresh_endpoint: str,
    sample_access_token: str,
    data_values: DataValues,
) -> None:
    client = Dhis2Client(**config)

    write_buffer = client.write_buffer
    assert len(write_buffer) == 0

    some_endpoint = "/some/endpoint"
    join_url_fragments = client._join_url_fragments(some_endpoint)
    assert join_url_fragments == f"{base_url}/api/29{some_endpoint}"

    requests_mock.post(
        url=token_refresh_endpoint,
        json={"access_token": sample_access_token, "expires_in": 43199},
    )
    assert client._get_auth_headers() == {
        "Authorization": f"Bearer {sample_access_token}"
    }

    text_response = "request succeeded"
    requests_mock.get(url=join_url_fragments, text=text_response)
    assert client.request("GET", some_endpoint).text == text_response

    requests_mock.post(
        url=client._join_url_fragments(DATA_VALUE_SETS_PATH),
        text=text_response,
    )
    assert client._batch_write(data_values).text == text_response

    for data_value in data_values:
        client.queue_write_operation(data_value)
    assert len(client.write_buffer) == 2
    assert client.write_buffer == data_values
    assert client.buffer_is_full() is False

    client.flush()
    assert len(client.write_buffer) == 0
