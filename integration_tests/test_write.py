import json
from typing import Any, Mapping, cast

from airbyte_cdk.models import (  # type: ignore # see this https://github.com/airbytehq/airbyte/pull/22963
    AirbyteMessage,
    ConfiguredAirbyteCatalog,
    ConfiguredAirbyteStream,
    Type,
)
from pytest import LogCaptureFixture, fixture
from requests_mock import Mocker
from requests_mock.request import _RequestObjectProxy

from destination_dhis2 import DestinationDhis2, Dhis2Client
from destination_dhis2.constants import DATA_VALUE_SETS_PATH


@fixture(scope="session", autouse=True)
def configured_catalog_fixture() -> ConfiguredAirbyteCatalog:
    with open("sample_files/configured_catalog.json", "r") as f:
        data = f.read()
        return ConfiguredAirbyteCatalog(
            streams=[
                ConfiguredAirbyteStream(**stream)
                for stream in json.loads(data)["streams"]
            ]
        )


@fixture(scope="session", autouse=True)
def input_messages_fixture() -> list[AirbyteMessage]:
    with open("sample_files/messages.jsonl", "r") as f:
        data = f.read()
        return [AirbyteMessage(**json.loads(line)) for line in data.split("\n")]


def test_write(
    config: Mapping[str, Any],
    configured_catalog_fixture: ConfiguredAirbyteCatalog,
    input_messages_fixture: list[AirbyteMessage],
    requests_mock: Mocker,
    token_refresh_endpoint: str,
    sample_access_token: str,
    caplog: LogCaptureFixture,
) -> None:
    destination = DestinationDhis2()
    client = Dhis2Client(**config)

    requests_mock.post(
        url=token_refresh_endpoint,
        json={"access_token": sample_access_token, "expires_in": 43199},
    )
    text_response = "request succeeded"
    requests_mock.post(
        url=client._join_url_fragments(DATA_VALUE_SETS_PATH),
        text=text_response,
    )

    result = list(
        destination.write(config, configured_catalog_fixture, input_messages_fixture)
    )

    assert "Starting syncing SourceHapiFHIRAggregator" in caplog.messages
    assert "Read 4 records from dataElements stream" in caplog.messages
    assert (
        "Stream non_defined_stream was not present in configured streams, skipping"
        in caplog.messages
    )

    assert len(result) == 1
    assert result[0].type == Type.STATE
    assert requests_mock.called
    assert requests_mock.call_count == 2

    input_messages = [
        message.record.data
        for message in input_messages_fixture
        if message.type == Type.RECORD and message.record.stream != "non_defined_stream"
    ]

    assert cast(_RequestObjectProxy, requests_mock.last_request).json() == {
        "dataValues": input_messages
    }
