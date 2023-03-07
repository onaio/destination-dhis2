import json
from unittest.mock import MagicMock

from airbyte_cdk.models import (  # type: ignore # see this https://github.com/airbytehq/airbyte/pull/22963
    ConnectorSpecification,
)

from destination_dhis2.destination import DestinationDhis2

spec_file = open("destination_dhis2/spec.json")
spec_file_data = json.load(spec_file)


def test_spec() -> None:
    source = DestinationDhis2()
    spec = source.spec(MagicMock())
    assert (
        spec.connectionSpecification["title"]
        == spec_file_data["connectionSpecification"]["title"]
    )
    assert spec.connectionSpecification["title"] == "Destination Dhis2"
    assert isinstance(spec, ConnectorSpecification)
