import json
from unittest.mock import MagicMock

from airbyte_cdk.models import ConnectorSpecification

from destination_dhis2.destination import DestinationDhis2

spec_file = open("destination_dhis2/spec.json")
spec_file_data = json.load(spec_file)


def test_spec(snapshot):
    source = DestinationDhis2()
    logger_mock = MagicMock()
    spec = source.spec(logger_mock)
    assert (
        spec.connectionSpecification["title"]
        == spec_file_data["connectionSpecification"]["title"]
    )
    snapshot.assert_match(spec.connectionSpecification["title"])
    assert isinstance(spec, ConnectorSpecification)
