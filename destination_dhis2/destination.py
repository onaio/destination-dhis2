#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#


import logging
from typing import Any, Iterable, Mapping

from airbyte_cdk.destinations import Destination
from airbyte_cdk.models import (  # type: ignore # see this https://github.com/airbytehq/airbyte/pull/22963
    AirbyteConnectionStatus,
    AirbyteMessage,
    ConfiguredAirbyteCatalog,
    Status,
)
from requests.exceptions import RequestException

from .client import Dhis2Client
from .constants import DATA_ELEMENTS_PATH


class DestinationDhis2(Destination):
    def write(
        self,
        config: Mapping[str, Any],
        configured_catalog: ConfiguredAirbyteCatalog,
        input_messages: Iterable[AirbyteMessage],
    ) -> Iterable[AirbyteMessage]:
        """
        TODO
        Reads the input stream of messages, config, and catalog to write data to the destination.

        This method returns an iterable (typically a generator of AirbyteMessages via yield) containing state messages received
        in the input message stream. Outputting a state message means that every AirbyteRecordMessage which came before it has been
        successfully persisted to the destination. This is used to ensure fault tolerance in the case that a sync fails before fully completing,
        then the source is given the last state message output from this method as the starting point of the next sync.

        :param config: dict of JSON configuration matching the configuration declared in spec.json
        :param configured_catalog: The Configured Catalog describing the schema of the data being received and how it should be persisted in the
                                    destination
        :param input_messages: The stream of input messages received from the source
        :return: Iterable of AirbyteStateMessages wrapped in AirbyteMessage structs
        """

        pass

    def check(
        self, logger: logging.Logger, config: Mapping[str, Any]
    ) -> AirbyteConnectionStatus:
        try:
            client = Dhis2Client(**config)

            response = client.request(
                http_method="GET",
                endpoint=DATA_ELEMENTS_PATH,
                # return smallest possible subset
                params={
                    "paging": "true",
                    "page": 1,
                    "pageSize": 1,
                },
            )

            if not response.ok:
                try:
                    response_message = response.json()["message"]
                    raise RequestException(response_message)
                except ValueError:
                    response.raise_for_status()
            return AirbyteConnectionStatus(status=Status.SUCCEEDED)

        except (RequestException, Exception) as req_err:
            logger.error(f"Exception in check command: {req_err}")
            return AirbyteConnectionStatus(
                status=Status.FAILED,
                message=f"Exception in check command: {repr(req_err)}",
            )
