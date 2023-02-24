#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#


from typing import Any, Iterable, Mapping

import requests
from airbyte_cdk import AirbyteLogger
from airbyte_cdk.destinations import Destination
from airbyte_cdk.models import (
    AirbyteConnectionStatus,
    AirbyteMessage,
    ConfiguredAirbyteCatalog,
    Status,
)

from .dhis2_auth import Dhis2Authenticator


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
        self, logger: AirbyteLogger, config: Mapping[str, Any]
    ) -> AirbyteConnectionStatus:
        base_url = config["base_url"]
        client_id = config["client_id"]
        client_secret = config["client_secret"]
        refresh_token = config["refresh_token"]

        try:
            authenticator = Dhis2Authenticator(
                token_refresh_endpoint=f"{base_url}/uaa/oauth/token",
                client_id=client_id,
                client_secret=client_secret,
                refresh_token=refresh_token,
            )

            response = requests.get(
                url=f"{base_url}/api/dataElements.json",
                headers=authenticator.get_auth_header(),
                # return smallest possible subset
                params={
                    "paging": "true",
                    "page": 1,
                    "pageSize": 1,
                },
            )

            try:
                response.raise_for_status()
            except requests.exceptions.HTTPError as e:
                try:
                    error = response.json()["message"]
                    logger.error(f"{error}:{e}")
                    raise Exception(f"{error}:{e}")
                except Exception as e:
                    raise e
            return AirbyteConnectionStatus(status=Status.SUCCEEDED)

        except Exception as e:
            logger.error(f"Error in check command: {e}")
            return AirbyteConnectionStatus(
                status=Status.FAILED, message=f"An exception occurred: {repr(e)}"
            )
