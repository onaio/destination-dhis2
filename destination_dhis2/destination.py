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
    Type,
)
from requests.exceptions import RequestException

from .client import Dhis2Client
from .constants import DATA_ELEMENTS_PATH

airbyteLogger = logging.getLogger("airbyte")


class DestinationDhis2(Destination):
    def write(
        self,
        config: Mapping[str, Any],
        configured_catalog: ConfiguredAirbyteCatalog,
        input_messages: Iterable[AirbyteMessage],
    ) -> Iterable[AirbyteMessage]:
        """This method returns an iterable (typically a generator of AirbyteMessages via yield) containing state messages received
        in the input message stream. Outputting a state message means that every AirbyteRecordMessage which came before it has been
        successfully persisted to the destination. This is used to ensure fault tolerance in the case that a sync fails before fully completing,
        then the source is given the last state message output from this method as the starting point of the next sync.


        Parameters
        ----------

        config : Mapping[str, Any]
            A dict of JSON configuration matching the configuration declared in spec.json

        configured_catalog : ConfiguredAirbyteCatalog
            The Configured Catalog describing the schema of the data being received and how it should be persisted in the destination

        input_messages : Iterable[AirbyteMessage]
            The stream of input messages received from the source

        Returns
        -------
        Iterable[AirbyteMessage]
            Iterable of AirbyteStateMessages wrapped in AirbyteMessage structs
        """

        client = Dhis2Client(**config)

        stream_names = {s.stream.name for s in configured_catalog.streams}

        for stream_name in stream_names:
            airbyteLogger.info(
                f"Starting write to DHIS2 with the '{stream_name}' stream"
            )

            for message in input_messages:
                if message.type == Type.RECORD:
                    data = message.record.data
                    record_stream_name = message.record.stream

                    if record_stream_name not in stream_names:
                        airbyteLogger.warning(
                            f"Stream {record_stream_name} was not present in configured streams, skipping"
                        )
                        continue

                    # add to buffer
                    client.queue_write_operation(dataValue=data)

                    if client.buffer_is_full():
                        try:
                            client.flush()
                        except RequestException as e:
                            airbyteLogger.error(
                                f"Exception flushing AirbyteRecordMessage: {e}"
                            )
                            raise e

                elif message.type == Type.STATE:
                    # Emitting a state message indicates that all records which came before it
                    # have been written to the destination.
                    # So we flush the queue to ensure writes happen
                    # then output the state message to indicate it's safe to checkpoint state.
                    try:
                        airbyteLogger.info(f"flushing buffer for state: {message}")
                        client.flush()
                        yield message
                    except RequestException as e:
                        airbyteLogger.error(
                            f"Exception flushing AirbyteStateMessage: {e}"
                        )
                        raise e

                elif message.type == Type.LOG:
                    airbyteLogger.log(
                        logging.getLevelName(message.log.level.value),
                        message.log.message,
                    )

                # ignore other message types for now
                else:
                    airbyteLogger.info(
                        f"Message type {message.type} not supported, skipping"
                    )
                    continue

            # Make sure to flush any records still in the queue
            try:
                client.flush()
            except RequestException as e:
                airbyteLogger.error(f"Exception flushing AirbyteRecordMessage's: {e}")
                raise e

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

        except RequestException as req_err:
            logger.error(f"Exception in check command: {req_err}")
            return AirbyteConnectionStatus(
                status=Status.FAILED,
                message=f"Exception in check command: {repr(req_err)}",
            )
