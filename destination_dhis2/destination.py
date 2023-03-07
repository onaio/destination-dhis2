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
        client = Dhis2Client(**config)

        streams = {s.stream.name for s in configured_catalog.streams}

        for configured_stream in configured_catalog.streams:
            airbyteLogger.info(
                f"Starting write to DHIS2 with the '{configured_stream.stream.name}' stream"
            )

            for message in input_messages:
                if message.type == Type.RECORD:
                    data = message.record.data
                    stream = message.record.stream

                    if stream not in streams:
                        airbyteLogger.debug(
                            f"Stream {stream} was not present in configured streams, skipping"
                        )
                        continue

                    # add to buffer
                    client.queue_write_operation(dataValue=data)

                    if client.buffer_is_full():
                        try:
                            client.flush()
                        except Exception as e:
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
                    except Exception as e:
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
            except Exception as e:
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

        except (RequestException, Exception) as req_err:
            logger.error(f"Exception in check command: {req_err}")
            return AirbyteConnectionStatus(
                status=Status.FAILED,
                message=f"Exception in check command: {repr(req_err)}",
            )
