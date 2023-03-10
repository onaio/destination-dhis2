from typing import Any, Literal, Mapping, Optional, TypedDict
from urllib.parse import urljoin

import requests

from .authenticator import Dhis2Authenticator
from .constants import API_PATH, DATA_VALUE_SETS_PATH, PAGE_SIZE, TOKEN_REFRESH_PATH


class DataValue(TypedDict):
    dataElement: str
    completeDate: str
    period: str
    orgUnit: str
    value: str


DataValues = list[DataValue]


class Dhis2Client:
    write_buffer: DataValues = []

    def __init__(
        self,
        base_url: str,
        client_id: str,
        client_secret: str,
        refresh_token: str,
        api_version: str,
    ):
        self.base_url = base_url
        self.client_id = client_id
        self.client_secret = client_secret
        self.refresh_token = refresh_token
        self.api_version = api_version

    def _join_url_fragments(self, endpoint: str) -> str:
        # constitute complete url by join url fragments
        # while stripping possibly misplaced forward slashes
        return urljoin(
            self.base_url,
            "/".join(
                part.strip("/") for part in (API_PATH, self.api_version, endpoint)
            ),
        )

    @property
    def _flush_interval(self) -> int:
        return PAGE_SIZE

    @property
    def _token_refresh_endpoint(self) -> str:
        # does not use the API_PATH prefix
        return urljoin(self.base_url, TOKEN_REFRESH_PATH)

    def _get_auth_headers(self) -> Mapping[str, Any]:
        authenticator = Dhis2Authenticator(
            token_refresh_endpoint=self._token_refresh_endpoint,
            client_id=self.client_id,
            client_secret=self.client_secret,
            refresh_token=self.refresh_token,
        )
        return authenticator.get_auth_header()

    def request(
        self,
        http_method: str,
        endpoint: str,
        params: Optional[dict[str, Any]] = None,
        json: Optional[dict[str, Any]] = None,
    ) -> requests.Response:
        url = self._join_url_fragments(endpoint)
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            **self._get_auth_headers(),
        }
        response = requests.request(
            method=http_method, url=url, headers=headers, params=params, json=json
        )
        return response

    def _batch_write(self, dataValues: DataValues) -> requests.Response:
        """
        https://docs.dhis2.org/en/develop/using-the-api/dhis-core-version-master/data.html#webapi_sending_bulks_data_values
        """

        request_body: dict[Literal["dataValues"], DataValues] = {
            "dataValues": dataValues
        }
        response = self.request(
            http_method="POST",
            endpoint=DATA_VALUE_SETS_PATH,
            json=request_body,  # type: ignore # Type[Literal["some_str"]] != str
        )
        return response

    def queue_write_operation(self, dataValue: DataValue) -> None:
        # remove other possible appends
        data_value: DataValue = {
            "dataElement": dataValue["dataElement"],
            "completeDate": dataValue["completeDate"],
            "period": dataValue["period"],
            "orgUnit": dataValue["orgUnit"],
            "value": dataValue["value"],
        }
        self.write_buffer.append(data_value)

    def buffer_is_full(self) -> bool:
        return len(self.write_buffer) >= self._flush_interval

    def flush(self) -> None:
        # TODO: Handle retry?
        # best place to handle retry imo
        if len(self.write_buffer) > 0:
            response = self._batch_write(self.write_buffer)
            response.raise_for_status()
            self.write_buffer.clear()
