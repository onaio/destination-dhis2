from typing import Tuple

import requests
from airbyte_cdk.sources.streams.http.requests_native_auth import (
    BasicHttpAuthenticator,
    Oauth2Authenticator,
)


class Dhis2Authenticator(Oauth2Authenticator):
    def refresh_access_token(self) -> Tuple[str, int]:
        try:
            response = requests.request(
                method="POST",
                url=self.get_token_refresh_endpoint(),
                data=self.build_refresh_request_body(),
                # override default class
                # to inject basic auth headers to refresh token method
                headers=BasicHttpAuthenticator(
                    username=self.get_client_id(), password=self.get_client_secret()
                ).get_auth_header(),
            )
            response.raise_for_status()
            response_json = response.json()
            return (
                response_json[self.get_access_token_name()],
                response_json[self.get_expires_in_name()],
            )
        except Exception as e:
            raise Exception(f"Error while refreshing access token: {e}") from e
