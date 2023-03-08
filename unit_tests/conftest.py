from typing import Callable
from urllib.parse import urljoin

import pytest

from destination_dhis2 import Dhis2Client
from destination_dhis2.constants import DATA_ELEMENTS_PATH, TOKEN_REFRESH_PATH


@pytest.fixture(scope="session", autouse=True)
def base_url() -> str:
    return "https://test.com"


@pytest.fixture(scope="session", autouse=True)
def token_refresh_endpoint(base_url: str) -> str:
    return urljoin(base_url, TOKEN_REFRESH_PATH)


@pytest.fixture(scope="session", autouse=True)
def base_configs() -> dict[str, str]:
    return {
        "client_id": "001",
        "client_secret": "super-secret-41",
        "refresh_token": "1984",
    }


@pytest.fixture(scope="session", autouse=True)
def oauth_configs(
    base_configs: dict[str, str], token_refresh_endpoint: str
) -> dict[str, str]:
    return {**base_configs, "token_refresh_endpoint": token_refresh_endpoint}


@pytest.fixture(scope="session", autouse=True)
def config(base_configs: dict[str, str], base_url: str) -> dict[str, str]:
    return {**base_configs, "base_url": base_url, "api_version": "29"}


@pytest.fixture(scope="session", autouse=True)
def join_url_fragments(config: dict[str, str]) -> Callable[[str], str]:
    return Dhis2Client(**config)._join_url_fragments


@pytest.fixture(scope="session", autouse=True)
def data_elements_url(join_url_fragments: Callable[[str], str]) -> str:
    return join_url_fragments(DATA_ELEMENTS_PATH)


@pytest.fixture(scope="session", autouse=True)
def sample_access_token() -> str:
    return "01-example-access-token-23"
