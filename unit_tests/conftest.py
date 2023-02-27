from urllib.parse import urljoin

import pytest

from destination_dhis2.constants import TOKEN_REFRESH_PATH


@pytest.fixture(scope="session", autouse=True)
def base_url():
    return "https://test.com"


@pytest.fixture(scope="session", autouse=True)
def base_configs():
    return {
        "client_id": "001",
        "client_secret": "super-secret-41",
        "refresh_token": "1984",
    }


@pytest.fixture(scope="session", autouse=True)
def config(base_url, base_configs):
    return {"base_url": base_url, **base_configs}


@pytest.fixture(scope="session", autouse=True)
def oauth_configs(base_url, base_configs):
    return {
        "token_refresh_endpoint": urljoin(base_url, TOKEN_REFRESH_PATH),
        **base_configs,
    }


@pytest.fixture(scope="session", autouse=True)
def sample_access_token():
    return "01-example-access-token-23"
