from pathlib import Path

import requests
from ruamel.yaml import YAML

BASE_URL = "https://platform.beauhurst.com/_api/v1"

DATA = Path("../data")


def authenticate(
        extra_headers=None, secrets_filename: str = "beauhurst_auth.yaml"
):
    """

    :param extra_headers:
    :param secrets_filename:
    :return:
    """

    if extra_headers is None:
        extra_headers = {}
    yaml = YAML(typ="safe")
    secrets = yaml.load(DATA / f"{secrets_filename}")
    headers = {
        "Content-type": "application/json",
        "Authorization": f'apikey {secrets["apikey"]}',
    } | extra_headers
    return headers


def call_api(endpoint: str, headers: dict, params=None) -> dict:
    """

    :param params:
    :param endpoint:
    :param headers:
    :return:
    """

    if params is None:
        params = {}
    url = BASE_URL + endpoint

    r = requests.get(url=url, headers=headers, params=params)
    if r.status_code == 200:
        return r.json()
    else:
        raise Exception(f"APi Call failed {r.status_code}")
