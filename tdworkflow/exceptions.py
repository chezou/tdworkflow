from typing import NoReturn

import requests


class HttpError(Exception):
    pass


def raise_response_error(r: requests.Response) -> NoReturn | None:
    try:
        r.raise_for_status()
        return None
    except requests.exceptions.HTTPError as e:
        response = {}
        if r.content and "application/json" in r.headers.get("Content-Type", ""):
            response = r.json()
        message = response.get("message", "")
        raise HttpError(f"{e}\n{message}")
