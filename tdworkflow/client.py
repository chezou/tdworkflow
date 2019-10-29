import os
from typing import Dict, Optional

import requests
from requests.exceptions import HTTPError


class Client:
    def __init__(self, site: str, apikey: Optional[str] = None) -> None:
        """Treasure Workflow REST API client

        :param site: Site for Treasure Workflow. {"us", "eu01", "jp"}
        :type site: str
        :param apikey: Treasure Data API key, defaults to None
        :type apikey: Optional[str], optional
        :raises ValueError: If ``site`` is unknown name.
        :raises ValueError: If ``apikey`` is empty and environment variable
                            ``TD_API_KEY`` doesn't exist
        """
        self.site = site

        if site == "us":
            self.endpoint = "api-workflow.treasuredata.com"
        elif site == "jp":
            self.endpoint = "api-workflow.treasuredata.co.jp"
        elif site == "eu01":
            self.endpoint = "api-workflow.eu01.treasuredata.com"
        else:
            raise ValueError(f"Unknown site: {site}. Use 'us', 'jp', or 'eu01'")

        self.apikey = apikey
        if self.apikey is None:
            self.apikey = os.getenviron["TD_API_KEY"]
            if self.apikey is None:
                raise ValueError(
                    f"apikey must be set or should be passed"
                    "by TD_API_KEY in environment variable."
                )

        self.api_base = f"https://{self.endpoint}/api/"
        self.header = {"Authorization": f"TD1 {apikey}"}

    def get(self, path: str, params: Optional[Dict[str, str]] = None) -> Dict[str, str]:
        """GET operator for REST API

        :param path: Treasure Workflow API path
        :type path: str
        :param params: Query parameters, defaults to None
        :type params: Optional[Dict[str, str]], optional
        :return: Response data got with JSON
        :rtype: Dict[str, str]
        """
        url = f"{self.api_base}{path}"
        try:
            r = requests.get(url, params=params, headers=self.header)
        except HTTPError as e:
            print(f"HTTP error occurred:  {e}")
        except Exception as e:
            print(f"Other error occurred: {e}")

        return r.json()

    def put(self, path: str, body: Dict[str, str]) -> bool:
        """PUT operator for REST API

        :param path: Treasure Workflow API path
        :type path: str
        :param body: Content body
        :type body: Dict[str, str]
        :return: ``True`` if succeeded
        :rtype: bool
        """
        url = f"{self.api_base}{path}"
        _header = self.header.copy()
        _header["content-type"] = "application/json"
        try:
            requests.put(url, json=body, headers=_header)
        except HTTPError as e:
            print(f"HTTP error occurred:  {e}")
            return False
        except Exception as e:
            print(f"Other error occurred: {e}")
            return False

        return True

    def delete(self, path: str, params: Optional[Dict[str, str]] = None) -> bool:
        """DELETE operator for REST API

        :param path: Treasure Workflow API path
        :type path: str
        :param params: Query parameters, defaults to None
        :type params: Optional[Dict[str, str]], optional
        :return: ``True`` if succeeded
        :rtype: bool
        """
        url = f"{self.api_base}{path}"
        try:
            requests.delete(url, params=params, headers=self.header)
        except HTTPError as e:
            print(f"HTTP error occurred:  {e}")
            return False
        except Exception as e:
            print(f"Other error occurred: {e}")
            return False

        return True
