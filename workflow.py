import os

from typing import Optional, Dict, List

import requests

class Client:
    def __init__(self, site: str, apikey: Optional[str] = None) -> None:
        """Treasure Workflow REST API client

        :param site: Site for Treasure Workflow. {"us", "eu01", "jp"}
        :type site: str
        :param apikey: Treasure Data API key, defaults to None
        :type apikey: Optional[str], optional
        :raises ValueError: If ``site`` is unknown name.
        :raises ValueError: If ``apikey`` is empty and environment variable ``TD_API_KEY`` doesn't exist
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
                raise ValueError(f"apikey must be set or should be passed by TD_API_KEY in environment variable.")

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
            r = requests.put(url, json=body, headers=_header)
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
            r = requests.delete(url, params=params, headers=self.header)
        except HTTPError as e:
            print(f"HTTP error occurred:  {e}")
            return False
        except Exception as e:
            print(f"Other error occurred: {e}")
            return False

        return True


class Workflow:
    def __init__(self, client: Client, project_name: str) -> None:
        """An object represents workflow

        :param client: Treasure Workflow API Client
        :type client: Client
        :param project_name: Workflow project name
        :type project_name: str
        :raises ValueError: Raise if ``project_name`` doesn't exist
        """
        self.client = client

        r = self.client.get("projects", params={"name": project_name})
        if r is None or len(r["projects"]) == 0:
            raise ValueError(f"Can't find project: {project_name}")

        project = r["projects"][0]
        self.id = int(project["id"])
        self.name = project["name"]
        self.revision = project["revision"]
        self.archive_type = project["archiveType"]
        self.archive_md5 = project["archiveMd5"]
        self.created_at = project["createdAt"]
        self.deleted_at = project["deletedAt"]
        self.updated_at = project["updatedAt"]

    def set_secrets(self, secrets: Dict[str, str]) -> bool:
        """Set project secrets

        :param secrets: Workflow secrets
        :type secrets: Dict[str, str]
        :return: ``True`` if succeeded
        :rtype: bool
        """
        succeeded = True
        for k, v in secrets.items():
            r = self.client.put(f"projects/{self.id}/secrets/{k}", body={"value": v})
            if r:
                print(f"Succeeded to set secret for {k}")
            else:
                succeeded = False
                print(f"Failed to set secret for {k}")

        return succeeded

    def secrets(self) -> List[str]:
        """Show secret keys

        :return: The list of secret keys
        :rtype: List[str]
        """
        r = self.client.get(f"projects/{self.id}/secrets/")
        if r is None or len(r) == 0:
            return []
        else:
            return [e["key"] for e in r["secrets"]]

    def delete_secret(self, key: str) -> bool:
        """Delete secret key

        :param key: Secret key to be deleted
        :type key: str
        :return: ``True`` if succeeded
        :rtype: bool
        """
        old_secret_keys = self.secrets()
        if key not in old_secret_keys:
            print(f"Secret key {key} doesn't exist")
            return False

        r = self.client.delete(f"projects/{self.id}/secrets/{key}")
        if r:
            print(f"Succeeded to delete secret: {key}")
            return True

        return False

    def delete_secrets(self, keys: List[str]) -> bool:
        """Delete multiple secret keys at once

        :param keys: The list of secret keys to be deleted
        :type keys: List[str]
        :return: ``True`` if succeeded
        :rtype: bool
        """
        if len(keys) == 0:
            return False

        succeeded = True
        for key in keys:
            r = self.delete_secret(key)
            if not r:
                succeeded = False

        return succeeded
