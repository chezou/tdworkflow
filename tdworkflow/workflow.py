from typing import Dict, List

from .client import Client


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
