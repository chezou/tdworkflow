import os
from typing import Dict, List, Optional, Union

import requests
from requests.adapters import HTTPAdapter
from requests.exceptions import HTTPError
from urllib3.util.retry import Retry

import tdworkflow

from .project import Project
from .workflow import Workflow


class WorkflowAPI:
    def workflows(self) -> List[Workflow]:
        """List worlfows

        :return:
        """
        res = self.get("workflows")
        if len(res) > 0:
            return [Workflow(**wf) for wf in res["workflows"]]
        else:
            return []

    def workflow(self, workflow_id: int) -> Workflow:
        """Get a specific workflow

        :param workflow_id: Id for workflow
        :type workflow_id: int
        :return: A workflow
        :rtype: Workflow
        """
        res = self.get(f"workflows/{workflow_id}")
        return Workflow(**res)


class ProjectAPI:
    def project(self, project_id: int) -> Project:
        r = self.get(f"projects/{project_id}")
        return Project(r.json())

    def projects(self, name: Optional[str] = None) -> List[Project]:
        params = None
        if name:
            params = {"name": name}

        res = self.get(f"projects", params=params)
        if res:
            return [Project(**proj) for proj in res["projects"]]
        else:
            return []

    def project_workflows(self, project_id: int) -> List[Workflow]:
        res = self.get(f"projects/{project_id}/workflows")
        if res:
            return [Workflow(**wf) for wf in res["workflows"]]
        else:
            return []

    def project_workflows_by_name(self, name: str) -> List[Workflow]:
        projects = self.projects(name)
        if len(projects) == 0:
            raise ValueError(f"Unable to find project name {name}")

        return self.project_workflows(projects[0].id)

    def set_secrets(
        self, project: Union[int, Project], secrets: Dict[str, str]
    ) -> bool:
        """Set project secrets

        :param project: Project ID or Project object
        :type project: Union[int, Project]
        :param secrets: Workflow secrets
        :type secrets: Dict[str, str]
        :return: ``True`` if succeeded
        :rtype: bool
        """
        succeeded = True
        project_id = project.id if isinstance(project, Project) else project

        for k, v in secrets.items():
            r = self.put(f"projects/{project_id}/secrets/{k}", body={"value": v})
            if r:
                print(f"Succeeded to set secret for {k}")
            else:
                succeeded = False
                print(f"Failed to set secret for {k}")

        return succeeded

    def secrets(self, project: Union[int, Project]) -> List[str]:
        """Show secret keys

        :param project: Project ID or Project object
        :type project: Union[int, Project]
        :return: The list of secret keys
        :rtype: List[str]
        """
        project_id = project.id if isinstance(project, Project) else project
        r = self.get(f"projects/{project_id}/secrets/")
        if r is None or len(r) == 0:
            return []
        else:
            return [e["key"] for e in r["secrets"]]

    def delete_secret(self, project: Union[int, Project], key: str) -> bool:
        """Delete secret key

        :param project: Project ID or Project object
        :type project: Union[int, Project]
        :param key: Secret key to be deleted
        :type key: str
        :return: ``True`` if succeeded
        :rtype: bool
        """
        old_secret_keys = self.secrets()
        if key not in old_secret_keys:
            print(f"Secret key {key} doesn't exist")
            return False

        project_id = project.id if isinstance(project, Project) else project
        r = self.delete(f"projects/{project_id}/secrets/{key}")
        if r:
            print(f"Succeeded to delete secret: {key}")
            return True

        return False

    def delete_secrets(self, project: Union[int, Project], keys: List[str]) -> bool:
        """Delete multiple secret keys at once

        :param project: Project ID or Project object
        :type project: Union[int, Project]
        :param keys: The list of secret keys to be deleted
        :type keys: List[str]
        :return: ``True`` if succeeded
        :rtype: bool
        """
        if len(keys) == 0:
            return False

        succeeded = True
        project_id = project.id if isinstance(project, Project) else project
        for key in keys:
            r = self.delete_secret(project_id, key)
            if not r:
                succeeded = False

        return succeeded


class Client(WorkflowAPI, ProjectAPI):
    def __init__(
        self,
        site: str,
        apikey: Optional[str] = None,
        user_agent: Optional[str] = None,
        _session: Optional[requests.Session] = None,
    ) -> None:
        """Treasure Workflow REST API client

        :param site: Site for Treasure Workflow. {"us", "eu01", "jp"}
        :type site: str
        :param apikey: Treasure Data API key, defaults to None
        :type apikey: Optional[str], optional
        :param user_agent: User-Agent for request header
        :type user_agent: Optional[str], optional
        :param _session: HTTP object to make requests
        :type _session: Optional[requests.Session]
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
            self.apikey = os.getenv("TD_API_KEY")
            if self.apikey is None:
                raise ValueError(
                    f"apikey must be set or should be passed"
                    "by TD_API_KEY in environment variable."
                )

        if _session is None:
            _session = requests.Session()
            user_agent = user_agent or f"tdworkflow/{tdworkflow.__version__}"
            _session.headers.update(
                {"Authorization": f"TD1 {self.apikey}", "User-Agent": user_agent}
            )

        retries = Retry(
            total=5, backoff_factor=1, status_forcelist=[500, 502, 503, 504]
        )

        _session.mount("https://", HTTPAdapter(max_retries=retries))
        _session.mount("http://", HTTPAdapter(max_retries=retries))

        self._session = _session
        self.api_base = f"https://{self.endpoint}/api/"

    @property
    def session(self):
        """Established session

        :return:
        """
        return self._session

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
            r = self._session.get(url, params=params)
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
        headers = {"content-type": "application/json"}
        try:
            self._session.put(url, json=body, headers=headers)
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
            self.session.delete(url, params=params)
        except HTTPError as e:
            print(f"HTTP error occurred:  {e}")
            return False
        except Exception as e:
            print(f"Other error occurred: {e}")
            return False

        return True
