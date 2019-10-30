import gzip
import io
import json
import os
import time
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, BinaryIO, Dict, List, Optional, Tuple, Union

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

import tdworkflow

from .attempt import Attempt
from .log import LogFile
from .project import Project
from .revision import Revision
from .schedule import Schedule, ScheduleAttempt
from .session import Session
from .util import archive_files
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

    def workflow(self, workflow: Union[int, Workflow]) -> Workflow:
        """Get a specific workflow

        :param workflow: Id for workflow or Workflow object
        :type workflow: Union[int, Workflow]
        :return: A workflow
        :rtype: Workflow
        """
        workflow_id = workflow.id if isinstance(workflow, Workflow) else workflow
        res = self.get(f"workflows/{workflow_id}")
        return Workflow(**res)


class ProjectAPI:
    def project(self, project: Union[int, Project]) -> Project:
        project_id = project.id if isinstance(project, Project) else project
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

    def project_workflows(self, project: Union[int, Project]) -> List[Workflow]:
        project_id = project.id if isinstance(project, Project) else project
        res = self.get(f"projects/{project_id}/workflows")
        if res:
            return [Workflow(**wf) for wf in res["workflows"]]
        else:
            return []

    def create_project(
        self,
        project_name: str,
        target_dir: str,
        exclude_patterns: Optional[List[str]] = None,
        revision: Optional[str] = None,
    ) -> Project:
        revision = revision or str(uuid.uuid4())
        params = {"project": project_name, "revision": revision}

        default_excludes = ["venv", ".venv", "__pycache__", ".egg-info", ".digdag"]
        if exclude_patterns:
            exclude_patterns.extend(default_excludes)
        else:
            exclude_patterns = default_excludes
        data = archive_files(target_dir, exclude_patterns)
        r = self.put("projects", params=params, data=data)

        if r:
            return Project(**r)
        else:
            raise ValueError("Unable to crate project")

    def delete_project(self, project: Union[int, Project]) -> bool:
        project_id = project.id if isinstance(project, Project) else project
        res = self.delete(f"projects/{project_id}")
        if res:
            return True
        else:
            return False

    def download_project_archive(
        self,
        project: Union[int, Project],
        file_path: str,
        revision: Optional[str] = None,
    ) -> bool:
        params = {"revision": revision} if revision else {}
        project_id = project.id if isinstance(project, Project) else project
        res = self.get(f"projects/{project_id}/archive", params=params, content=True)

        # File will be downloaded as tar.gz format
        with open(file_path, "wb") as f:
            f.write(res)

        return True

    def project_workflows_by_name(self, project_name: str) -> List[Workflow]:
        projects = self.projects(project_name)
        if len(projects) == 0:
            raise ValueError(f"Unable to find project name {project_name}")

        return self.project_workflows(projects[0].id)

    def project_revisions(self, project: Union[int, Project]) -> List[Revision]:
        project_id = project.id if isinstance(project, Project) else project
        res = self.get(f"projects/{project_id}/revisions")
        if res:
            return [Revision(**rev) for rev in res["revisions"]]
        else:
            return []

    def project_schedules(
        self,
        project: Union[int, Project],
        workflow: Optional[Union[str, Workflow]] = None,
        last_id: Optional[int] = None,
    ) -> List[Schedule]:
        params = {}
        if workflow:
            workflow_name = (
                workflow.name if isinstance(workflow, Workflow) else workflow
            )
            params["workflow"] = workflow_name
        if last_id:
            params["last_id"] = last_id
        project_id = project.id if isinstance(project, Project) else project
        res = self.get(f"projects/{project_id}/schedules", params=params)
        if res:
            return [Schedule(**s) for s in res["schedules"]]
        else:
            return []

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

    def project_sessions(
        self,
        project: Union[int, Project],
        workflow: Optional[Union[str, Workflow]] = None,
        last_id: Optional[int] = None,
        page_size: Optional[int] = None,
    ) -> List[Session]:
        params = {}
        if workflow:
            workflow_name = (
                workflow.name if isinstance(workflow, Workflow) else workflow
            )
            params["workflow"] = workflow_name
        if last_id:
            params["last_id"] = last_id
        if page_size:
            params["page_size"] = page_size
        project_id = project.id if isinstance(project, Project) else project
        r = self.get(f"projects/{project_id}/sessions")
        if r:
            return [Session(**s) for s in r["sessions"]]
        else:
            return []

    def project_workflows(
        self,
        project: Union[int, Project],
        workflow: Optional[Union[str, Workflow]] = None,
        revision: Optional[str] = None,
    ) -> List[Workflow]:
        params = {}
        if workflow:
            workflow_name = (
                workflow.name if isinstance(workflow, Workflow) else workflow
            )
            params["workflow"] = workflow_name
        if revision:
            params["revision"] = revision
        project_id = project.id if isinstance(project, Project) else project
        r = self.get(f"projects/{project_id}/workflows", params=params)
        if r:
            return [Workflow(**wf) for wf in r["workflows"]]
        else:
            return []


class AttemptAPI:
    def attempts(
        self,
        project: Optional[Union[str, Project]] = None,
        workflow: Optional[Union[str, Workflow]] = None,
        include_retried: Optional[bool] = None,
        last_id: Optional[int] = None,
        page_size: Optional[int] = None,
    ) -> List[Attempt]:
        """List attempts

        :param project: Project name or Project object, optional
        :type project: Optional[Union[str, Project]]
        :param workflow: Workflow name or Workflow object, optional
        :type workflow: Optional[Union[str, Workflow]]
        :param include_retried: Flag to include retried
        :type include_retried: Optional[bool]
        :param last_id: Last ID
        :type last_id: Optional[int]
        :param page_size: Page size
        :type page_size: Optional[int]
        :return: List of Attempt object
        :rtype: List[Attempt]
        """
        params = {}
        if project:
            project_name = project.name if isinstance(project, Project) else project
            params.update({"project": project_name})
        if workflow:
            workflow_name = (
                workflow.name if isinstance(workflow, Workflow) else workflow
            )
            params.upadte({"workflow": workflow_name})
        if include_retried:
            params.update({"include_retried": include_retried})
        if last_id:
            params.update({"last_id": last_id})
        if page_size:
            params.update({"page_size": page_size})

        r = self.get("attempts", params=params)
        res = [Attempt(**attempt) for attempt in r["attempts"]] if r else []
        return res

    def attempt(self, attempt: Union[int, Attempt]) -> Attempt:
        """Get an attempt

        :param attempt: Attempt ID or Attempt object
        :type attempt: int
        :return: Attempt object
        :rtype: :class:`Attempt`
        """
        attempt_id = attempt.id if isinstance(attempt, Attempt) else attempt
        r = self.get(f"attempts/{attempt_id}")
        if not r:
            raise ValueError(f"Unable to find attempt id {attempt_id}")

        return Attempt(**r)

    def retried_attempts(self, attempt: Union[int, Attempt]) -> List[Attempt]:
        """Get retried attempt list

        :param attempt: Attempt id or Attempt object
        :return: List of Attempt
        """

        attempt_id = attempt.id if isinstance(attempt, Attempt) else attempt
        r = self.get(f"attempts/{attempt_id}/retries")
        res = [Attempt(**attempt) for attempt in r["attempts"]] if r else []
        return res

    def start_attempt(
        self,
        workflow: Union[int, Workflow],
        session_time: Optional[str] = None,
        retry_attempt_name: Optional[str] = None,
        workflow_params: Optional[Dict[str, Any]] = None,
    ) -> Attempt:
        """Start workflow session

        :param workflow: Workflow id or Workflow object
        :param session_time: Session time, optional
        :param retry_attempt_name: Retry attempt name, optional
        :param workflow_params: Extra workflow parameters
        :return:
        """
        workflow_id = workflow.id if isinstance(workflow, Workflow) else workflow
        _params = {"workflowId": workflow_id}
        workflow_params = workflow_params if workflow_params else {}
        _params.update({"params": workflow_params})
        if not session_time:
            utc = timezone(timedelta(), "UTC")
            session_time = datetime.now(utc).isoformat()
        if retry_attempt_name:
            _params.update({"retryAttemptName": retry_attempt_name})

        _params["sessionTime"] = session_time
        r = self.put("attempts", _json=_params)
        if r:
            return Attempt(**r)
        else:
            raise ValueError("Unable to start attempt")

    def kill_attempt(self, attempt: Union[int, Attempt]) -> bool:
        """Kill a session

        :param attempt: Attempt ID or Attempt object
        :type attempt: Union[int, Attempt]
        :return: ``True`` if succeeded
        :rtype: bool
        """
        attempt_id = attempt.id if isinstance(attempt, Attempt) else attempt
        r = self.post(f"attempts/{attempt_id}/kill")
        return r

    def wait_attempt(
        self, attempt: Union[int, Attempt], wait_interval: int = 5
    ) -> Attempt:
        """Wait until an attempt finished

        :param attempt: Attempt ID or Attempt object
        :type attempt: Union[int, Attempt]
        :param wait_interval: Wait interval in second. Default 5 sec
        :type wait_interval: int
        :return: Latest status of Attempt
        :rtype: Attempt
        """
        while not attempt.done:
            time.sleep(wait_interval)
            attempt = self.attempt(attempt)

        return attempt


class ScheduleAPI:
    def schedules(self, last_id: Optional[int] = None) -> List[Schedule]:
        r = self.get("schedules", params={"last_id": last_id})
        if r:
            return [Schedule(**s) for s in r["schedules"]]
        else:
            return []

    def schedule(self, schedule: Union[int, Schedule]) -> Schedule:
        schedule_id = schedule.id if isinstance(schedule, Schedule) else schedule
        r = self.get(f"schedules/{schedule_id}")
        if r:
            return Schedule(**r)
        else:
            raise ValueError(f"Unable to find schedule id: {schedule_id}")

    def backfill_schedule(
        self,
        schedule: Union[int, Schedule],
        from_time: Optional[str] = None,
        attempt_name: Optional[str] = None,
        count: Optional[int] = None,
        dry_run: Optional[bool] = None,
    ) -> ScheduleAttempt:
        params = {}
        if from_time:
            params["fromTime"] = from_time
        if attempt_name:
            params["attemptName"] = attempt_name
        if count:
            params["count"] = count
        if dry_run:
            params["dryRun"] = dry_run

        schedule_id = schedule.id if isinstance(schedule, Schedule) else schedule
        r = self.post(f"schedules/{schedule_id}/backfill", body=params)
        if r:
            return ScheduleAttempt(**r)
        else:
            raise ValueError(f"Unable to backfill for schedule: {schedule_id}")

    def disable_schedule(self, schedule: Union[int, Schedule]) -> Schedule:
        schedule_id = schedule.id if isinstance(schedule, Schedule) else schedule
        r = self.post(f"schedules/{schedule_id}/disable")
        if r:
            return Schedule(**r)
        else:
            raise ValueError(f"Unable to disable schedule id: {schedule_id}")

    def enable_schedule(self, schedule: Union[int, Schedule]) -> Schedule:
        schedule_id = schedule.id if isinstance(schedule, Schedule) else schedule
        r = self.post(f"schedules/{schedule_id}/enable")
        if r:
            return Schedule(**r)
        else:
            raise ValueError(f"Unable to enable schedule id: {schedule_id}")

    def skip_schedule(
        self,
        schedule: Union[int, Schedule],
        from_time: Optional[str] = None,
        next_time: Optional[str] = None,
        next_run_time: Optional[str] = None,
        dry_run: Optional[bool] = False,
    ) -> Schedule:
        params = {}
        if from_time:
            params["fromTime"] = from_time
        if next_time:
            params["nextTime"] = next_time
        if next_run_time:
            params["nextRunTime"] = next_run_time
        if dry_run:
            params["dryRun"] = dry_run
        schedule_id = schedule.id if isinstance(schedule, Schedule) else schedule
        r = self.post(f"schedules/{schedule_id}/skip", body=params)
        if r:
            return Schedule(**r)
        else:
            raise ValueError(f"Unable to skip schedule id: {schedule_id}")


class SessionAPI:
    def sessions(
        self, last_id: Optional[int] = None, page_size: Optional[int] = None
    ) -> List[Session]:
        params = {}
        if last_id:
            params["last_id"] = last_id
        if page_size:
            params["page_size"] = page_size

        r = self.get("sessions", params=params)
        if r:
            return [Session(**s) for s in r["attempts"]]
        else:
            return []

    def session(self, session: Union[int, Session]) -> Session:
        session_id = session.id if isinstance(session, Session) else session
        r = self.get(f"sessions/{session_id}")
        if r:
            return Session(**r)
        else:
            raise ValueError(f"Unable to get sesesion id: {session_id}")

    def session_attempts(
        self,
        session: Union[int, Session],
        last_id: Optional[int] = None,
        page_size: Optional[int] = None,
    ) -> List[Attempt]:
        params = {}
        if last_id:
            params["last_id"] = last_id
        if page_size:
            params["page_size"] = page_size

        session_id = session.id if isinstance(session, Session) else session
        r = self.get(f"sessions/{session_id}/attempts", params=params)
        if r:
            return [Attempt(**e) for e in r["attempts"]]
        else:
            return []


class LogAPI:
    def log_files(
        self,
        attempt: Union[Attempt, int],
        task: Optional[str] = None,
        direct_download: Optional[bool] = None,
    ) -> List[LogFile]:
        params = {}
        if task:
            params["task"] = task
        if direct_download:
            params["direct_download"] = True

        attempt_id = attempt.id if isinstance(attempt, Attempt) else attempt
        r = self.get(f"logs/{attempt_id}/files")
        if r:
            return [LogFile(**l) for l in r["files"]]
        else:
            return []

    def log_file(self, attempt: Union[Attempt, int], file: Union[LogFile, str]) -> str:

        attempt_id = attempt.id if isinstance(attempt, Attempt) else attempt
        file_name = file.file_name if isinstance(file, LogFile) else file
        r = self.get(f"logs/{attempt_id}/files/{file_name}", content=True)
        if r:
            gzfile = io.BytesIO(r)
            with gzip.open(gzfile, "rt") as f:
                return f.read()
        else:
            raise ValueError(f"Unable to get file: {file_name}")

    def logs(self, attempt: Union[Attempt, int]) -> List[str]:
        """Get log string for an attempt

        :param attempt: Attempt ID or Attempt object
        :return: A list of log

        .. code-block:: python

           >>> import tdworkflow
           >>> client = tdworkflow.client.Client("us")
           >>> attempts = client.attempts(project="pandas-df")
           >>> logs = client.logs(attempts[0])
           >>> print(logs)
           ['2019-10-30 08:34:51.672 +0000 [INFO] (0250@[1:pandas-df]+pandas-df+read_into_df) io.digdag.core.agent.OperatorManager: py>: py_scripts.examples.read_td_table\n',
           '2019-10-30 08:34:59.879 +0000 [INFO] (0237@[1:pandas-df]+pandas-df+read_into_df) io.digdag.core.agent.OperatorManager: py>: py_scripts.examples.read_td_table\nWait running a command task: status provisioning',
           ...
        """  # noqa

        files = self.log_files(attempt)
        logs = []
        for file in files:
            logs.append(self.log_file(attempt, file))

        return logs


class Client(AttemptAPI, WorkflowAPI, ProjectAPI, ScheduleAPI, SessionAPI, LogAPI):
    def __init__(
        self,
        site: Optional[str] = "us",
        endpoint: Optional[str] = None,
        apikey: Optional[str] = None,
        user_agent: Optional[str] = None,
        _session: Optional[requests.Session] = None,
    ) -> None:
        """Treasure Workflow REST API client

        :param site: Site for Treasure Workflow. {"us", "eu01", "jp"} default: "us"
                     `site` or `endpoint` must be set.
        :type site: Optional[str], optional
        :param endpoint: Treasure Data Workflow endpoint
        :type endpoint: Optional[str], optional
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

        if endpoint:
            self.endpoint = endpoint
        elif site == "us":
            self.endpoint = "api-workflow.treasuredata.com"
        elif site == "jp":
            self.endpoint = "api-workflow.treasuredata.co.jp"
        elif site == "eu01":
            self.endpoint = "api-workflow.eu01.treasuredata.com"
        else:
            raise ValueError(
                f"Unknown site: {site}. Use 'us', 'jp', or 'eu01' "
                "or you need to set endpoint"
            )

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

        self._http = _session
        self.api_base = f"https://{self.endpoint}/api/"

    @property
    def http(self):
        """
        :return: Established session
        :rtype: requests.Session
        """
        return self._http

    def get(
        self, path: str, params: Optional[Dict[str, str]] = None, content: bool = False
    ) -> Dict[str, str]:
        """GET operator for REST API

        :param path: Treasure Workflow API path
        :type path: str
        :param params: Query parameters, defaults to None
        :type params: Optional[Dict[str, str]], optional
        :param content: Return content if ``True``
        :type content: bool
        :return: Response data got with JSON
        :rtype: Dict[str, str]
        """
        url = f"{self.api_base}{path}"
        r = self.http.get(url, params=params)

        if not 200 <= r.status_code < 300:
            if len(r.text) > 0:
                print(r.json())
            raise r.raise_for_status()
        elif content:
            return r.content
        else:
            return r.json()

    def post(self, path: str, body: Optional[Dict[str, str]] = None) -> bool:
        """POST operator for REST API

        :param path: Treasure Workflow API path
        :type path: str
        :param body:
        :type body: Optional[Dict[str, str]], optional
        :return: ``True`` if succeeded
        """
        url = f"{self.api_base}{path}"
        r = self.http.post(url, json=body)

        if not 200 <= r.status_code < 300:
            if len(r.text) > 0:
                print(r.json())
            raise r.raise_for_status()
        else:
            return r.json()

    def put(
        self,
        path: str,
        data: Optional[Union[Dict, List[Tuple], BinaryIO]] = None,
        _json: Optional[Dict[str, str]] = None,
        params: Optional[Dict[str, str]] = None,
    ) -> Optional[Dict[str, str]]:
        """PUT operator for REST API

        :param path: Treasure Workflow API path
        :type path: str
        :param data: Content body
        :type data: Optional[Union[Dict, List[Tuple], BinaryIO]], oprional
        :param _json: Content body as JSON
        :type _json: Optional[Dict[str, str]], optional
        :param params: Query parameters
        :type params: Optional[Dict[str, str]], optional
        :return: Response content
        :rtype: Dict[str,str]
        """
        url = f"{self.api_base}{path}"
        headers = {}
        if _json:
            headers["Content-Type"] = "application/json"
            data = json.dumps(_json)
        if not _json and data and hasattr(data, "read"):
            headers["Content-Type"] = "application/gzip"

        r = self.http.put(url, data=data, headers=headers, params=params)

        if not 200 <= r.status_code < 300:
            if len(r.text) > 0:
                print(r.json())
            raise r.raise_for_status()
        else:
            return r.json()

    def delete(
        self, path: str, params: Optional[Dict[str, str]] = None
    ) -> Optional[Dict[str, str]]:
        """DELETE operator for REST API

        :param path: Treasure Workflow API path
        :type path: str
        :param params: Query parameters, defaults to None
        :type params: Optional[Dict[str, str]], optional
        :return: ``True`` if succeeded
        :rtype: bool
        """
        url = f"{self.api_base}{path}"

        r = self.http.delete(url, params=params)

        if not 200 <= r.status_code < 300:
            if len(r.text) > 0:
                print(r.json())
            raise r.raise_for_status()
        else:
            return r.json()
