import gzip
import io
import json
import logging
import os
import time
import uuid
from datetime import datetime
from typing import Any, BinaryIO, Dict, List, Optional, Tuple, Union

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

import tdworkflow

from . import exceptions
from .attempt import Attempt
from .log import LogFile
from .project import Project
from .revision import Revision
from .schedule import Schedule, ScheduleAttempt
from .session import Session
from .task import Task
from .util import archive_files, to_iso8601
from .workflow import Workflow

logger = logging.getLogger(__name__)


class WorkflowAPI:
    def workflows(
        self,
        name_pattern: Optional[str] = None,
        search_project_name: bool = False,
        order: Optional[str] = None,
        count: Optional[int] = None,
        last_id: Optional[int] = None,
    ) -> List[Workflow]:
        """List worlfows

        :param name_pattern: Name pattern to be partially matched
        :type name_pattern: Optional[str], optional
        :param search_project_name: Flag to use name_pattern to search partial project name. Default False
        :type search_project_name: bool
        :param order: Sort order. 'asc' or 'dsc'. Default 'asc'
        :type order: Optional[str]
        :param count: Number of workflows to return
        :type count: Optional[int], optional
        :param last_id: List workflows whose id is grater than this id for pagination.
        :type last_id: Optional[int], optional
        :return: List of Workflow
        :rtype: List[Workflow]
        """
        params = {}
        if name_pattern:
            params["name_pattern"] = name_pattern
        if search_project_name:
            params["search_project_name"] = search_project_name
        if order:
            params["order"] = order
        if count:
            params["count"] = count
        if last_id:
            params["last_id"] = last_id
        res = self.get("workflows", params=params)
        if len(res) > 0:
            return [Workflow.from_api_repr(**wf) for wf in res["workflows"]]
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
        return Workflow.from_api_repr(**res)


class ProjectAPI:
    def project(self, project: Union[int, Project]) -> Project:
        """Get a project

        :param project: Project id or Project object
        :type project: Union[int, Project]
        :return: A Project
        """
        project_id = project.id if isinstance(project, Project) else project
        r = self.get(f"projects/{project_id}")
        return Project.from_api_repr(**r)

    def projects(
        self,
        name: Optional[str] = None,
        name_pattern: Optional[str] = None,
        count: Optional[int] = None,
        last_id: Optional[int] = None,
    ) -> List[Project]:
        """List projects

        :param name: Project name
        :type name: Optional[str], optional
        :param name_pattern: Name pattern to be partially matched
        :type name_pattern: Optional[str], optional
        :param count: Number of projects to return
        :type count: Optional[int], optional
        :param last_id: List projects whose id is grater than this id for pagination.
        :type last_id: Optional[int], optional
        :return: List of Project
        :rtype: List[Project]
        """
        params = {}
        if name:
            params["name"] = name
        if name_pattern:
            params["name_pattern"] = name_pattern
        if count:
            params["count"] = count
        if last_id:
            params["last_id"] = last_id

        res = self.get("projects", params=params)
        if res:
            return [Project.from_api_repr(**proj) for proj in res["projects"]]
        else:
            return []

    def project_workflows(
        self,
        project: Union[int, Project],
        workflow: Optional[Union[str, Workflow]] = None,
        revision: Optional[str] = None,
    ) -> List[Workflow]:
        """Get workflows associated with a project

        :param project: Project id or Project object
        :type project: Union[int, Project]
        :param workflow: Workflow name or Workflow object
        :type project: Optional[Union[str, Workflow]], optional
        :param revision: Revision name
        :type revision: Optional[str], optional
        :return: List of Workflow
        :rtype: List[Workflow]
        """
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
            return [Workflow.from_api_repr(**wf) for wf in r["workflows"]]
        else:
            return []

    def create_project(
        self,
        project_name: str,
        target_dir: str,
        exclude_patterns: Optional[List[str]] = None,
        revision: Optional[str] = None,
    ) -> Project:
        """Create a new project

        :param project_name: Project name
        :param target_dir: Target directory name
        :param exclude_patterns: Exclude file patterns. They are treated as regexp
                                 patterns.
                                 default: ["venv", ".venv", "__pycache__", ".egg-info",\
                                  ".digdag", ".pyc"] + dot files
        :param revision: Revision name
        :return:
        """
        revision = revision or str(uuid.uuid4())
        params = {"project": project_name, "revision": revision}

        default_excludes = [
            "venv",
            ".venv",
            "__pycache__",
            ".egg-info",
            ".digdag",
            ".pyc",
        ]
        if exclude_patterns:
            exclude_patterns.extend(default_excludes)
        else:
            exclude_patterns = default_excludes
        data = archive_files(target_dir, exclude_patterns)
        r = self.put("projects", params=params, data=data)

        if r:
            return Project.from_api_repr(**r)
        else:
            raise ValueError("Unable to crate project")

    def delete_project(self, project: Union[int, Project]) -> bool:
        """Delete a project

        :param project: Project id or Project object
        :return: ``True`` if succeeded
        """
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
        """Download a project and save as a file (tar.gz)

        :param project: Project id or Project object
        :param file_path: Target file path to be saved in tar.gz
        :param revision: Revision name
        :return: ``True`` if succeeded
        """
        params = {"revision": revision} if revision else {}
        project_id = project.id if isinstance(project, Project) else project
        res = self.get(f"projects/{project_id}/archive", params=params, content=True)

        # File will be downloaded as tar.gz format
        with open(file_path, "wb") as f:
            f.write(res)

        return True

    def project_workflows_by_name(self, project_name: str) -> List[Workflow]:
        """List workflows associate with Project by project name

        :param project_name: Target project name
        :return: List of Workflow
        """
        projects = self.projects(project_name)
        if not projects:
            raise ValueError(f"Unable to find project name {project_name}")

        return self.project_workflows(projects[0].id)

    def project_revisions(self, project: Union[int, Project]) -> List[Revision]:
        """List revisions associated with Project

        :param project: Project id or Project object
        :return: List of Revision
        """
        project_id = project.id if isinstance(project, Project) else project
        res = self.get(f"projects/{project_id}/revisions")
        if res:
            return [Revision.from_api_repr(**rev) for rev in res["revisions"]]
        else:
            return []

    def project_schedules(
        self,
        project: Union[int, Project],
        workflow: Optional[Union[str, Workflow]] = None,
        last_id: Optional[int] = None,
    ) -> List[Schedule]:
        """List schedules associated with Project

        :param project: Project ID or project object
        :param workflow: Workflow name or Workflow object
        :param last_id: List schedules whose id is grater than this id for pagination
        :return: List of Schedule
        """
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
            return [Schedule.from_api_repr(**s) for s in res["schedules"]]
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
            try:
                self.put(f"projects/{project_id}/secrets/{k}", _json={"value": v})
                logger.info(f"Succeeded to set secret for {k}")
            except exceptions.HttpError as e:
                succeeded = False
                logger.warning(f"Failed to set secret for {k}")

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
        old_secret_keys = self.secrets(project)
        if key not in old_secret_keys:
            logger.warning(f"Secret key {key} doesn't exist")
            return False

        project_id = project.id if isinstance(project, Project) else project
        try:
            self.delete(f"projects/{project_id}/secrets/{key}")
            logger.info(f"Succeeded to delete secret: {key}")
            return True
        except exceptions.HttpError as e:
            logger.warning(f"Failed to delete secret: {key}")
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
        """List sessions associated with a Project

        :param project: Project ID or Project object
        :param workflow: Workflow name or Workflow object
        :param last_id: List sessions whose id is grater than this id for pagination
        :param page_size: Number of sessions to return
        :return: List of Session
        """
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
            return [Session.from_api_repr(**s) for s in r["sessions"]]
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
        :param include_retried: List more than 1 attempts per session
        :type include_retried: Optional[bool]
        :param last_id: List attempts whose id is grater than this id for pagination
        :type last_id: Optional[int]
        :param page_size: Number of attempts to return
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
        res = (
            [Attempt.from_api_repr(**attempt) for attempt in r["attempts"]] if r else []
        )
        return res

    def attempt(
        self, attempt: Union[int, Attempt], inplace: bool = False
    ) -> Optional[Attempt]:
        """Get an attempt

        :param attempt: Attempt ID or Attempt object
        :type attempt: int
        :param inplace: If True, do operation inplace and return None
        :return: Attempt object
        :rtype: :class:`Attempt`
        """
        attempt_id = attempt.id if isinstance(attempt, Attempt) else attempt
        r = self.get(f"attempts/{attempt_id}")
        if not r:
            raise ValueError(f"Unable to find attempt id {attempt_id}")

        if inplace:
            attempt.update(**r)
        else:
            return Attempt.from_api_repr(**r)

    def attempt_tasks(self, attempt: Union[int, Attempt]) -> List[Task]:
        """Get tasks of a session

        :param attempt: Attempt id or Attempt object
        :return: List of :class:`Task`
        """

        attempt_id = attempt.id if isinstance(attempt, Attempt) else attempt
        r = self.get(f"attempts/{attempt_id}/tasks")
        res = [Task.from_api_repr(**task) for task in r["tasks"]] if r else []
        return res

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
        :param session_time: Session time, optional Default: ``datetime.datetime.now()``
        :param retry_attempt_name: Retry attempt name, optional
        :param workflow_params: Extra workflow parameters
        :return:
        """
        workflow_id = workflow.id if isinstance(workflow, Workflow) else workflow
        _params = {"workflowId": workflow_id}
        workflow_params = workflow_params if workflow_params else {}
        _params.update({"params": workflow_params})
        if not session_time:
            session_time = to_iso8601(datetime.now())
        if retry_attempt_name:
            _params.update({"retryAttemptName": retry_attempt_name})

        _params["sessionTime"] = session_time
        r = self.put("attempts", _json=_params)
        if r:
            return Attempt.from_api_repr(**r)
        else:
            raise ValueError("Unable to start attempt")

    def kill_attempt(
        self, attempt: Union[int, Attempt], inplace: bool = False
    ) -> Optional[Attempt]:
        """Kill a session

        :param attempt: Attempt ID or Attempt object
        :type attempt: Union[int, Attempt]
        :param inplace: If True, do operation inplace and return None
        :return: ``True`` if succeeded
        :rtype: Attempt
        """
        attempt_id = attempt.id if isinstance(attempt, Attempt) else attempt
        self.post(f"attempts/{attempt_id}/kill", content=True)
        if inplace:
            self.attempt(attempt, inplace=True)
        else:
            return self.attempt(attempt)

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
            self.attempt(attempt, inplace=True)

        return attempt


class ScheduleAPI:
    def schedules(self, last_id: Optional[int] = None) -> List[Schedule]:
        """List schedules

        :param last_id: List schedules whose id is grater than this id for pagination.
        :return: List of Schedule
        """
        r = self.get("schedules", params={"last_id": last_id})
        if r:
            return [Schedule.from_api_repr(**s) for s in r["schedules"]]
        else:
            return []

    def schedule(self, schedule: Union[int, Schedule]) -> Schedule:
        """Get a schedule

        :param schedule: Schedule id or Schedule object
        :return: Schedule
        """
        schedule_id = schedule.id if isinstance(schedule, Schedule) else schedule
        r = self.get(f"schedules/{schedule_id}")
        if r:
            return Schedule.from_api_repr(**r)
        else:
            raise ValueError(f"Unable to find schedule id: {schedule_id}")

    def backfill_schedule(
        self,
        schedule: Union[int, Schedule],
        attempt_name: str,
        from_time: Union[str, datetime],
        dry_run: bool = False,
        count: Optional[int] = None,
    ) -> ScheduleAttempt:
        """Run or re-run past schedules

        :param schedule: Target Schedule id or Schedule object
        :param attempt_name: Attempt name
        :param from_time: From time e.g "2019-11-01T06:20:07.000+00:00" in ``str`` or
                          :class:`datetime.datetime`.
        :param dry_run: Flag for dry run
        :param count: Count
        :return: ScheduleAttempt
        """
        params = {}
        if from_time:
            params["fromTime"] = to_iso8601(from_time)
        if attempt_name:
            params["attemptName"] = attempt_name
        if count:
            params["count"] = count
        params["dryRun"] = dry_run

        schedule_id = schedule.id if isinstance(schedule, Schedule) else schedule
        r = self.post(f"schedules/{schedule_id}/backfill", body=params)
        if r:
            return ScheduleAttempt.from_api_repr(**r)
        else:
            raise ValueError(f"Unable to backfill for schedule: {schedule_id}")

    def disable_schedule(self, schedule: Union[int, Schedule]) -> Schedule:
        """Disable a schedule

        :param schedule: Schedule ID or Schedule object
        :return: New Schedule
        """
        schedule_id = schedule.id if isinstance(schedule, Schedule) else schedule
        r = self.post(f"schedules/{schedule_id}/disable")
        if r:
            return Schedule.from_api_repr(**r)
        else:
            raise ValueError(f"Unable to disable schedule id: {schedule_id}")

    def enable_schedule(self, schedule: Union[int, Schedule]) -> Schedule:
        """Enable a schedule

        :param schedule: Schedule ID or Schedule object
        :return: New Schedule
        """
        schedule_id = schedule.id if isinstance(schedule, Schedule) else schedule
        r = self.post(f"schedules/{schedule_id}/enable")
        if r:
            return Schedule.from_api_repr(**r)
        else:
            raise ValueError(f"Unable to enable schedule id: {schedule_id}")

    def skip_schedule(
        self,
        schedule: Union[int, Schedule],
        from_time: Optional[Union[str, datetime]] = None,
        next_time: Optional[str] = None,
        next_run_time: Optional[Union[str, datetime]] = None,
        dry_run: Optional[bool] = False,
    ) -> Schedule:
        """Skip schedules forward to a future time

        :param schedule: Schedule ID or Schedule object
        :param from_time: From time
        :param next_time: Next time
        :param next_run_time: Next run time
        :param dry_run: Flag for dry run
        :return: New Schedule
        """
        params = {}
        if from_time:
            params["fromTime"] = to_iso8601(from_time)
        if next_time:
            params["nextTime"] = next_time
        if next_run_time:
            params["nextRunTime"] = to_iso8601(next_run_time)
        params["dryRun"] = dry_run
        schedule_id = schedule.id if isinstance(schedule, Schedule) else schedule
        r = self.post(f"schedules/{schedule_id}/skip", body=params)
        if r:
            return Schedule.from_api_repr(**r)
        else:
            raise ValueError(f"Unable to skip schedule id: {schedule_id}")


class SessionAPI:
    def sessions(
        self, last_id: Optional[int] = None, page_size: Optional[int] = None
    ) -> List[Session]:
        """List sessions

        :param last_id: List sessions whose id is grater than this id for pagination
        :param page_size: Number of sessions to return
        :return: List of Session
        """
        params = {}
        if last_id:
            params["last_id"] = last_id
        if page_size:
            params["page_size"] = page_size

        r = self.get("sessions", params=params)
        if r:
            return [Session(**s) for s in r["sessions"]]
        else:
            return []

    def session(self, session: Union[int, Session]) -> Session:
        """Get a session

        :param session: Sesion ID or Session object
        :return: New Session
        """
        session_id = session.id if isinstance(session, Session) else session
        r = self.get(f"sessions/{session_id}")
        if r:
            return Session.from_api_repr(**r)
        else:
            raise ValueError(f"Unable to get sesesion id: {session_id}")

    def session_attempts(
        self,
        session: Union[int, Session],
        last_id: Optional[int] = None,
        page_size: Optional[int] = None,
    ) -> List[Attempt]:
        """Get attempts of a session

        :param session: Session ID or Session object
        :param last_id: List attempts whose id is grater than this id for pagination
        :param page_size: Number of attempts to return
        :return: List of Attempt
        """
        params = {}
        if last_id:
            params["last_id"] = last_id
        if page_size:
            params["page_size"] = page_size

        session_id = session.id if isinstance(session, Session) else session
        r = self.get(f"sessions/{session_id}/attempts", params=params)
        if r:
            return [Attempt.from_api_repr(**e) for e in r["attempts"]]
        else:
            return []


class LogAPI:
    def log_files(
        self,
        attempt: Union[Attempt, int],
        task: Optional[str] = None,
        direct_download: Optional[bool] = None,
    ) -> List[LogFile]:
        """Get log files information

        :param attempt: Target Attempt id or Attempt object
        :param task: Task name
        :param direct_download: Flag for direct download
        :return: List of LogFile
        """
        params = {}
        if task:
            params["task"] = task
        if direct_download:
            params["direct_download"] = True

        attempt_id = attempt.id if isinstance(attempt, Attempt) else attempt
        r = self.get(f"logs/{attempt_id}/files")
        if r:
            return [LogFile.from_api_repr(**l) for l in r["files"]]
        else:
            return []

    def log_file(self, attempt: Union[Attempt, int], file: Union[LogFile, str]) -> str:
        """Get a log string for an attempt

        :param attempt: Target Attempt id or Attempt object
        :param file: LogFile name or LogFile object
        :return: Log string
        """

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
        """Get log string list for an attempt

        :param attempt: Attempt ID or Attempt object
        :return: A list of log


        .. code-block:: python

           >>> import tdworkflow
           >>> client = tdworkflow.client.Client("us")
           >>> attempts = client.attempts(project="pandas-df")
           >>> logs = client.logs(attempts[0])
           >>> print(logs)
           ['2019-10-30 08:34:51.672 +0000 [INFO] (0250@[1:pandas-df]+pandas-df+read_into_df) io.digdag.core.agent.OperatorManager: py>: py_scripts.examples.read_td_table\\n',
           '2019-10-30 08:34:59.879 +0000 [INFO] (0237@[1:pandas-df]+pandas-df+read_into_df) io.digdag.core.agent.OperatorManager: py>: py_scripts.examples.read_td_table\\nWait running a command task: status provisioning',
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
        site: str = "us",
        endpoint: Optional[str] = None,
        apikey: Optional[str] = None,
        user_agent: Optional[str] = None,
        _session: Optional[requests.Session] = None,
        scheme: str = "https",
    ) -> None:
        """Treasure Workflow REST API client

        :param site: Site for Treasure Workflow.
                     {"us", "eu01", "jp", "ap02", "ap03"} default: "us"
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
        :param scheme: URI scheme default: "https"
        :type scheme: str
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
        elif site == "ap02":
            self.endpoint = "api-workflow.ap02.treasuredata.com"
        elif site == "ap03":
            self.endpoint = "api-workflow.ap03.treasuredata.com"
        else:
            raise ValueError(
                f"Unknown site: {site}. Use 'us', 'jp', 'eu01', or 'ap02' "
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
        self.api_base = f"{scheme}://{self.endpoint}/api/"

    @property
    def http(self):
        """
        :return: Established session
        :rtype: requests.Session
        """
        return self._http

    def get(
        self, path: str, params: Optional[Dict[str, str]] = None, content: bool = False
    ) -> Union[Dict[str, str], bytes]:
        """GET operator for REST API

        :param path: Treasure Workflow API path
        :type path: str
        :param params: Query parameters, defaults to None
        :type params: Optional[Dict[str, str]], optional
        :param content: Return content body without parsing JSON if ``True``
        :type content: bool
        :return: Response data in JSON or bytes
        :rtype: Union[Dict[str, str], bytes]
        """
        url = f"{self.api_base}{path}"
        r = self.http.get(url, params=params)
        logger.debug(f"{r.status_code}\n{r.content}")

        if not 200 <= r.status_code < 300:
            exceptions.raise_response_error(r)

        if content:
            return r.content
        else:
            return r.json()

    def post(
        self, path: str, body: Optional[Dict[str, str]] = None, content: bool = False
    ) -> Optional[Union[Dict[str, str], bytes]]:
        """POST operator for REST API

        :param path: Treasure Workflow API path
        :type path: str
        :param body: Content body in dictionary to be passed in JSON
        :type body: Optional[Dict[str, str]], optional
        :param content: Return content body without parsing JSON if ``True``
        :type content: bool
        :return: ``True`` if succeeded
        """
        url = f"{self.api_base}{path}"
        r = self.http.post(url, json=body)
        logger.debug(f"{r.status_code}\n{r.content}")

        if not 200 <= r.status_code < 300:
            exceptions.raise_response_error(r)

        if content:
            return r.content
        elif r.content and "application/json" in r.headers.get("Content-Type", ""):
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
        logger.debug(f"{r.status_code}\n{r.content}")

        if not 200 <= r.status_code < 300:
            exceptions.raise_response_error(r)

        if r.content and "application/json" in r.headers.get("Content-Type", ""):
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
        logger.debug(f"{r.status_code}\n{r.content}")

        if not 200 <= r.status_code < 300:
            exceptions.raise_response_error(r)

        if r.content and "application/json" in r.headers.get("Content-Type", ""):
            return r.json()
