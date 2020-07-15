import copy
import datetime
import io

import pytest
import requests

import tdworkflow
from tdworkflow import exceptions
from tdworkflow.attempt import Attempt
from tdworkflow.client import Client
from tdworkflow.log import LogFile
from tdworkflow.project import Project
from tdworkflow.revision import Revision
from tdworkflow.schedule import Schedule, ScheduleAttempt
from tdworkflow.session import Session
from tdworkflow.task import Task
from tdworkflow.workflow import Workflow

RESP_DATA_GET_0 = {
    "projects": [
        {
            "id": "115819",
            "name": "pandas-df",
            "revision": "c53fa6f9117c491bac024d693332ccf5",
            "createdAt": "2019-07-17T04:48:45Z",
            "updatedAt": "2019-10-30T08:34:39Z",
            "deletedAt": None,
            "archiveType": "s3",
            "archiveMd5": "KruTqOtJ659HHpIJ6NyTTA==",
        }
    ]
}

RESP_DATA_GET_1 = {
    "workflows": [
        {
            "id": "1614347",
            "name": "pandas-df",
            "project": {"id": "115819", "name": "pandas-df"},
            "revision": "c53fa6f9117c491bac024d693332ccf5",
            "timezone": "UTC",
            "config": {
                "+read_into_df": {
                    "py>": "py_scripts.examples.read_td_table",
                    "database_name": "sample_datasets",
                    "table_name": "nasdaq",
                    "docker": {"image": "digdag/digdag-python:3.7"},
                    "_env": {
                        "TD_API_KEY": "${secret:td.apikey}",
                        "TD_API_SERVER": "${secret:td.apiserver}",
                    },
                },
                "+write_into_td": {
                    "py>": "py_scripts.examples.write_td_table",
                    "database_name": "pandas_test",
                    "table_name": "my_df",
                    "docker": {"image": "digdag/digdag-python:3.7"},
                    "_env": {
                        "TD_API_KEY": "${secret:td.apikey}",
                        "TD_API_SERVER": "${secret:td.apiserver}",
                    },
                },
            },
        }
    ]
}

RESP_DATA_GET_2 = {
    "revisions": [
        {
            "revision": "2a01a9ba-96a1-420a-851f-a1521f874493",
            "createdAt": "2019-11-01T05:34:15Z",
            "archiveType": "s3",
            "archiveMd5": "+sWKEpHPDe7DS81vcrO51Q==",
            "userInfo": {
                "td": {
                    "user": {
                        "id": 24446,
                        "name": "Michiaki Ariga",
                        "email": "ariga@treasure-data.com",
                    }
                }
            },
        }
    ]
}

RESP_DATA_GET_3 = {
    "schedules": [
        {
            "id": "23494",
            "project": {"id": "168037", "name": "python-tdworkflow"},
            "workflow": {"id": "1624118", "name": "simple"},
            "nextRunTime": "2019-11-01T07:00:00Z",
            "nextScheduleTime": "2019-11-01T00:00:00+00:00",
            "disabledAt": None,
        }
    ]
}

RESP_DATA_GET_4 = {
    "id": "23494",
    "project": {"id": "168037", "name": "python-tdworkflow"},
    "workflow": {"id": "1624118", "name": "simple"},
    "attempts": [],
}

RESP_DATA_GET_5 = {
    "sessions": [
        {
            "id": "14412528",
            "project": {"id": "113895", "name": "wf-performance-monitor"},
            "workflow": {"name": "wf-task-duration", "id": "1204939"},
            "sessionUuid": "b6af05c4-4875-48d1-9aed-8cbdf2104ae5",
            "sessionTime": "2019-11-01T08:03:00+00:00",
            "lastAttempt": {
                "id": "62497627",
                "retryAttemptName": None,
                "done": True,
                "success": True,
                "cancelRequested": False,
                "params": {
                    "last_session_time": "2019-11-01T08:00:00+00:00",
                    "next_session_time": "2019-11-01T08:06:00+00:00",
                    "last_executed_session_time": "2019-11-01T08:00:00+00:00",
                },
                "createdAt": "2019-11-01T08:03:00Z",
                "finishedAt": "2019-11-01T08:03:03Z",
            },
        }
    ]
}

RESP_DATA_GET_6 = {
    "attempts": [
        {
            "id": "62487260",
            "index": 1,
            "project": {"id": "168037", "name": "python-tdworkflow"},
            "workflow": {"name": "simple", "id": "1624118"},
            "sessionId": "14410781",
            "sessionUuid": "83dff830-5cff-427b-8647-4c5ab88cbb6f",
            "sessionTime": "2019-11-01T00:00:00+00:00",
            "retryAttemptName": None,
            "done": True,
            "success": True,
            "cancelRequested": False,
            "params": {
                "last_session_time": "2019-10-31T00:00:00+00:00",
                "next_session_time": "2019-11-02T00:00:00+00:00",
            },
            "createdAt": "2019-11-01T07:00:00Z",
            "finishedAt": "2019-11-01T07:06:38Z",
            "status": "running",
        }
    ]
}

RESP_DATA_GET_7 = {
    "files": [
        {
            "fileName": "+simple+simple_with_arg@example.com.log.gz",
            "fileSize": 161,
            "taskName": "+simple+simple_with_arg",
            "fileTime": "2019-11-01T07:00:22Z",
            "agentId": "8@ip-172-18-168-153.ec2.internal",
            "direct": "https://digdag.example.com/log/2019-11-01/XXXX",
        }
    ]
}

RESP_DATA_GET_8 = {
    "tasks": [
        {
            "id": "1",
            "fullName": "+simple",
            "parentId": None,
            "config": {},
            "upstreams": [],
            "state": "planned",
            "cancelRequested": False,
            "exportParams": {},
            "storeParams": {},
            "stateParams": {},
            "updatedAt": "2019-12-15T07:27:16Z",
            "retryAt": None,
            "startedAt": None,
            "error": {},
            "isGroup": True,
        },
        {
            "id": "2",
            "fullName": "+simple+simple_with_arg",
            "parentId": "1",
            "config": {
                "py>": "py_scripts.examples.print_arg",
                "msg": "Hello World",
                "docker": {"image": "digdag/digdag-python:3.7"},
            },
            "upstreams": [],
            "state": "running",
            "cancelRequested": False,
            "exportParams": {},
            "storeParams": {},
            "stateParams": {},
            "updatedAt": "2019-12-15T07:27:16Z",
            "retryAt": None,
            "startedAt": "2019-12-15T07:27:16Z",
            "error": {},
            "isGroup": False,
        },
        {
            "id": "3",
            "fullName": "+simple+simple_with_env",
            "parentId": "1",
            "config": {
                "py>": "py_scripts.examples.print_env",
                "_env": {"MY_ENV_VAR": "hello"},
                "docker": {"image": "digdag/digdag-python:3.7"},
            },
            "upstreams": ["2"],
            "state": "blocked",
            "cancelRequested": False,
            "exportParams": {},
            "storeParams": {},
            "stateParams": {},
            "updatedAt": "2019-12-15T07:27:16Z",
            "retryAt": None,
            "startedAt": None,
            "error": {},
            "isGroup": False,
        },
        {
            "id": "4",
            "fullName": "+simple+simple_with_import",
            "parentId": "1",
            "config": {
                "py>": "py_scripts.examples.import_another_file",
                "docker": {"image": "digdag/digdag-python:3.7"},
            },
            "upstreams": ["3"],
            "state": "blocked",
            "cancelRequested": False,
            "exportParams": {},
            "storeParams": {},
            "stateParams": {},
            "updatedAt": "2019-12-15T07:27:16Z",
            "retryAt": None,
            "startedAt": None,
            "error": {},
            "isGroup": False,
        },
        {
            "id": "5",
            "fullName": "+simple+simple_with_workflow_env",
            "parentId": "1",
            "config": {},
            "upstreams": ["4"],
            "state": "blocked",
            "cancelRequested": False,
            "exportParams": {},
            "storeParams": {},
            "stateParams": {},
            "updatedAt": "2019-12-15T07:27:16Z",
            "retryAt": None,
            "startedAt": None,
            "error": {},
            "isGroup": True,
        },
        {
            "id": "6",
            "fullName": "+simple+simple_with_workflow_env+store_msg",
            "parentId": "5",
            "config": {
                "py>": "py_scripts.examples.store_workflow_env",
                "msg": "Hello World",
                "docker": {"image": "digdag/digdag-python:3.7"},
            },
            "upstreams": [],
            "state": "blocked",
            "cancelRequested": False,
            "exportParams": {},
            "storeParams": {},
            "stateParams": {},
            "updatedAt": "2019-12-15T07:27:16Z",
            "retryAt": None,
            "startedAt": None,
            "error": {},
            "isGroup": False,
        },
        {
            "id": "7",
            "fullName": "+simple+simple_with_workflow_env+restore_msg",
            "parentId": "5",
            "config": {"echo>": "${my_msg}"},
            "upstreams": ["6"],
            "state": "blocked",
            "cancelRequested": False,
            "exportParams": {},
            "storeParams": {},
            "stateParams": {},
            "updatedAt": "2019-12-15T07:27:16Z",
            "retryAt": None,
            "startedAt": None,
            "error": {},
            "isGroup": False,
        },
    ]
}

RESP_DATA_PUT_0 = {
    "id": "167272",
    "name": "python-tdworkflow",
    "revision": "1d4629f3-f4dd-4d17-82a0-6d0e23b291fa",
    "createdAt": "2019-10-30T14:05:34Z",
    "updatedAt": "2019-11-01T03:27:47Z",
    "deletedAt": None,
    "archiveType": "s3",
    "archiveMd5": "rYhVxGxbiyQxK+cbNNokHw==",
}

RESP_DATA_DELETE_0 = RESP_DATA_PUT_0


def prepare_mock(
    client,
    mocker,
    ret_json=None,
    status_code=200,
    _error=None,
    method="get",
    content=b"",
    mock=True,
    json=False,
    responses=None,
):
    if mock:
        client._http = mocker.MagicMock()
    response = getattr(client._http, method).return_value
    response.status_code = status_code
    response.content = content
    if ret_json:
        response.json.return_value = ret_json
    elif responses:
        response.json.side_effect = responses
    if _error:
        response.raise_for_status.side_effect = _error
    if json:
        response.headers = {"Content-Type": "application/json"}


def test_create_client():
    client = Client(site="us", apikey="APIKEY")
    assert client.site == "us"
    assert client.endpoint == "api-workflow.treasuredata.com"
    assert client.apikey == "APIKEY"
    assert isinstance(client.http, requests.Session)
    assert client.http.headers["Authorization"] == "TD1 APIKEY"
    assert client.http.headers["User-Agent"] == f"tdworkflow/{tdworkflow.__version__}"
    assert client.api_base == "https://api-workflow.treasuredata.com/api/"


def test_create_client_with_endpoint():
    client = Client(endpoint="digdag.example.com", apikey="APIKEY")
    assert client.site == "us"
    assert client.endpoint == "digdag.example.com"
    assert client.apikey == "APIKEY"
    assert isinstance(client.http, requests.Session)
    assert client.http.headers["Authorization"] == "TD1 APIKEY"
    assert client.http.headers["User-Agent"] == f"tdworkflow/{tdworkflow.__version__}"
    assert client.api_base == "https://digdag.example.com/api/"


def test_create_client_with_scheme():
    session = requests.Session()
    client = Client(
        endpoint="localhost:65432", apikey="", _session=session, scheme="http"
    )
    assert client.endpoint == "localhost:65432"
    assert client.api_base == "http://localhost:65432/api/"
    assert "Authorization" not in client.http.headers


class TestProjectAPI:
    def setup_method(self, method):
        print("method{}".format(method.__name__))
        self.client = Client(site="us", apikey="APIKEY")

    def test_projects(self, mocker):
        prepare_mock(self.client, mocker, RESP_DATA_GET_0)

        pjs = self.client.projects()
        assert pjs == [Project(**p) for p in RESP_DATA_GET_0["projects"]]
        assert isinstance(pjs[0].created_at, datetime.datetime)
        assert isinstance(pjs[0].updated_at, datetime.datetime)
        assert pjs[0].deleted_at is None

        pj_name = RESP_DATA_GET_0["projects"][0]["name"]
        pjs2 = self.client.projects(name=pj_name)
        assert pjs2 == [Project(**p) for p in RESP_DATA_GET_0["projects"]]

    def test_project(self, mocker):
        prepare_mock(self.client, mocker, RESP_DATA_GET_0["projects"][0])

        pj = RESP_DATA_GET_0["projects"][0]
        assert Project(**pj) == self.client.project(int(pj["id"]))

    def test_nonexist_project(self, mocker):
        res = {
            "message": "Resource does not exist: project id=-1",
            "status": 404,
        }
        prepare_mock(self.client, mocker, res, 404, requests.exceptions.HTTPError())

        with pytest.raises(exceptions.HttpError):
            self.client.project(-1)

    def test_project_workflows(self, mocker):
        prepare_mock(self.client, mocker, RESP_DATA_GET_1)

        pj = RESP_DATA_GET_0["projects"][0]
        wfs = self.client.project_workflows(int(pj["id"]))
        assert [Workflow(**w) for w in RESP_DATA_GET_1["workflows"]] == wfs

    def test_project_workflow_by_name(self, mocker):
        responses = [RESP_DATA_GET_0, RESP_DATA_GET_1]
        prepare_mock(self.client, mocker, responses=responses)

        wfs = self.client.project_workflows_by_name("pandas-df")
        assert [Workflow(**w) for w in RESP_DATA_GET_1["workflows"]] == wfs

    def test_nonexist_project_workflow_by_name(self, mocker):
        prepare_mock(self.client, mocker)

        with pytest.raises(ValueError):
            self.client.project_workflows_by_name("non-existent-pj")

    def test_create_project(self, mocker):
        prepare_mock(
            self.client,
            mocker,
            RESP_DATA_PUT_0,
            method="put",
            content=b"abc",
            json=True,
        )

        pj = self.client.create_project(
            "test-project", "tests/resources/sample_project"
        )
        assert Project(**RESP_DATA_PUT_0) == pj

    def test_delete_project(self, mocker):
        prepare_mock(
            self.client,
            mocker,
            RESP_DATA_DELETE_0,
            status_code=204,
            method="delete",
            content=b"abc",
            json=True,
        )

        pj_id = RESP_DATA_GET_0["projects"][0]["id"]
        assert self.client.delete_project(int(pj_id)) is True

    def test_project_revisions(self, mocker):
        prepare_mock(self.client, mocker, RESP_DATA_GET_2)

        pj = RESP_DATA_GET_0["projects"][0]
        revs = self.client.project_revisions(int(pj["id"]))
        assert [Revision(**r) for r in RESP_DATA_GET_2["revisions"]] == revs

    def test_project_schedules(self, mocker):
        prepare_mock(self.client, mocker, RESP_DATA_GET_3)

        pj = RESP_DATA_GET_0["projects"][0]
        sches = self.client.project_schedules(int(pj["id"]))
        assert [Schedule(**s) for s in RESP_DATA_GET_3["schedules"]] == sches

    def test_set_secrets(self, mocker):
        prepare_mock(self.client, mocker, method="put")

        pj_id = RESP_DATA_GET_0["projects"][0]["id"]
        assert self.client.set_secrets(pj_id, {"test": "SECRET"})

    def test_secrets(self, mocker):
        content = b'{"secrets":[{"key":"foo"},{"key":"bar"}]}'
        ret_val = {"secrets": [{"key": "foo"}, {"key": "bar"}]}
        prepare_mock(self.client, mocker, ret_json=ret_val, content=content)

        pj_id = RESP_DATA_GET_0["projects"][0]["id"]
        assert self.client.secrets(int(pj_id)) == ["foo", "bar"]

    def test_delete_secret(self, mocker):
        content = b'{"secrets":[{"key":"foo"},{"key":"bar"}]}'
        ret_val = {"secrets": [{"key": "foo"}, {"key": "bar"}]}
        prepare_mock(self.client, mocker, ret_json=ret_val, content=content)

        prepare_mock(
            self.client,
            mocker,
            status_code=204,
            ret_json={"secrets": [{"key": "foo"}]},
            method="delete",
            mock=False,
        )

        pj_id = RESP_DATA_GET_0["projects"][0]["id"]
        assert self.client.delete_secret(int(pj_id), "foo") is True


class TestWorkflowAPI:
    def setup_method(self, method):
        print("method{}".format(method.__name__))
        self.client = Client(site="us", apikey="APIKEY")

    def test_workflows(self, mocker):
        prepare_mock(self.client, mocker, ret_json=RESP_DATA_GET_1)
        wfs = self.client.workflows()
        assert [Workflow(**w) for w in RESP_DATA_GET_1["workflows"]] == wfs

    def test_workflow(self, mocker):
        target_wf = RESP_DATA_GET_1["workflows"][0]
        prepare_mock(self.client, mocker, ret_json=target_wf)

        wf = self.client.workflow(int(target_wf["id"]))
        assert Workflow(**target_wf) == wf

    def test_unexist_workflow(self, mocker):
        prepare_mock(
            self.client,
            mocker,
            status_code=404,
            ret_json={
                "message": "Resource does not exist: workflow id=-1",
                "status": 404,
            },
            _error=requests.exceptions.HTTPError(),
        )

        with pytest.raises(exceptions.HttpError):
            self.client.workflow(-1)


class TestScheduleAPI:
    def setup_method(self, method):
        print("method{}".format(method.__name__))
        self.client = Client(site="us", apikey="APIKEY")

    def test_schedules(self, mocker):
        prepare_mock(self.client, mocker, ret_json=RESP_DATA_GET_3)

        sches = self.client.schedules()
        assert [Schedule(**s) for s in RESP_DATA_GET_3["schedules"]] == sches
        assert isinstance(sches[0].next_run_time, datetime.datetime)

    def test_schedule(self, mocker):
        sched = RESP_DATA_GET_3["schedules"][0]
        prepare_mock(self.client, mocker, ret_json=sched)

        assert Schedule(**sched) == self.client.schedule(int(sched["id"]))

    def test_backfill_schedule(self, mocker):
        prepare_mock(
            self.client,
            mocker,
            ret_json=RESP_DATA_GET_4,
            method="post",
            json=True,
            content=b"acb",
        )
        sched = RESP_DATA_GET_3["schedules"][0]

        s_attempt = self.client.backfill_schedule(
            int(sched["id"]), 12345, "2019-11-01T15:45:37.018124+00:00", dry_run=False
        )
        assert ScheduleAttempt(**RESP_DATA_GET_4) == s_attempt

    def test_disable_schedule(self, mocker):
        scheds = copy.deepcopy(RESP_DATA_GET_3)
        sched = scheds["schedules"][0]
        sched["disabledAt"] = "2019-11-01T07:37:51Z"
        prepare_mock(
            self.client,
            mocker,
            ret_json=sched,
            method="post",
            json=True,
            content=b"abc",
        )
        s = self.client.disable_schedule(int(sched["id"]))
        assert Schedule(**sched) == s
        assert s.disabled_at is not None

    def test_enable_schedule(self, mocker):
        sched = RESP_DATA_GET_3["schedules"][0]
        prepare_mock(
            self.client,
            mocker,
            ret_json=sched,
            method="post",
            json=True,
            content=b"abc",
        )
        s = self.client.disable_schedule(int(sched["id"]))
        assert Schedule(**sched) == s
        assert s.disabled_at is None

    def test_skip_schedule(self, mocker):
        sched = RESP_DATA_GET_3["schedules"][0]
        prepare_mock(
            self.client,
            mocker,
            ret_json=sched,
            method="post",
            json=True,
            content=b"abc",
        )
        s = self.client.skip_schedule(int(sched["id"]))
        assert Schedule(**sched) == s


class TestSessionAPI:
    def setup_method(self, method):
        print("method{}".format(method.__name__))
        self.client = Client(site="us", apikey="APIKEY")

    def test_sessions(self, mocker):
        prepare_mock(self.client, mocker, ret_json=RESP_DATA_GET_5)
        s = self.client.sessions()
        assert [Session(**ss) for ss in RESP_DATA_GET_5["sessions"]] == s

    def test_session(self, mocker):
        session = RESP_DATA_GET_5["sessions"][0]
        prepare_mock(self.client, mocker, ret_json=session)
        s = self.client.session(int(session["id"]))
        assert Session(**session) == s

    def test_session_attempts(self, mocker):
        prepare_mock(self.client, mocker, ret_json=RESP_DATA_GET_6)
        session = RESP_DATA_GET_5["sessions"][0]
        a = self.client.session_attempts(int(session["id"]))
        assert [Attempt(**at) for at in RESP_DATA_GET_6["attempts"]] == a


class TestLogAPI:
    def setup_method(self, method):
        print("method{}".format(method.__name__))
        self.client = Client(site="us", apikey="APIKEY")

    def test_log_files(self, mocker):
        attempt = RESP_DATA_GET_6["attempts"][0]
        prepare_mock(self.client, mocker, ret_json=RESP_DATA_GET_7)
        files = self.client.log_files(int(attempt["id"]))
        assert [LogFile(**l) for l in RESP_DATA_GET_7["files"]] == files

    def test_log_file(self, mocker):
        import gzip

        attempt_id = int(RESP_DATA_GET_6["attempts"][0]["id"])
        file = RESP_DATA_GET_7["files"][0]

        dummy_file = io.BytesIO()
        with gzip.open(dummy_file, "wb") as f:
            f.write(b"abc")

        prepare_mock(self.client, mocker, ret_json=file, content=dummy_file.getvalue())
        f = self.client.log_file(attempt_id, file["fileName"])
        assert isinstance(f, str)


class TestAttemptAPI:
    def setup_method(self, method):
        print("method{}".format(method.__name__))
        self.client = Client(site="us", apikey="APIKEY")

    def test_attemtps(self, mocker):
        prepare_mock(self.client, mocker, ret_json=RESP_DATA_GET_6)
        attempts = self.client.attempts()
        assert [Attempt(**a) for a in RESP_DATA_GET_6["attempts"]] == attempts

    def test_attempt(self, mocker):
        attempt = RESP_DATA_GET_6["attempts"][0]
        prepare_mock(self.client, mocker, ret_json=attempt)
        assert Attempt(**attempt) == self.client.attempt(attempt["id"])

        attempt2 = copy.deepcopy(RESP_DATA_GET_6["attempts"][0])
        attempt2["cancelRequested"] = "True"
        a_obj = Attempt(**attempt)
        prepare_mock(self.client, mocker, ret_json=attempt2)
        r = self.client.attempt(a_obj, inplace=True)
        assert r is None
        assert a_obj.cancelRequested is True

    def test_attempt_tasks(self, mocker):
        prepare_mock(self.client, mocker, ret_json=RESP_DATA_GET_8)
        tasks = self.client.attempt_tasks(1)
        assert [Task(**t) for t in RESP_DATA_GET_8["tasks"]] == tasks

    def test_retried_attempts(self, mocker):
        prepare_mock(self.client, mocker, ret_json=RESP_DATA_GET_6)
        attempts = self.client.retried_attempts(RESP_DATA_GET_6["attempts"][0]["id"])
        assert [Attempt(**a) for a in RESP_DATA_GET_6["attempts"]] == attempts

    def test_start_attempt(self, mocker):
        a = RESP_DATA_GET_6["attempts"][0]
        prepare_mock(self.client, mocker, a, method="put", content=b"abc", json=True)
        attempt = self.client.start_attempt(a["id"])
        assert Attempt(**a) == attempt

    def test_kill_attempt(self, mocker):
        a = RESP_DATA_GET_6["attempts"][0]
        a["cancelRequested"] = "True"
        prepare_mock(self.client, mocker, method="post", status_code=204)
        prepare_mock(self.client, mocker, a, mock=False)
        attempt = self.client.kill_attempt(a["id"])
        assert Attempt(**a) == attempt
        assert attempt.cancel_requested is True

    def test_unknown_field(self, mocker):
        attempt = RESP_DATA_GET_6["attempts"][0]
        redundant_attempt = copy.deepcopy(attempt)
        redundant_attempt["unknonField"] = "foo"
        prepare_mock(self.client, mocker, ret_json=redundant_attempt)
        assert Attempt(**attempt) == self.client.attempt(attempt["id"])
