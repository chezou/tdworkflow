tdwokflow
=========

Unofficial Treasure Workflow API client.

Installation
------------

.. code-block:: shell

   pip install tdworkflow

If you want to use development version, run as follows:

.. code-block:: shell

   pip install git+https://github.com/chezou/tdworkflow.git

Usage
-----

.. code-block:: python

   import os

   from tdworkflow.client import Client

   apikey = os.getenv("TD_API_KEY")
   client = Client(site="us", apikey=apikey)
   # Or, write endpoint explicitly
   # client = Client(endpoint="api-workflow.treasuredata.com", apikey=apikey)

   projects = client.projects("pandas-df")

   secrets = {"td.apikey": apikey, "td.apiserver": "https://api.treasuredata.com", "test": "secret-foo"}

   client.set_secrets(projects[0], secrets)

   client.secrets(projects[0])
   # ['td.apikey', 'td.apiserver', "test"]
   client.delete_secrets(projects[0], ["test", "td.apiserver"])

Uplaod Project from GitHub
^^^^^^^^^^^^^^^^^^^^^^^^^^

Before executing example code, you have to install git-python

.. code-block:: shell

   pip install gitpython

Clone example repository with git-python and upload a digdag project.

.. code-block:: python

   import tempfile
   import os
   import shutil

   import tdworkflow

   from git import Git

   # Download example GitHub repositoory

   tempdir = tempfile.gettempdir()

   git_repo = "https://github.com/treasure-data/treasure-boxes/"

   shutil.rmtree(os.path.join(tempdir, "treasure-boxes"))

   try:
       Git(tempdir).clone(git_repo)
       print("Clone repository succeeded")
   except Exception:
       print("Repository clone failed")
       raise

   # Upload specific Workflow project

   apikey = os.getenv("TD_API_KEY")
   site = "us"

   target_box = os.path.join("integration-box", "python")
   target_path = os.path.join(tempdir, "treasure-boxes", target_box)

   client = tdworkflow.client.Client(site=site, apikey=apikey)
   project = client.create_project("my-project", target_path)

If you want to open Treasure Workflow console on your browser, you can get the workflow URL as the following:

.. code-block:: python

   CONSOLE_URL = {
       "us": "https://console.treasuredata.com/app/workflows",
       "eu01": "https://console.eu01.treasuredata.com/app/workflows",
       "jp": "https://console.treasuredata.co.jp/app/workflows",
   }

   workflows = client.project_workflows(project)
   workflows = list(filter(lambda w: w.name != "test", workflows))
   if workflows:
       print(f"Project created! Open {CONSOLE_URL[site]}/{workflows[0].id}/info on your browser and clieck 'New Run' button.")
   else:
       print("Project creation failed.")

Start workflow session
^^^^^^^^^^^^^^^^^^^^^^

You can start workflow session by using ``Client.start_attempt``.

.. code-block:: python

   attempt = client.start_attempt(workflows[0])

   # Wait attempt until finish. This may require few minutes.
   attempt = client.wait_attempt(attempt)


Connect to open source digdag
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Since Treasure Workflow is hosted digdag, tdworkflow is compatible with open source digdag.

.. note::
   Open source digdag API may be different with Treasure Workflow API so that tdworkflow might not work with some API of opensource digdag.

Here is the example code to connect local digdag server.

.. code-block:: python

    >>> import tdworkflow
    >>> import requests
    >>> session = requests.Session()
    >>> client = tdworkflow.client.Client(
    ... endpoint="localhost:65432", apikey="", _session=session, scheme="http")
    >>> client.projects()
    [Project(id=1, name='python-tdworkflow', revision='134fe2f9-ded3-4e7c-af8e-8a82d55d688b', archiveType='db', archiveMd5='5Lc6F6m3DtmBN4DA5MzK8A==', createdAt='2019-11-01T13:03:26Z', deletedAt=None, updatedAt='2019-11-01T13:03:26Z')]
