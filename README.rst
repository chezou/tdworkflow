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
