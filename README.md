# td-workflow

Unofficial Treasure Workflow API client.

```py
import os

from tdworkflow.client import Client
from tdworkflow.workflow import Workflow

apikey = os.getenv("TD_API_KEY")
client = Client("us", apikey=apikey)
wf = Workflow(client, "pandas-df")

secrets = {"td.apikey": apikey, "td.apiserver": "https://api.treasuredata.com", "test": "secret-foo"}

wf.set_secrets(secrets)

wf.secrets()
# ['td.apikey', 'td.apiserver', "test"]
wf.delete_secrets(["test", "td.apiserver"])
```
