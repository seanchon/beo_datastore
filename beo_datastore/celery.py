from __future__ import absolute_import, unicode_literals

import os

from celery import Celery
import dotenv

# set the default Django settings module for the 'celery' program.
dotenv_file = os.path.join(os.path.dirname(os.path.dirname(__file__)), ".env")
if os.path.exists(dotenv_file):
    dotenv.read_dotenv(dotenv_file)
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "beo_datastore.settings")

app = Celery("beo_datastore")

# Using a string here means the worker doesn't have to serialize
# the configuration object to child processes.
# - namespace='CELERY' means all celery-related configuration keys
#   should have a `CELERY_` prefix.
app.config_from_object("django.conf:settings")

# Load task modules from all registered Django app configs.
app.autodiscover_tasks()


@app.task(bind=True)
def debug_task(self):
    print("Request: {0!r}".format(self.request))
