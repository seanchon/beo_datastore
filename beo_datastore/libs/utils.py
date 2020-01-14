import os
from pathlib import Path

from beo_datastore.settings import AWS_MEDIA_BUCKET_NAME


def timedelta_to_hours(timedelta_):
    """
    Convert timedelta_ to hours.
    """
    return timedelta_.seconds / 3600


def mkdir_p(path):
    """
    Creates all directories in path if they do not exist. This has the same
    functionality as a "mkdir -p" in bash.
    """
    if not AWS_MEDIA_BUCKET_NAME and not os.path.exists(path):
        with Path(path) as p:
            p.mkdir(parents=True)
