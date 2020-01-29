import hashlib
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


def file_md5sum(file, chunk_size=65536):
    """
    Return md5sum of Django UploadedFile.

    :param file: Django UploadedFile
    :return: md5sum
    """
    hasher = hashlib.md5()
    for buf in file.chunks(chunk_size=chunk_size):
        hasher.update(buf)

    return hasher.hexdigest()
