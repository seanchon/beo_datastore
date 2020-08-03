from django.conf import settings
from django.core.files.storage import default_storage
import warnings

from beo_datastore.settings import AWS_MEDIA_BUCKET_NAME
from storages.backends.s3boto3 import S3Boto3Storage


class MediaStorage(S3Boto3Storage):
    """
    Custom storage for collecting user-generated media files to dedicated
    S3 bucket.
    """

    bucket_name = settings.AWS_MEDIA_BUCKET_NAME
    location = settings.MEDIA_ROOT_DIR


class MediaMigrationHelper:
    """
    Assists in relocating media files during Django migrations. If the
    migration is run in an environment that leverages the S3 backend, we
    use the boto3 utilities to relocate files; if not, the files are relocated
    locally using basic OS operations
    """

    def __init__(self):
        if AWS_MEDIA_BUCKET_NAME:
            self.storage = MediaStorage()
        else:
            self.storage = default_storage

    def migrate_file(self, old_path: str, dest_path: str):
        """
        Migrates a file given an old path and a destination path
        :param old_path:
        :param dest_path:
        """
        if not self.storage.exists(old_path):
            raise Exception("File at {} does not exist".format(old_path))

        try:
            # As of this writing, all saved application files are parquet
            # files, which are binary
            old_file = self.storage.open(old_path, "rb")
            actual_dest_path = self.storage.save(dest_path, old_file)
        except Exception as e:
            # Catch-all exception: clean up the new file that the default
            # storage system creates and re-raise the error
            self.storage.delete(dest_path)
            raise e

        old_file.close()

        # If the file system was forced to create a new name for the file
        # that does not precisely match the requested name, print a
        # warning. This can happen if a file already exists at `dest_path`
        if actual_dest_path != dest_path:
            warnings.warn(
                "Could not copy from {} to {}: file already exists. File was copied to {}".format(
                    old_path, dest_path, actual_dest_path
                )
            )

        # Delete the old file
        self.storage.delete(old_path)


media_migration_helper = MediaMigrationHelper()
