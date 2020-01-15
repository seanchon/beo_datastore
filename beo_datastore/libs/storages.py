from django.conf import settings

from storages.backends.s3boto3 import S3Boto3Storage


class MediaStorage(S3Boto3Storage):
    """
    Custom storage for collecting user-generated media files to dedicated
    S3 bucket.
    """

    bucket_name = settings.AWS_MEDIA_BUCKET_NAME
