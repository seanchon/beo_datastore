import os
import pandas as pd
import s3fs
from tempfile import mkstemp
from typing import List

from django.http import FileResponse


def convert_columns_type(dataframe, type_):
    """
    Convert columns type to another type_.

    :param dataframe: pandas DataFrame
    :param type_: Python type
    :return: pandas DataFrame
    """
    dataframe.columns = dataframe.columns.astype(type_)
    return dataframe


def read_csv(path: str, **kwargs):
    """
    Uses pandas to read a CSV.

    :param path: path to the CSV file to read
    :return: DataFrame
    """
    return read_file_with_cache_invalidation(path, pd.read_csv, **kwargs)


def read_parquet(path: str, **kwargs):
    """
    Uses pandas to read a parquet file.

    :param path: path to the parquet file to read
    :return: DataFrame
    """
    return read_file_with_cache_invalidation(path, pd.read_parquet, **kwargs)


def read_file_with_cache_invalidation(path: str, read_fn, **kwargs):
    """
    Reads a file, potentially clearing the s3fs cache

    If the path is to an s3 file, s3fs will check its cache for previous
    requests to the same path and return the cached response if found. This is
    an issue for buckets that have dynamically created assets: if a file is
    added to a bucket directory after we have queried the directory, the cached
    response will not contain the new file. In this instance, s3fs will raise a
    FileNotFound error. We catch that error and clear the cache before trying
    the request again. We also catch PermissionErrors because s3fs will try to
    fetch bucket contents without credentials if a credentialed request fails

    :param path: path to the file to read
    :param read_fn: method to read the file, either pandas.read_csv or
      pandas.read_parquet
    :return: DataFrame
    """
    try:
        return read_fn(path, **kwargs)
    except (PermissionError, FileNotFoundError) as e:
        # if the path isn't to S3, simply re-raise
        if not path.startswith("s3://"):
            raise e

        # otherwise invalidate the path in the cache and try again
        s3fs.S3FileSystem(anon=False).invalidate_cache(path)
        return read_fn(path, **kwargs)


def download_dataframe(
    dataframe: pd.DataFrame,
    filename: str,
    index: bool = True,
    exclude: List[str] = None,
) -> FileResponse:
    """
    Returns a FileResponse object containing a CSV of the contents of the
    provided dataframe.

    :param dataframe: The dataframe to download
    :param filename: The name of the file (should omit the `.csv` suffix)
    :param index: Whether to keep or drop the index
    :param exclude: List of columns to exclude from the CSV
    """
    if exclude is None:
        exclude = []

    columns = [c for c in dataframe.columns if c not in exclude]
    dataframe = dataframe[columns]

    _, tmp_filename = mkstemp(suffix=".csv")
    dataframe.to_csv(tmp_filename, index=index)

    # read the file contents and delete the file
    file = open(tmp_filename, "r")
    data = file.read()
    file.close()
    os.remove(tmp_filename)

    # make the response
    response = FileResponse(data, filename=filename, content_type="text/csv")
    response["Content-Disposition"] = f'attachment; filename="{filename}.csv"'
    response["Content-Length"] = len(data)
    response["Content-Type"] = "text/csv"
    return response
