import numpy as np
import os

from beo_datastore.libs.dataframe import convert_columns_type, read_parquet
from beo_datastore.libs.intervalframe import (
    ValidationDataFrame,
    ValidationFrame288,
    PowerIntervalFrame,
    ArbitraryDataFrame,
)
from beo_datastore.libs.utils import mkdir_p


class DataFrameFile(ValidationDataFrame):
    """
    Base class that offers file-handling methods for saving/retrieving pandas
    DataFrames to/from disk.

    The following attributes must be set in a child class:
    -   file_directory
    """

    def __init__(self, dataframe, reference_object=None, *args, **kwargs):
        """
        :param reference_object: reference object DataFrame belongs to
        :param dataframe: pandas DataFrame
        """
        self.reference_object = reference_object
        self.dataframe = dataframe

    def save(self):
        """
        Saves dataframe to disk.
        """
        mkdir_p(self.file_directory)
        if self.reference_object.id is not None:
            self.dataframe.to_parquet(self.file_path)

    def delete(self):
        """
        Deletes dataframe from disk.
        """
        if os.path.exists(self.file_path):
            os.remove(self.file_path)

    @property
    def file_directory(self):
        """
        Set as an attribute in child class to directory where files should be
        stored.

        Ex.
            file_directory = "project_root/directory_xyz/"
        """
        raise NotImplementedError(
            "file_directory must be set in {}".format(self.__class__)
        )

    @property
    def filename(self):
        return self.get_filename(self.reference_object)

    @property
    def file_path(self):
        """
        Full file path of parquet file.
        """
        return self.get_file_path(self.reference_object)

    @classmethod
    def get_filename(cls, reference_object):
        """
        Generate filename of parquet file in format
        <class name>_<reference_object.id>.parquet.
        """
        return "{}_{}.parquet".format(cls.__name__, reference_object.id)

    @classmethod
    def get_file_path(cls, reference_object):
        """
        Generate file_path of parquet file.
        """
        return os.path.join(
            cls.file_directory, cls.get_filename(reference_object)
        )

    @classmethod
    def get_frame_from_file(
        cls, reference_object, file_path=None, *args, **kwargs
    ):
        """
        Return DataFrameFile based on reference_object.id if it exists.

        :param reference_object: reference object PowerIntervalFrameFile belongs to
        :param file_path: location of parquet file
        :return: cls instance
        """
        if file_path is None:
            file_path = cls.get_file_path(reference_object)

        try:
            return cls(
                dataframe=read_parquet(file_path),
                reference_object=reference_object,
            )
        except OSError:
            return cls(
                dataframe=cls.default_dataframe,
                reference_object=reference_object,
            )


class PowerIntervalFrameFile(PowerIntervalFrame, DataFrameFile):
    """
    Combines a PowerIntervalFrame with file-handling capabilities of a
    DataFrameFile.
    """

    pass


class Frame288File(ValidationFrame288, DataFrameFile):
    """
    Combines a PowerIntervalFrame with file-handling capabilities of a
    DataFrameFile.
    """

    def save(self, *args, **kwargs):
        """
        Convert columns to string on save().
        """
        mkdir_p(self.file_directory)
        if self.reference_object.id is not None:
            dataframe = convert_columns_type(self.dataframe.copy(), str)
            dataframe.to_parquet(self.file_path)

    @classmethod
    def get_frame_from_file(
        cls, reference_object, file_path=None, *args, **kwargs
    ):
        """
        Return Frame288File based on reference_object.id if it exists. Convert
        columns to Int64 on get_frame_from_file().

        :param reference_object: reference object PowerIntervalFrameFile belongs to
        :param file_path: location of parquet file
        :return: pandas Frame288File
        """
        if file_path is None:
            file_path = cls.get_file_path(reference_object)

        try:
            dataframe = read_parquet(file_path)
            dataframe = convert_columns_type(dataframe, np.int64)
            return cls(dataframe=dataframe, reference_object=reference_object)
        except OSError:
            return cls(
                dataframe=cls.default_dataframe,
                reference_object=reference_object,
            )


class ArbitraryDataFrameFile(ArbitraryDataFrame, DataFrameFile):
    """
    Combines a ArbitraryDataFrame with file-handling capabilities of a
    DataFrameFile.
    """

    def save(self, *args, **kwargs):
        """
        Convert columns to string on save().
        """
        mkdir_p(self.file_directory)
        if self.reference_object.id is not None:
            dataframe = convert_columns_type(self.dataframe.copy(), str)
            dataframe.to_parquet(self.file_path)
