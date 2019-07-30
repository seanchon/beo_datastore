import numpy as np
import os
import pandas as pd


from beo_datastore.libs.battery import BatteryIntervalFrame
from beo_datastore.libs.dataframe import convert_columns_type
from beo_datastore.libs.intervalframe import (
    ValidationDataFrame,
    ValidationFrame288,
    ValidationIntervalFrame,
)
from beo_datastore.libs.utils import mkdir_p


class DataFrameFile(ValidationDataFrame):
    """
    Base class that offers file-handling methods for saving/retrieving pandas
    DataFrames to/from disk.

    The following attributes must be set in a child class:
    -   file_directory
    """

    def __init__(self, reference_object, dataframe, *args, **kwargs):
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
        raise NotImplementedError()

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
    def get_frame_from_file(cls, reference_object):
        """
        Returns DataFrameFile based on reference_object.id if it exists.

        :param reference_object: reference object IntervalFrameFile belongs to
        :return: cls instance
        """
        file_path = cls.get_file_path(reference_object)

        if os.path.exists(file_path):
            return cls(reference_object, pd.read_parquet(file_path))
        else:
            return cls(reference_object, cls.default_dataframe)


class IntervalFrameFile(ValidationIntervalFrame, DataFrameFile):
    """
    Combines a ValidationIntervalFrame with file-handling capabilities of a
    DataFrameFile.
    """

    pass


class Frame288File(ValidationFrame288, DataFrameFile):
    """
    Combines a ValidationIntervalFrame with file-handling capabilities of a
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
    def get_frame_from_file(cls, reference_object, *args, **kwargs):
        """
        Returns Frame288File based on reference_object.id if it exists. Convert
        columns to Int64 on get_frame_from_file().

        :param reference_object: reference object IntervalFrameFile belongs to
        :return: pandas Frame288File
        """
        file_path = cls.get_file_path(reference_object)

        if os.path.exists(file_path):
            dataframe = pd.read_parquet(file_path)
            dataframe = convert_columns_type(dataframe, np.int64)
            return cls(reference_object, dataframe)
        else:
            return cls(reference_object, cls.default_dataframe)


class BatteryIntervalFrameFile(BatteryIntervalFrame, DataFrameFile):
    """
    Combines a BatteryIntervalFrame with file-handling capabilities of a
    DataFrameFile.
    """

    pass