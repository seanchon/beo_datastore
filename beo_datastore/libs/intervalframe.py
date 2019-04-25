import os
import numpy as np
import pandas as pd

from beo_datastore.libs.dataframe import (
    convert_columns_type,
    csv_url_to_dataframe,
)


class ValidationDataFrame(object):
    """
    Base class that offers validation methods for restricting DataFrame format.

    The following attributes must be set in a child class:
    -   default_dataframe
    """

    def __init__(self, dataframe):
        """
        :param dataframe: pandas DataFrame
        """
        self.dataframe = dataframe

    @property
    def dataframe(self):
        return self._dataframe

    @dataframe.setter
    def dataframe(self, dataframe):
        self.validate_dataframe_index(dataframe)
        self.validate_dataframe_columns(dataframe)
        self._dataframe = dataframe

    @property
    def default_dataframe(self):
        """
        Set as an attribute in child class to default pandas DataFrame. The
        following example is the default for an IntervalFrameFile:

        ex.
            default_dataframe = pd.DataFrame(
                columns=["kw"],
                index=pd.to_datetime([])
            )
        """
        raise NotImplementedError()

    @classmethod
    def validate_dataframe_index(cls, dataframe):
        """
        Performs validation checks that dataframe and cls.default_dataframe
        indexes are same type.
        """
        index_type = type(cls.default_dataframe.index)
        if not isinstance(dataframe.index, index_type):
            raise TypeError("dataframe index must be {}.".format(index_type))

    @classmethod
    def validate_dataframe_columns(cls, dataframe):
        """
        Performs validation checks that dataframe and cls.default_dataframe:
            - columns are same type.
            - have the same columns.
        """
        columns_type = type(cls.default_dataframe.columns)
        if not isinstance(dataframe.columns, columns_type):
            raise TypeError(
                "dataframe columns must be {}.".format(columns_type)
            )

        if not cls.default_dataframe.columns.equals(dataframe.columns):
            raise LookupError(
                "dataframe columns must have same columns as {}'s "
                "default_dataframe.".format(cls.__name__)
            )


class DataFrameFile(ValidationDataFrame):
    """
    Base class that offers file-handling methods for saving/retrieving pandas
    DataFrames to/from disk.

    The following attributes must be set in a child class:
    -   reference_model
    -   file_directory
    """

    def __init__(self, reference_object, dataframe):
        """
        :param reference_object: reference object DataFrame belongs to
        :param dataframe: pandas DataFrame
        """
        self.validate_model(reference_object)
        self.reference_object = reference_object
        self.dataframe = dataframe

    def save(self):
        """
        Saves dataframe to disk.
        """
        if not os.path.exists(self.file_directory):
            os.mkdir(self.file_directory)
        if self.reference_object.id is not None:
            self.dataframe.to_parquet(self.file_path)

    def delete(self):
        """
        Deletes dataframe from disk.
        """
        if os.path.exists(self.file_path):
            os.remove(self.file_path)

    @property
    def reference_model(self):
        """
        Set as an attribute in child class to a reference model for DataFrames.

        Ex.
            reference_model = Channel
        """
        raise NotImplementedError()

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

    def validate_model(self, reference_object):
        """
        Raises an Exception if reference_object is not an instance of
        self.reference_model.
        """
        if not isinstance(reference_object, self.reference_model):
            raise TypeError(
                "reference_object should be of type {}.".format(
                    self.reference_model
                )
            )


class ValidationIntervalFrame(ValidationDataFrame):
    """
    Container class for pandas DataFrames with the following format:

    DatetimeIndex   |   kw      |
    datetime        |   float   |

    The following attributes can be modified for additional use cases:
    -   default_dataframe can be updated to reflect a different default
        DataFrame structure.
    -   aggregation_column can be updated to compute 288 summary tables on a
        different column.
    """

    # returns this blank dataframe if parquet file does not exist
    default_dataframe = pd.DataFrame(columns=["kw"], index=pd.to_datetime([]))

    # default column used for aggregation calculations, can be overwritten
    aggregation_column = "kw"

    @property
    def average_288_dataframe(self):
        """
        Returns a 12 x 24 dataframe of average values.
        """
        return self.compute_288_dataframe(aggfunc=np.mean)

    @property
    def minimum_288_dataframe(self):
        """
        Returns a 12 x 24 dataframe of minimum values.
        """
        return self.compute_288_dataframe(aggfunc=np.min)

    @property
    def maximum_288_dataframe(self):
        """
        Returns a 12 x 24 dataframe of maximum values.
        """
        return self.compute_288_dataframe(aggfunc=np.max)

    @property
    def sum_288_dataframe(self):
        """
        Returns a 12 x 24 dataframe of sums.
        """
        return self.compute_288_dataframe(aggfunc=sum)

    @property
    def count_288_dataframe(self):
        """
        Returns a 12 x 24 dataframe of counts.
        """
        return self.compute_288_dataframe(aggfunc=len)

    @staticmethod
    def set_index(dataframe, index_column, convert_to_datetime=False):
        """
        Sets index on index_column. If convert_to_datetime is True, attempts
        to set index as a DatetimeIndex.

        :param dataframe: pandas DataFrame
        :param index_column: column to use as index
        :param convert_to_datetime: convert index_column to datetime if True
        :return: pandas DataFrame
        """
        if convert_to_datetime:
            dataframe[index_column] = pd.to_datetime(dataframe[index_column])
        dataframe.set_index(index_column, inplace=True)

        return dataframe

    @classmethod
    def csv_file_to_intervalframe(
        cls,
        reference_object,
        csv_location,
        index_column=None,
        convert_to_datetime=False,
    ):
        """
        Reads a csv from file and returns an IntervalFrameFile.

        :param reference_object: reference object IntervalFrameFile belongs to
        :param csv_location: path of csv file
        :param index_column: column to use as index
        :param convert_to_datetime: convert index_column to datetime if True
        :return: pandas DataFrame
        """
        dataframe = pd.read_csv(csv_location)
        if index_column:
            dataframe = cls.set_index(
                dataframe, index_column, convert_to_datetime
            )

        return cls(reference_object, dataframe)

    @classmethod
    def csv_url_to_intervalframe(
        cls,
        reference_object,
        csv_url,
        index_column=None,
        convert_to_datetime=False,
    ):
        """
        Reads a csv from a url and returns an IntervalFrameFile.

        :param reference_object: reference object IntervalFrameFile belongs to
        :param csv_url: url of csv
        :param index_column: column to use as index
        :param convert_to_datetime: convert index_column to datetime if True
        :return: pandas DataFrame
        """
        dataframe = csv_url_to_dataframe(csv_url)
        if index_column:
            dataframe = cls.set_index(
                dataframe, index_column, convert_to_datetime
            )

        return cls(reference_object, dataframe)

    def compute_288_dataframe(self, aggfunc, default_value=0):
        """
        Calculates a 12-month by 24-hour (12 x 24 = 288) dataframe where each
        cell represents all values in that particular month and hour passed
        through the aggfunc.

        Some example aggfunc's are:
            - np.mean for the "average"
            - np.max for the "maximum"
            - len for the "count"

        :param aggfunc: aggregation function
        :param default_value: default value for empty cells
        :return: 12 x 24 pandas DataFrame
        """

        # create a default 288
        results_288 = pd.DataFrame(
            default_value,
            columns=[x for x in range(1, 13)],
            index=[x for x in range(0, 24)],
        )

        # create summary 288 limited to self.aggregation_column
        dataframe = self.dataframe[[self.aggregation_column]]

        if not dataframe.empty:
            calculated_288 = (
                pd.crosstab(
                    dataframe.index.hour,
                    dataframe.index.month,
                    dataframe.values,
                    aggfunc=aggfunc,
                )
                .rename_axis(None)
                .rename_axis(None, axis=1)
            )

            # merge summary 288 values into default values
            results_288.update(calculated_288)

        return results_288


class IntervalFrameFile(ValidationIntervalFrame, DataFrameFile):
    """
    Combines a ValidationIntervalFrame with file-handling capabilities of a
    DataFrameFile.
    """

    pass


class ValidationFrame288(ValidationDataFrame):
    """
    Container class for 12 x 24 pandas DataFrames with the following format:

    Int64Index  |   1       |   2       |...    |   11      |   12      |
    0           |   float   |   float   |       |   float   |   float   |
    1           |   float   |   float   |       |   float   |   float   |
    ...
    22          |   float   |   float   |       |   float   |   float   |
    23          |   float   |   float   |       |   float   |   float   |
    """

    # returns this blank dataframe if parquet file does not exist
    default_dataframe = pd.DataFrame(
        columns=np.array(range(1, 13)), index=np.array(range(0, 24))
    )


class Frame288File(ValidationFrame288, DataFrameFile):
    """
    Combines a ValidationIntervalFrame with file-handling capabilities of a
    DataFrameFile.
    """

    def save(self, *args, **kwargs):
        """
        Convert columns to string on save().
        """
        if not os.path.exists(self.file_directory):
            os.mkdir(self.file_directory)
        if self.reference_object.id is not None:
            dataframe = convert_columns_type(self.dataframe, str)
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
