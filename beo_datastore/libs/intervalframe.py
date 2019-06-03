from cached_property import cached_property
import os
import numpy as np
import pandas as pd

from beo_datastore.libs.dataframe import (
    convert_columns_type,
    csv_url_to_dataframe,
    filter_dataframe_by_datetime,
    filter_dataframe_by_weekday,
    filter_dataframe_by_weekend,
    get_dataframe_period,
    merge_dataframe,
    resample_dataframe,
    set_dataframe_index,
)


class ValidationDataFrame(object):
    """
    Base class that offers validation methods for restricting DataFrame format.

    The following attributes must be set in a child class:
    -   default_dataframe
    """

    def __init__(self, dataframe, *args, **kwargs):
        """
        :param dataframe: pandas DataFrame
        """
        self.dataframe = dataframe

    @property
    def dataframe(self):
        return self._dataframe

    @dataframe.setter
    def dataframe(self, dataframe):
        self.validate_dataframe(dataframe)
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
    def validate_dataframe(cls, dataframe):
        """
        Container method to run other validation steps.
        """
        cls.validate_dataframe_index(dataframe)
        cls.validate_dataframe_columns(dataframe)

    @classmethod
    def validate_dataframe_index(cls, dataframe):
        """
        Performs validation checks that dataframe and cls.default_dataframe
        indexes are same type.
        """
        index_type = type(cls.default_dataframe.index)
        if not isinstance(dataframe.index, index_type):
            raise TypeError("dataframe index must be {}.".format(index_type))
        if not dataframe.index.is_monotonic:
            raise IndexError("dataframe index must be in ascending order.")

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

    def __init__(self, reference_object, dataframe, *args, **kwargs):
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
    default_aggregation_column = "kw"

    def __add__(self, other):
        """
        Returns another ValidationIntervalFrame added to self.

        Steps:
            1. Keep existing intervals from self not found in other.
            1. Add overlapping intervals from other to self.
            2. Append new intervals found in other to self.

        :param other: ValidationIntervalFrame
        :return: ValidationIntervalFrame
        """
        # filter other dataframe so columns match
        other = ValidationIntervalFrame(
            other.dataframe[list(self.default_dataframe.columns)]
        )

        if other.dataframe.empty:
            return self
        elif self.dataframe.empty:
            return other

        if self.period != other.period:
            raise IndexError("Periods must match.")

        df_1 = self.dataframe
        df_2 = other.dataframe

        existing_indices = df_1.index.difference(df_2.index)
        overlapping_indices = df_1.index.intersection(df_2.index)
        new_indices = df_2.index.difference(df_1.index)

        return ValidationIntervalFrame(
            df_1.loc[existing_indices]
            .append(
                df_1.loc[overlapping_indices] + df_2.loc[overlapping_indices]
            )
            .append(df_2.loc[new_indices])
            .sort_index()
        )

    @property
    def aggregation_column(self):
        """
        Column used for aggregation calculations, can be overwritten.
        """
        if not hasattr(self, "_aggregation_column"):
            return self.default_aggregation_column
        else:
            return self._aggregation_column

    @aggregation_column.setter
    def aggregation_column(self, aggregation_column):
        """
        Reset cached_property values when aggregation_column is overwritten.
        """
        if aggregation_column is not self.aggregation_column:
            for key in [
                k
                for k, v in self.__dict__.items()
                if isinstance(v, ValidationFrame288)
            ]:
                self.__dict__.pop(key, None)
        self._aggregation_column = aggregation_column

    @property
    def start_timestamp(self):
        """
        Return earliest index value as pandas Timestamp.
        """
        return self.dataframe.index.min()

    @property
    def start_datetime(self):
        """
        Return earliest index value as datetime object.
        """
        return self.start_timestamp.to_pydatetime()

    @property
    def end_timestamp(self):
        """
        Return latest index value as pandas Timestamp.
        """
        return self.dataframe.index.max()

    @property
    def end_datetime(self):
        """
        Return latest timestamp as datetime object.
        """
        return self.end_timestamp.to_pydatetime()

    @cached_property
    def period(self):
        """
        Return the dataframe period as a datetime.timedelta object.
        """
        return get_dataframe_period(self.dataframe)

    @property
    def days(self):
        """
        Returns the number of days in the ValidationIntervalFrame that have
        interval data.
        """
        return len(self.distinct_dates)

    @property
    def distinct_dates(self):
        """
        Return a pandas Index of distinct dates within this
        ValidationIntervalFrame.
        """
        return self.dataframe.index.map(pd.Timestamp.date).unique()

    @cached_property
    def average_frame288(self):
        """
        Returns a ValidationFrame288 of hourly average values in kWh.
        """
        return self.compute_frame288(aggfunc=np.mean, convert_to_kwh=True)

    @cached_property
    def minimum_frame288(self):
        """
        Returns a ValidationFrame288 of hourly minimum values in kW.
        """
        return self.compute_frame288(aggfunc=np.min)

    @cached_property
    def maximum_frame288(self):
        """
        Returns a ValidationFrame288 of hourly maximum values in kW.
        """
        return self.compute_frame288(aggfunc=np.max)

    @cached_property
    def total_frame288(self):
        """
        Returns a ValidationFrame288 of hourly totals in kWh.
        """
        return self.compute_frame288(aggfunc=sum, convert_to_kwh=True)

    @cached_property
    def count_frame288(self):
        """
        Returns a ValidationFrame288 of counts.
        """
        return self.compute_frame288(aggfunc=len)

    @classmethod
    def csv_file_to_intervalframe(
        cls,
        csv_location,
        index_column=None,
        convert_to_datetime=False,
        *args,
        **kwargs
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
            dataframe = set_dataframe_index(
                dataframe, index_column, convert_to_datetime
            )

        return cls(dataframe=dataframe.sort_index(), *args, **kwargs)

    @classmethod
    def csv_url_to_intervalframe(
        cls,
        csv_url,
        index_column=None,
        convert_to_datetime=False,
        *args,
        **kwargs
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

        return cls(dataframe=dataframe.sort_index(), *args, **kwargs)

    def merge_intervalframe(self, other, overwrite_rows=False):
        """
        Merges other_intervalframe.dataframe into self.dataframe. If
        overwrite_rows is True, rows in other_dataframe will overwrite any
        existing rows with colliding indices.

        :param other: IntervalSetFrame object
        :param overwrite_rows: boolean
        """
        return ValidationIntervalFrame(
            merge_dataframe(self.dataframe, other.dataframe, overwrite_rows)
        )

    def filter_by_datetime(
        self, start=pd.Timestamp.min, end_limit=pd.Timestamp.max
    ):
        """
        Returns a ValidationIntervalFrame filtered by index beginning on and
        including start and ending on but excluding end_limit.

        :param start: datetime object
        :param end_limit: datetime object
        :return: ValidationIntervalFrame
        """
        return ValidationIntervalFrame(
            filter_dataframe_by_datetime(
                dataframe=self.dataframe, start=start, end_limit=end_limit
            )
        )

    def filter_by_weekday(self):
        """
        Returns a ValidationIntervalFrame filtered by weekdays.

        :return: ValidationIntervalFrame
        """
        return ValidationIntervalFrame(
            filter_dataframe_by_weekday(dataframe=self.dataframe)
        )

    def filter_by_weekend(self):
        """
        Returns a ValidationIntervalFrame filtered by weekend days.

        :return: ValidationIntervalFrame
        """
        return ValidationIntervalFrame(
            filter_dataframe_by_weekend(dataframe=self.dataframe)
        )

    def filter_by_months(self, months):
        """
        Returns a ValidationIntervalFrame filtered by months.

        :param months: set/list of integers (1-12)
        :return: ValidationIntervalFrame
        """
        return ValidationIntervalFrame(
            dataframe=self.dataframe[self.dataframe.index.month.isin(months)]
        )

    def resample_intervalframe(self, rule, aggfunc):
        """
        Resamples ValidationIntervalFrame to a new period based on rule and
        aggfunc, where rule is an offset alias (ex. "1min") and aggfunc is an
        aggregation function (ex. np.mean).

        :param rule: timeseries offset alias
        :param aggfunc: aggregation function
        :return: ValidationIntervalFrame
        """
        return ValidationIntervalFrame(
            dataframe=resample_dataframe(
                dataframe=self.dataframe, rule=rule, aggfunc=aggfunc
            )
        )

    def compute_frame288(self, aggfunc, convert_to_kwh=False, default_value=0):
        """
        Returns a 12-month by 24-hour (12 x 24 = 288) ValidationFrame288 where
        each cell represents an aggregate computation on all intervals in that
        particular month and hour.

        Converting to kWh (convert_to_kwh=True) may be required for certain
        calculations prior to running the aggregation function.

        Some example aggfunc's are:
            - np.mean for the "average"
            - np.max for the "maximum"
            - len for the "count"

        :param aggfunc: aggregation function
        :param resample: resample dataframe to 1-hour prior to computation
        :param default_value: default value for empty cells
        :return: ValidationFrame288
        """

        # create a default 288
        results_288 = pd.DataFrame(
            default_value,
            columns=[x for x in range(1, 13)],
            index=[x for x in range(0, 24)],
        )

        # filter dataframe to single column of values
        dataframe = self.dataframe[[self.aggregation_column]]

        if convert_to_kwh:
            dataframe = resample_dataframe(
                dataframe=dataframe, rule="1H", aggfunc=np.mean
            )

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

        return ValidationFrame288(results_288)


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

    def __add__(self, other):
        """
        Returns self plus other.

        :param other: ValidationFrame288
        :return: ValidationFrame288
        """
        self.validate_dataframe(other.dataframe)
        return ValidationFrame288(self.dataframe + other.dataframe)

    def __sub__(self, other):
        """
        Returns self minus other.

        :param other: ValidationFrame288
        :return: ValidationFrame288
        """
        self.validate_dataframe(other.dataframe)
        return ValidationFrame288(self.dataframe + other.dataframe)

    def __mul__(self, other):
        """
        Returns self times other.

        :param other: ValidationFrame288
        :return: ValidationFrame288
        """
        self.validate_dataframe(other.dataframe)
        return ValidationFrame288(self.dataframe * other.dataframe)

    def __truediv__(self, other):
        """
        Returns self divided by other.

        :param other: ValidationFrame288
        :return: ValidationFrame288
        """
        self.validate_dataframe(other.dataframe)
        return ValidationFrame288(self.dataframe / other.dataframe)

    @classmethod
    def validate_dataframe_columns(cls, dataframe):
        """
        Performs validation checks that dataframe and cls.default_dataframe:
            - indices are same type.
            - have the same indices.
        """
        index_type = type(cls.default_dataframe.index)
        if not isinstance(dataframe.index, index_type):
            raise TypeError("dataframe index must be {}.".format(index_type))

        if not cls.default_dataframe.index.equals(dataframe.index):
            raise LookupError(
                "dataframe index must have same index as {}'s "
                "default_dataframe.".format(cls.__name__)
            )

    @classmethod
    def convert_matrix_to_frame288(cls, matrix):
        """
        Convert a 12 x 24 matrix commonly found in OpenEI data to a
        ValidationFrame288 object.

        :param matrix: 12 x 24 matrix (array of arrays)
        :return: ValidationFrame288
        """
        dataframe = pd.DataFrame(matrix)
        dataframe.index = dataframe.index + 1
        dataframe.index = pd.to_numeric(dataframe.index)
        dataframe.columns = pd.to_numeric(dataframe.columns)

        if not dataframe.empty:
            return cls(dataframe.transpose())
        else:
            return cls(ValidationFrame288.default_dataframe)

    def get_mask(self, key):
        """
        Returns ValidationFrame288 of True values when cell value matches key
        and False values otherwise.

        :param key: key of any type
        :return: ValidationFrame288
        """
        return ValidationFrame288(self.dataframe == key)


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
