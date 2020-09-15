from cached_property import cached_property
from datetime import timedelta
from functools import reduce
import numpy as np
import pandas as pd

from beo_datastore.libs.dataframe import (
    add_interval_dataframe,
    csv_url_to_dataframe,
    downsample_dataframe,
    filter_dataframe_by_datetime,
    filter_dataframe_by_weekday,
    filter_dataframe_by_weekend,
    get_dataframe_period,
    merge_dataframe,
    set_dataframe_index,
    read_csv,
    upsample_dataframe,
    resample_dataframe,
)


class ValidationDataFrame(object):
    """
    Base class that offers validation methods for restricting DataFrame format.

    The following attributes must be set in a child class:
    -   default_dataframe
    """

    def __init__(self, dataframe=pd.DataFrame(), *args, **kwargs):
        """
        :param dataframe: pandas DataFrame
        """
        self.dataframe = dataframe

    def __hash__(self):
        """
        Return hash of dataframe elements.
        """
        return int(pd.util.hash_pandas_object(self.dataframe).sum())

    def __eq__(self, other):
        """
        Compare dataframes on __hash__.
        """
        return self.__hash__() == other.__hash__()

    @property
    def dataframe(self):
        return self._dataframe

    @dataframe.setter
    def dataframe(self, dataframe):
        if dataframe.empty:
            dataframe = self.default_dataframe
        self.validate_dataframe(dataframe)
        self._dataframe = dataframe

    @property
    def default_dataframe(self):
        """
        Set as an attribute in child class to default pandas DataFrame. The
        following example is the default for an PowerIntervalFrameFile:

        ex.
            default_dataframe = pd.DataFrame(
                columns=["kw"],
                index=pd.to_datetime([])
            )
        """
        raise NotImplementedError(
            "default_dataframe must be set in {}".format(self.__class__)
        )

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
        if any(dataframe.index.duplicated()):
            raise LookupError("dataframe has duplicate index values.")

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


class ValidationIntervalFrame(ValidationDataFrame):
    """
    Container class for pandas DataFrame. Validations are made to ensure that
    columns and index type match the default_dataframe.

    The following must be set in child classes:
    -   default_dataframe defining the structure of a dataframe.
    -   default_aggregation_column defining the default column used for
        aggregation calculations (i.e. 288 transforms).

    The following attributes can be modified in an instance/object:
    -   aggregation_column can be updated to compute 288 summary tables on a
        different column than the default_aggregation_column.
    """

    @property
    def default_dataframe(self):
        """
        Return this blank dataframe if parquet file does not exist.

        Ex.
            default_dataframe = pd.DataFrame(
                columns=["kw"],
                index=pd.to_datetime([])
            )
        """
        raise NotImplementedError(
            "default_dataframe must be set in {}".format(self.__class__)
        )

    @property
    def default_aggregation_column(self):
        """
        Default column used for aggregation calculations. Can be overwritten.
        """
        raise NotImplementedError(
            "default_aggregation_column must be set in {}".format(
                self.__class__
            )
        )

    def __add__(self, other):
        """
        Return another ValidationIntervalFrame added to self.

        Steps:
            1. Keep existing intervals from self not found in other.
            1. Add overlapping intervals from other to self.
            2. Append new intervals found in other to self.

        :param other: ValidationIntervalFrame
        :return: ValidationIntervalFrame
        """
        # change other to type self.__class__
        other = self.__class__(
            dataframe=other.dataframe[list(self.default_dataframe.columns)]
        )

        if other.dataframe.empty:
            return self
        elif self.dataframe.empty:
            return other

        # filter other dataframe so columns match
        other_dataframe = other.dataframe[list(self.default_dataframe.columns)]

        return self.__class__(
            dataframe=add_interval_dataframe(self.dataframe, other_dataframe)
        )

    def __sub__(self, other):
        """
        Return another ValidationIntervalFrame subtracted from self.

        :param other: ValidationIntervalFrame
        :return: ValidationIntervalFrame
        """
        return self + other.inverse_intervalframe

    @property
    def inverse_intervalframe(self):
        """
        Return self with negative values.
        """
        return self.__class__(dataframe=(-1 * self.dataframe))

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
            self.reset_cached_properties()
        self._aggregation_column = aggregation_column

    @property
    def start_timestamp(self):
        """
        Earliest index value as pandas Timestamp.
        """
        return self.dataframe.index.min()

    @property
    def start_datetime(self):
        """
        Earliest index value as datetime object.
        """
        return self.start_timestamp.to_pydatetime()

    @property
    def end_timestamp(self):
        """
        Latest index value as pandas Timestamp.
        """
        return self.dataframe.index.max()

    @property
    def end_datetime(self):
        """
        Latest timestamp as datetime object.
        """
        return self.end_timestamp.to_pydatetime()

    @property
    def end_limit_timestamp(self):
        """
        Latest index value plus period as pandas Timestamp.
        """
        return self.end_timestamp + self.period

    @property
    def end_limit_datetime(self):
        """
        Latest index value plus period as datetime object.
        """
        return self.end_datetime + self.period

    @property
    def date_range(self):
        """
        Returns a tuple of the intervalframe's start and end times
        """
        return [self.start_datetime, self.end_datetime]

    @property
    def years(self):
        """
        Returns a list of the years that the intervalframe spans
        """
        start_year = self.start_datetime.year
        end_year = self.end_datetime.year
        no_start_year = np.isnan(start_year)
        no_end_year = np.isnan(end_year)

        if no_start_year and no_end_year:
            return []
        if no_start_year:
            return [end_year]
        if no_end_year:
            return [start_year]
        return list(range(start_year, end_year + 1))

    @cached_property
    def period(self):
        """
        The dataframe period as a datetime.timedelta object.
        """
        return get_dataframe_period(self.dataframe)

    @property
    def days(self):
        """
        The number of days in the ValidationIntervalFrame that have interval
        data.
        """
        return len(self.distinct_dates)

    @property
    def distinct_dates(self):
        """
        pandas Index of distinct dates within this ValidationIntervalFrame.
        """
        return self.dataframe.index.map(pd.Timestamp.date).unique()

    @property
    def distinct_month_years(self):
        """
        List of distinct (month, year) tuples within this
        ValidationIntervalFrame.
        """
        return [
            (int(x.split("/")[0]), int(x.split("/")[1]))
            for x in np.unique(self.dataframe.index.strftime("%m/%Y")).tolist()
        ]

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
        Reads a csv from file and returns an ValidationIntervalFrame.

        :param reference_object: reference object ValidationIntervalFrame
            belongs to
        :param csv_location: path of csv file
        :param index_column: column to use as index
        :param convert_to_datetime: convert index_column to datetime if True
        :return: pandas DataFrame
        """
        dataframe = read_csv(csv_location)
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
        Reads a csv from a url and returns an ValidationIntervalFrame.

        :param reference_object: reference object ValidationIntervalFrame
            belongs to
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

        :param other: ValidationIntervalFrame object
        :param overwrite_rows: boolean
        """
        return self.__class__(
            dataframe=merge_dataframe(
                self.dataframe, other.dataframe, overwrite_rows
            )
        )

    def filter_by_datetime(
        self, start=pd.Timestamp.min, end_limit=pd.Timestamp.max
    ):
        """
        Return a ValidationIntervalFrame filtered by index beginning on and
        including start and ending on but excluding end_limit.

        :param start: datetime object
        :param end_limit: datetime object
        :return: ValidationIntervalFrame
        """
        return self.__class__(
            dataframe=filter_dataframe_by_datetime(
                dataframe=self.dataframe, start=start, end_limit=end_limit
            )
        )

    def filter_by_weekday(self):
        """
        Return a ValidationIntervalFrame filtered by weekdays.

        :return: ValidationIntervalFrame
        """
        return self.__class__(
            dataframe=filter_dataframe_by_weekday(dataframe=self.dataframe)
        )

    def filter_by_weekend(self):
        """
        Return a ValidationIntervalFrame filtered by weekend days.

        :return: ValidationIntervalFrame
        """
        return self.__class__(
            dataframe=filter_dataframe_by_weekend(dataframe=self.dataframe)
        )

    def filter_by_months(self, months):
        """
        Return a ValidationIntervalFrame filtered by months.

        :param months: set/list of integers (1-12)
        :return: ValidationIntervalFrame
        """
        return self.__class__(
            dataframe=self.dataframe[self.dataframe.index.month.isin(months)]
        )

    def downsample_intervalframe(self, target_period, aggfunc):
        """
        Downsample a ValidationIntervalFrame to create an equivalent
        ValidationIntervalFrame with intervals occuring on a less-frequent
        basis.

        :param target_period: timedelta object
        :param aggfunc: aggregation function (ex. np.mean)
        :return: pandas ValidationIntervalFrame
        """
        return self.__class__(
            dataframe=downsample_dataframe(
                dataframe=self.dataframe,
                target_period=target_period,
                aggfunc=aggfunc,
            )
        )

    def upsample_intervalframe(self, target_period, method):
        """
        Upsample a ValidationIntervalFrame to create an equivalent
        ValidationIntervalFrame with intervals occuring on a more-frequent
        basis. The final interval is extrapolated forward.

        Example:
        This takes into consideration the final hour when upsampling 1-hour
        intervals to 15-minute intervals in order to not lose the final 3
        intervals.

        :param target_period: timedelta object
        :param method: None, ‘backfill’/’bfill’, ‘pad’/’ffill’, ‘nearest’
        :return: ValidationIntervalFrame
        """
        return self.__class__(
            dataframe=upsample_dataframe(
                dataframe=self.dataframe,
                target_period=target_period,
                method=method,
            )
        )

    def resample_intervalframe(
        self,
        target_period,
        downsample_aggfunc=np.mean,
        upsample_method="ffill",
    ):
        """
        Upsamples or downsamples a ValidationIntervalFrame to create an equivalent
        ValidationIntervalFrame with intervals occuring on a more- or less-frequent
        basis.

        :param target_period: timedelta object
        :param downsample_aggfunc: aggregation function (ex. np.mean)
        :param upsample_method: None, ‘backfill’/’bfill’, ‘pad’/’ffill’, ‘nearest’
        :return: ValidationIntervalFrame
        """
        return self.__class__(
            dataframe=resample_dataframe(
                dataframe=self.dataframe,
                target_period=target_period,
                downsample_aggfunc=downsample_aggfunc,
                upsample_method=upsample_method,
            )
        )

    def compute_frame288(self, aggfunc, convert_to_kwh=False, default_value=0):
        """
        Return a 12-month by 24-hour (12 x 24 = 288) ValidationFrame288 where
        each cell represents an aggregate computation on all intervals in that
        particular month and hour.

        Converting to kWh (convert_to_kwh=True) may be required for certain
        calculations prior to running the aggregation function.

        Some example aggfunc's are:
            - np.mean for the "average"
            - np.max for the "maximum"
            - len for the "count"

        :param aggfunc: aggregation function
        :param convert_to_kwh: resample dataframe to 1-hour prior to aggfunc
        :param default_value: default value for empty cells
        :return: ValidationFrame288
        """

        # TODO: Validate data does not exceed one year

        # create a default 288
        results_288 = pd.DataFrame(
            default_value,
            columns=[x for x in range(1, 13)],
            index=[x for x in range(0, 24)],
        )

        # filter dataframe to single column of values and drop null values
        dataframe = self.dataframe[[self.aggregation_column]].dropna()

        if convert_to_kwh:
            dataframe = downsample_dataframe(
                dataframe=dataframe,
                target_period=timedelta(hours=1),
                aggfunc=np.mean,
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

        return ValidationFrame288(dataframe=results_288)

    def reset_cached_properties(self):
        """
        Resets values of cached properties such as ValidationFrame288
        calculations.
        """
        for key in [
            k
            for k, v in self.__dict__.items()
            if isinstance(v, ValidationFrame288)
        ]:
            self.__dict__.pop(key, None)


class PowerIntervalFrame(ValidationIntervalFrame):
    """
    Container class for pandas DataFrames with the following format:

    DatetimeIndex   |   kw      |
    datetime        |   float   |
    """

    default_dataframe = pd.DataFrame(columns=["kw"], index=pd.to_datetime([]))
    default_aggregation_column = "kw"

    @cached_property
    def energy_intervalframe(self):
        """
        Equivalent EnergyIntervalFrame.
        """
        if self.period == timedelta(0):
            return EnergyIntervalFrame()

        dataframe = self.dataframe.copy()
        dataframe["kwh"] = dataframe["kw"] * (self.period / timedelta(0, 3600))

        return EnergyIntervalFrame(dataframe=dataframe[["kwh"]])

    @property
    def power_intervalframe(self):
        return self

    @cached_property
    def average_frame288(self):
        """
        ValidationFrame288 of hourly average values in kWh.
        """
        return self.compute_frame288(aggfunc=np.mean, convert_to_kwh=True)

    @cached_property
    def minimum_frame288(self):
        """
        ValidationFrame288 of hourly minimum values in kW.
        """
        return self.compute_frame288(aggfunc=np.min)

    @cached_property
    def maximum_frame288(self):
        """
        ValidationFrame288 of hourly maximum values in kW.
        """
        return self.compute_frame288(aggfunc=np.max)

    @cached_property
    def maximum(self):
        """
        Returns the maximum of all values in the `maximum_frame288`
        """
        return self.maximum_frame288.dataframe.max().max()

    @cached_property
    def total_frame288(self):
        """
        ValidationFrame288 of hourly totals in kWh.
        """
        return self.compute_frame288(aggfunc=sum, convert_to_kwh=True)

    @cached_property
    def total(self):
        """
        Returns the sum of all values in the `total_frame288`
        """
        return self.total_frame288.dataframe.sum().sum()

    @cached_property
    def count_frame288(self):
        """
        ValidationFrame288 of counts.
        """
        return self.compute_frame288(aggfunc=len)


class EnergyIntervalFrame(ValidationIntervalFrame):
    """
    Container class for pandas DataFrames with the following format:

    DatetimeIndex   |   kwh     |
    datetime        |   float   |
    """

    default_dataframe = pd.DataFrame(columns=["kwh"], index=pd.to_datetime([]))
    default_aggregation_column = "kwh"

    @cached_property
    def power_intervalframe(self):
        """
        Equivalent PowerIntervalFrame.
        """
        if self.period == timedelta(0):
            return PowerIntervalFrame()

        dataframe = self.dataframe.copy()
        dataframe["kw"] = dataframe["kwh"] * (timedelta(0, 3600) / self.period)

        return PowerIntervalFrame(dataframe=dataframe[["kw"]])

    @property
    def energy_intervalframe(self):
        return self

    @cached_property
    def average_frame288(self):
        """
        ValidationFrame288 of hourly average values in kWh.
        """
        return self.power_intervalframe.average_frame288

    @cached_property
    def minimum_frame288(self):
        """
        ValidationFrame288 of hourly minimum values in kW.
        """
        return self.power_intervalframe.minimum_frame288

    @cached_property
    def maximum_frame288(self):
        """
        ValidationFrame288 of hourly maximum values in kW.
        """
        return self.power_intervalframe.maximum_frame288

    @cached_property
    def total_frame288(self):
        """
        ValidationFrame288 of hourly totals in kWh.
        """
        return self.power_intervalframe.total_frame288

    @cached_property
    def count_frame288(self):
        """
        ValidationFrame288 of counts.
        """
        return self.power_intervalframe.count_frame288


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
        Return self plus other.

        :param other: ValidationFrame288
        :return: ValidationFrame288
        """
        self.validate_dataframe(other.dataframe)
        return self.__class__(dataframe=self.dataframe + other.dataframe)

    def __sub__(self, other):
        """
        Return self minus other.

        :param other: ValidationFrame288
        :return: ValidationFrame288
        """
        self.validate_dataframe(other.dataframe)
        return self.__class__(dataframe=self.dataframe + other.dataframe)

    def __mul__(self, other):
        """
        Return self times other.

        :param other: ValidationFrame288
        :return: ValidationFrame288
        """
        self.validate_dataframe(other.dataframe)
        return self.__class__(dataframe=self.dataframe * other.dataframe)

    def __truediv__(self, other):
        """
        Return self divided by other.

        :param other: ValidationFrame288
        :return: ValidationFrame288
        """
        self.validate_dataframe(other.dataframe)
        return self.__class__(dataframe=self.dataframe / other.dataframe)

    @cached_property
    def normalized_frame288(self):
        """
        Return ValidationFrame288 where values are between -1 and 1.
        """
        abs_max = max(
            abs(self.dataframe.min().min()), abs(self.dataframe.max().max())
        )

        if abs_max == 0:
            # empty ValidationFrame288
            return self
        else:
            return self.__class__(dataframe=self.dataframe / abs_max)

    @cached_property
    def flattened_array(self):
        """
        Return all 288 values as a single array ordered from left to right by
        month-hour.

        Ex. [Jan hour 1, Jan hour 1, ..., Dec hour 22, Dec hour 23]
        """
        return reduce(
            lambda x, y: x + y,
            [list(self.dataframe[x]) for x in self.dataframe],
        )

    @classmethod
    def validate_dataframe_index(cls, dataframe):
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
            return cls(dataframe=dataframe.transpose())
        else:
            return cls(dataframe=ValidationFrame288.default_dataframe)

    @classmethod
    def convert_flattened_array_to_frame288(cls, flattened_array):
        """
        Convert an array of 288 values ordered by month-hour to a
        ValidationFrame288 object.

        :param flattened_array: array of floats (288 length)
        :return: ValidationFrame288
        """
        matrix = [
            flattened_array[(x - 1) * 24 : (x * 24)] for x in range(1, 13)
        ]
        return cls.convert_matrix_to_frame288(matrix=matrix)

    def get_mask(self, key):
        """
        Return ValidationFrame288 of True values when cell value matches key
        and False values otherwise.

        :param key: key of any type
        :return: ValidationFrame288
        """
        return self.__class__(dataframe=self.dataframe == key)

    def compute_intervalframe(self, start, end_limit, period):
        """
        Returns a time-series dataframe with indexed timestamps from `start` to
        `end_limit`. The values at each interval will be taken from the 288
        dataframe, accounting for the month and time of day.

        :param start: datetime object
        :param end_limit: datetime object
        :param period: timedelta object
        :return: ValidationIntervalFrame
        """
        df = pd.DataFrame()
        df["start"] = pd.date_range(
            start=start, end=end_limit - period, freq=period
        )
        df.set_index("start", inplace=True)

        # Convert the 288 to a dataframe with `value` and `month-hour` columns. The month-hour
        # column is computed from the index of the unstacked dataframe, which comes in tuples
        # of (month, hour of day)
        df_288 = pd.DataFrame(self.dataframe.unstack(), columns=["value"])
        df_288["month-hour"] = df_288.index.map(lambda x: x[0] * 100 + x[1])
        df["month-hour"] = df.index.map(lambda x: x.month * 100 + x.hour)

        return (
            df.reset_index()
            .merge(df_288, on="month-hour", how="left")[["start", "value"]]
            .set_index("start")
        )


class ArbitraryDataFrame(ValidationDataFrame):
    """
    Container class for arbitrary DataFrames.
    """

    default_dataframe = pd.DataFrame([])

    @classmethod
    def validate_dataframe_index(cls, dataframe):
        """
        Ignore index checks.
        """
        pass

    @classmethod
    def validate_dataframe_columns(cls, dataframe):
        """
        Ignore column checks.
        """
        pass
