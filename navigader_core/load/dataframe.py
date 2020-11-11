import io
from datetime import timedelta

import numpy as np
import pandas as pd
import requests
from scipy import stats


def add_interval_dataframe(dataframe_1, dataframe_2):
    """
    Adds dataframe_1 to dataframe_2 when both DataFrames consist of intervals.
    """
    if dataframe_2.empty:
        return dataframe_1
    elif dataframe_1.empty:
        return dataframe_2

    dataframe_1_period = get_dataframe_period(dataframe_1)
    dataframe_2_period = get_dataframe_period(dataframe_2)

    if dataframe_1_period < dataframe_2_period:
        dataframe_2 = upsample_dataframe(
            dataframe=dataframe_2,
            target_period=dataframe_1_period,
            method="ffill",
        )
    elif dataframe_2_period < dataframe_1_period:
        dataframe_1 = upsample_dataframe(
            dataframe=dataframe_1,
            target_period=dataframe_2_period,
            method="ffill",
        )

    return dataframe_1.add(dataframe_2, fill_value=0)


def convert_columns_type(dataframe, type_):
    """
    Convert columns type to another type_.

    :param dataframe: pandas DataFrame
    :param type_: Python type
    :return: pandas DataFrame
    """
    dataframe.columns = dataframe.columns.astype(type_)
    return dataframe


def csv_url_to_dataframe(url):
    """
    Read a csv from a url and return a pandas Dataframe.

    :param url: url of csv
    :return: pandas DataFrame
    """
    csv = requests.get(url).content
    return pd.read_csv(io.StringIO(csv.decode("utf-8")))


def get_dataframe_period(
    dataframe: pd.DataFrame, by_column="", n=96
) -> timedelta:
    """
    Return dataframe period as a timedelta object,
    with the index by default or with a column if provided.

    :param dataframe: pandas DataFrame with DatetimeIndex
    :param by_column: if provided, use this column for timestamps
    :param n: run on first n lines to reduce computation
    :return: timedelta
    """
    if n:
        dataframe = dataframe.head(n=n)

    if by_column:
        values = pd.DatetimeIndex(dataframe[by_column])
    else:
        values = dataframe.index.values
    results = stats.mode(np.diff(values)).mode.tolist()
    results = [x / 1e9 for x in results]

    if not results:
        return timedelta(seconds=0)
    elif len(results) == 1:
        return timedelta(seconds=results[0])
    else:
        raise IndexError(
            "Multiple DataFrame periods detected - {}.".format(
                ", ".join([str(x) for x in results])
            )
        )


def get_dataframe_max_difference(dataframe_1, dataframe_2):
    """
    Return the maximum absolute difference between corresponding cells in two
    dataframes.

    Ex.
    dataframe_1 = pd.DataFrame([[-2.5, -1.5], [1, 2]])
    dataframe_2 = pd.DataFrame([[0, 0], [0, 0]])

    returns 2.5
    """
    return np.max(np.max(np.absolute(dataframe_1 - dataframe_2)))


def get_unique_values(dataframe):
    """
    Return sorted array of unique values found in a dataframe.
    """
    return sorted(pd.unique(dataframe.values.ravel()))


def filter_dataframe_by_datetime(
    dataframe, start=pd.Timestamp.min, end_limit=pd.Timestamp.max
):
    """
    Return dataframe filtered by index beginning on and including start and
    ending on but excluding end_limit.

    :param dataframe: pandas DataFrame
    :param start: datetime object
    :param end_limit: datetime object
    :return: pandas DataFrame
    """
    return dataframe[
        (pd.to_datetime(start) <= dataframe.index)
        & (dataframe.index < pd.to_datetime(end_limit))
    ]


def filter_dataframe_by_weekday(dataframe):
    """
    Return dataframe including only weekdays.

    :param dataframe: pandas DataFrame
    :return: pandas DataFrame
    """
    return dataframe[dataframe.index.dayofweek < 5]


def filter_dataframe_by_weekend(dataframe):
    """
    Return dataframe including only weekends.

    :param dataframe: pandas DataFrame
    :return: pandas DataFrame
    """
    return dataframe[5 <= dataframe.index.dayofweek]


def merge_dataframe(dataframe, other_dataframe, overwrite_rows=False):
    """
    Merge other_dataframe rows into dataframe. If overwrite_rows is True, rows
    in other_dataframe will overwrite any existing rows with colliding indices.

    :param other_dataframe: pandas DataFrame
    :param overwrite_rows: boolean
    :return: pandas DataFrame
    """
    if dataframe.empty:
        return other_dataframe

    if overwrite_rows:
        dataframe = other_dataframe.combine_first(dataframe)
    else:
        dataframe = dataframe.combine_first(other_dataframe)

    return dataframe


def downsample_dataframe(dataframe, target_period, aggfunc):
    """
    Downsample a dataframe to create an equivalent DataFrame with intervals
    occurring on a less-frequent basis.

    :param dataframe: pandas DataFrame
    :param target_period: timedelta object
    :param aggfunc: aggregation function (ex. np.mean)
    :return: pandas DataFrame
    """
    period = get_dataframe_period(dataframe)

    if target_period < period:
        raise ValueError(
            "target_period must be greater than or equal to period."
        )

    return (
        dataframe.astype(float)
        .resample(rule=target_period)
        .apply(func=aggfunc)
    )


def upsample_dataframe(dataframe, target_period, method):
    """
    Upsample a dataframe to create an equivalent DataFrame with intervals
    occurring on a more-frequent basis. The final interval is extrapolated
    forward.

    Example:
    This takes into consideration the final hour when upsampling 1-hour
    intervals to 15-minute intervals in order to not lose the final 3 intervals.

    :param dataframe: pandas DataFrame
    :param target_period: timedelta object
    :param method: None, ‘backfill’/’bfill’, ‘pad’/’ffill’, ‘nearest’
    :return: pandas DataFrame
    """
    period = get_dataframe_period(dataframe)

    if target_period > period:
        raise ValueError("target_period must be less than or equal to period.")

    # TODO: Don't ffill any interval gaps
    return dataframe.reindex(
        pd.date_range(
            dataframe.index.min(),
            dataframe.index.max() + period,
            freq=target_period,
            closed="left",
        ),
        method=method,
    )


def resample_dataframe(
    dataframe,
    target_period,
    downsample_aggfunc=np.mean,
    upsample_method="ffill",
):
    """
    Upsamples or downsample a dataframe to create an equivalent DataFrame with intervals
    occurring on a more- or less-frequent basis.

    :param dataframe: pandas DataFrame
    :param target_period: timedelta object
    :param downsample_aggfunc: aggregation function (ex. np.mean)
    :param upsample_method: None, ‘backfill’/’bfill’, ‘pad’/’ffill’, ‘nearest’
    :return: pandas DataFrame
    """
    period = get_dataframe_period(dataframe)

    if target_period > period:
        return downsample_dataframe(
            dataframe, target_period, downsample_aggfunc
        )
    elif target_period < period:
        return upsample_dataframe(dataframe, target_period, upsample_method)
    else:
        return dataframe


def set_dataframe_index(dataframe, index_column, convert_to_datetime=False):
    """
    Set index on index_column. If convert_to_datetime is True, attempt
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
