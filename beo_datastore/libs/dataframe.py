import io
import pandas as pd
import requests


def convert_columns_type(dataframe, type_):
    """
    Converts columns type to another type_.

    :param dataframe: pandas DataFrame
    :param type_: Python type
    :return: pandas DataFrame
    """
    dataframe.columns = dataframe.columns.astype(type_)
    return dataframe


def csv_url_to_dataframe(url):
    """
    Reads a csv from a url and returns a pandas Dataframe.

    :param url: url of csv
    :return: pandas DataFrame
    """
    csv = requests.get(url).content
    return pd.read_csv(io.StringIO(csv.decode("utf-8")))


def filter_dataframe_by_datetime(
    dataframe, start=pd.Timestamp.min, end_limit=pd.Timestamp.max
):
    """
    Returns dataframe filtered by index beginning on and including start and
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
    Returns dataframe including only weekdays.

    :param dataframe: pandas DataFrame
    :return: pandas DataFrame
    """
    return dataframe[dataframe.index.dayofweek < 5]


def filter_dataframe_by_weekend(dataframe):
    """
    Returns dataframe including only weekends.

    :param dataframe: pandas DataFrame
    :return: pandas DataFrame
    """
    return dataframe[5 <= dataframe.index.dayofweek]


def merge_dataframe(dataframe, other_dataframe, overwrite_rows=False):
    """
    Merges other_dataframe rows into dataframe. If overwrite_rows is True, rows
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


def resample_dataframe(dataframe, rule, aggfunc):
    """
    Resamples dataframe to a new period based on rule and aggfunc, where
    rule is an offset alias (ex. "1min") and aggfunc is an aggregation
    function (ex. np.mean).

    See the following link for offset aliases:
        http://pandas.pydata.org/pandas-docs/stable/user_guide/
        timeseries.html#timeseries-offset-aliases

    :param dataframe: pandas DataFrame
    :param rule: timeseries offset alias
    :param aggfunc: aggregation function
    :return: pandas DataFrame
    """
    return dataframe.resample(rule=rule).apply(func=aggfunc)


def set_dataframe_index(dataframe, index_column, convert_to_datetime=False):
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
