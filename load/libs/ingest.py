"""
15-minute interval data must include the following columns.
SA_ID,DIR,DATE,RS,0:15,0:30,0:45,1:00,1:15,1:30,1:45,2:00,2:15,2:30,2:45,3:00,3:15,3:30,3:45,4:00,4:15,4:30,4:45,5:00,5:15,5:30,5:45,6:00,6:15,6:30,6:45,7:00,7:15,7:30,7:45,8:00,8:15,8:30,8:45,9:00,9:15,9:30,9:45,10:00,10:15,10:30,10:45,11:00,11:15,11:30,11:45,12:00,12:15,12:30,12:45,13:00,13:15,13:30,13:45,14:00,14:15,14:30,14:45,15:00,15:15,15:30,15:45,16:00,16:15,16:30,16:45,17:00,17:15,17:30,17:45,18:00,18:15,18:30,18:45,19:00,19:15,19:30,19:45,20:00,20:15,20:30,20:45,21:00,21:15,21:30,21:45,22:00,22:15,22:30,22:45,23:00,23:15,23:30,23:45,24:00

60-minute interval data must include the following columns.
SA_ID,DIR,DATE,RS,1:00,2:00,3:00,4:00,5:00,6:00,7:00,8:00,9:00,10:00,11:00,12:00,13:00,14:00,15:00,16:00,17:00,18:00,19:00,20:00,21:00,22:00,23:00,24:00
"""

import csv
from datetime import datetime, timedelta
import pandas as pd
import re

from navigader_core.load.dataframe import get_dataframe_period


# BEO source file located at https://tvrp.app.box.com/file/420277168014


def csv_split(line, delimiter=",", quotechar='"'):
    """
    Perform a split on a CSV row and ignore delimiter inside of quotechar.
    """
    return list(csv.reader([line], delimiter=delimiter, quotechar=quotechar))[0]


def get_sa_id_column(dataframe):
    """
    Return column name from dataframe containing SA IDs.
    """
    sa_columns = [x for x in dataframe.columns if "SA" in x]
    if len(sa_columns) != 1:
        raise ValueError("A unique SA ID column not found.")
    return sa_columns[0]


def get_dataframe_saids(dataframe, sa_column):
    """
    Return all unique SA IDs in dataframe.
    """
    return set(dataframe[sa_column])


def filter_dataframe(dataframe, column_list=[]):
    """
    Filter Item 17 dataframe.
    """
    return dataframe[column_list]


def reformat_timestamp_columns(dataframe):
    """
    Reformat timestamp column to 24-hour timestamp (ex. H_0_15 to 0:15).
    """
    # TODO: subtrack 15-minutes. hour 24 should be represented as 0.
    return dataframe.rename(
        columns=lambda x: x.replace("H_", "").replace("_", "")
    )


def is_time_str(time_string: str) -> bool:
    """
    Return True if time_string in H:MM, HH:MM, H:MM:SS, HH:MM:SS format,
    ignore double quotes.
    """
    return bool(re.search(r"^\"?\d{1,2}:\d{2}(:\d{2})?\"?$", time_string))


def get_timestamp_columns(columns: list) -> list:
    """
    Return column names containing timestamps.
    """
    return [x for x in columns if is_time_str(x)]


def create_naive_datetime(time_string: str) -> datetime:
    """
    Convert time strings either HH:MM:SS or HH:MM,to a naive_datetime on December 31, 1999.
    Convert occurrences of "24" to "00".
    """
    # Ignore seconds i.e. convert %H:%M:%S to %H:%M
    if len(time_string.split(":")) == 3:
        time_string = time_string[:-3]

    hour, minute = time_string.split(":")
    if hour == "24":
        time_string = "00:" + minute

    return datetime.strptime("12/31/1999 " + time_string, "%m/%d/%Y %H:%M")


def get_timedelta_from_time_strings(time_strings: list) -> timedelta:
    """
    Examine a list of time strings (ex. "08:00") and determine the most-common
    timedelta between them.
    """
    if len(time_strings) < 2:
        return timedelta(0)

    datetimes = [create_naive_datetime(x) for x in time_strings]
    datetimes.sort()

    # get diffs between n+1 and n element
    diffs = [j - i for i, j in zip(datetimes[:-1], datetimes[1:])]

    # return most frequent diff
    return max(set(diffs), key=diffs.count)


def shift_time_string(time_string: str, amount: timedelta) -> str:
    """
    Shift string representation of time (ex. "08:00") by amount of time.

    ex.
        "08:00" shifted by timedelta(minutes=-15) yields "07:45"
    """
    naive_datetime = create_naive_datetime(time_string)
    shifted_datetime = naive_datetime + amount

    return shifted_datetime.strftime("%H:%M")


def get_rate_plan_name(dataframe, sa_column, said):
    """
    Return rate plan name corresponding to SA ID.
    """
    rate_plan_name = dataframe[dataframe[sa_column] == said]["RS"].iloc[0]
    if rate_plan_name != rate_plan_name:  # check for nan
        rate_plan_name = None

    return rate_plan_name


def stack_dataframe(dataframe):
    """
    Transform dataframe with date index on row and time index on column to a
    dataframe with DateTimeIndex.
    """
    df = dataframe.stack().reset_index()
    df["DATE"] = df["DATE"].astype(str)
    df["level_1"] = df["level_1"].astype(str)
    df["index"] = df["DATE"] + " " + df["level_1"]
    df.drop(["DATE", "level_1"], axis=1, inplace=True)
    df.rename(index=str, columns={0: "kw"}, inplace=True)
    df.set_index("index", inplace=True)
    try:
        df.index = pd.to_datetime(df.index, format="%m/%d/%Y %H:%M")
    except ValueError:  # two-digit year
        df.index = pd.to_datetime(df.index, format="%m/%d/%y %H:%M")
    df.sort_index(inplace=True)
    df = df.loc[~df.index.duplicated(keep="first")]

    return df


def reformat_item_17(dataframe):
    """
    Return a dataframe in PowerIntervalFrame format.

    :param dataframe: pandas DataFrame
    :return: dict
    """
    dataframe = reformat_timestamp_columns(dataframe)

    timestamp_columns = get_timestamp_columns(dataframe.columns)
    timestamp_columns = [timestamp_columns[-1]] + timestamp_columns[:-1]
    columns = ["DATE"] + timestamp_columns

    unit_of_measure = set(dataframe["UOM"])
    direction = set(dataframe["DIR"])

    if not dataframe.empty and unit_of_measure not in [{"KW"}, {"KWH"}]:
        raise LookupError("UOM column should contain only KW or KWH.")

    if not dataframe.empty and direction not in [{"D"}, {"R"}]:
        raise LookupError("DIR column should contain only single direction.")

    dataframe = dataframe[columns]
    dataframe.set_index("DATE", inplace=True)

    # transform to single column of values
    df = stack_dataframe(dataframe)
    df["kw"] = pd.to_numeric(df["kw"], errors="coerce")

    # invert export values
    if direction == {"R"}:
        df["kw"] = df["kw"] * -1.0

    # convert from kwh to kw
    if not df.empty and unit_of_measure == {"KWH"}:
        dataframe_period = get_dataframe_period(df)
        multiplier = timedelta(0, 3600) / dataframe_period
        if multiplier != 1:
            df["kw"] = df["kw"] * multiplier

    return df


def get_gas_dataframe(dataframe):
    """
    Returns a dataframe with a DatetimeIndex (period of 1 day) and a single
    column "therms". This is used during meter ingest and the dataframe input
    is assumed to be similar to an Item 17 file with the additional "therms" col

    :param dataframe: the Item 17-like dataframe to extract therms from
    """
    if "therms" not in dataframe.columns:
        return None

    df = dataframe[["DATE", "therms"]].copy()
    df.set_index("DATE", inplace=True)
    df.index.rename("index", inplace=True)
    df.index = pd.to_datetime(df.index)
    df.sort_index(inplace=True)
    df.therms = pd.to_numeric(df.therms)
    return df
