"""
15-minute interval data must include the following columns.
SA_ID,DIR,DATE,RS,0:15,0:30,0:45,1:00,1:15,1:30,1:45,2:00,2:15,2:30,2:45,3:00,3:15,3:30,3:45,4:00,4:15,4:30,4:45,5:00,5:15,5:30,5:45,6:00,6:15,6:30,6:45,7:00,7:15,7:30,7:45,8:00,8:15,8:30,8:45,9:00,9:15,9:30,9:45,10:00,10:15,10:30,10:45,11:00,11:15,11:30,11:45,12:00,12:15,12:30,12:45,13:00,13:15,13:30,13:45,14:00,14:15,14:30,14:45,15:00,15:15,15:30,15:45,16:00,16:15,16:30,16:45,17:00,17:15,17:30,17:45,18:00,18:15,18:30,18:45,19:00,19:15,19:30,19:45,20:00,20:15,20:30,20:45,21:00,21:15,21:30,21:45,22:00,22:15,22:30,22:45,23:00,23:15,23:30,23:45,0:00

60-minute interval data must include the following columns.
SA_ID,DIR,DATE,RS,1:00,2:00,3:00,4:00,5:00,6:00,7:00,8:00,9:00,10:00,11:00,12:00,13:00,14:00,15:00,16:00,17:00,18:00,19:00,20:00,21:00,22:00,23:00,0:00
"""


from datetime import timedelta
import pandas as pd
import re

from django.db import transaction

from beo_datastore.libs.dataframe import get_dataframe_period

from load.customer.models import Meter, Channel, ChannelIntervalFrame
from reference.reference_model.models import DataUnit


# BEO source file located at https://tvrp.app.box.com/file/420277168014


def get_sa_column(dataframe):
    sa_columns = [x for x in dataframe.columns if "SA" in x]
    if len(sa_columns) != 1:
        raise ValueError("A unique SA ID column not found.")
    return sa_columns[0]


def filter_dataframe(dataframe, sa_column, said, export, column_list=[]):
    if export:
        dataframe = dataframe[dataframe["DIR"] == "R"]
    else:
        dataframe = dataframe[dataframe["DIR"] == "D"]

    return dataframe[dataframe[sa_column] == said][column_list]


def reformat_timestamp_columns(dataframe):
    """
    Reformat timestamp column to 24-hour timestamp (ex. H_0_15 to 0:15).
    """
    # TODO: subtrack 15-minutes. hour 24 should be represented as 0.
    return dataframe.rename(
        columns=lambda x: x.replace("H_", "").replace("_", "")
    )


def get_timestamp_columns(dataframe):
    return [x for x in dataframe.columns if re.search(r"\d", x)]


def get_dataframe_saids(dataframe, sa_column):
    return set(dataframe[sa_column])


def get_rate_plan(dataframe, sa_column, said):
    rate_plan = dataframe[dataframe[sa_column] == said]["RS"].iloc[0]
    if rate_plan != rate_plan:  # check for nan
        rate_plan = None

    return rate_plan


def run(*args):
    """
    Usage:
        - python manage.py runscript load.customer.scripts.ingest_pge_data --script-args CSV_FILE
    """
    if len(args) != 1:
        print(
            "USAGE `python manage.py runscript "
            "load.customer.scripts.ingest_pge_data "
            "--script-args CSV_FILE`"
        )
        return
    dataframe = pd.read_csv(open(args[0], "rb"))
    dataframe = reformat_timestamp_columns(dataframe)
    sa_column = get_sa_column(dataframe)

    saids = get_dataframe_saids(dataframe, sa_column)
    timestamp_columns = get_timestamp_columns(dataframe)
    timestamp_columns = [timestamp_columns[-1]] + timestamp_columns[:-1]
    columns = ["DATE"] + timestamp_columns

    for said in saids:
        rate_plan = get_rate_plan(dataframe, sa_column, said)

        with transaction.atomic():
            if Meter.objects.filter(sa_id=said):
                continue
            meter, _ = Meter.objects.get_or_create(
                sa_id=said, rate_plan=rate_plan, state="CA"
            )
            try:
                for export in [True, False]:
                    df = filter_dataframe(
                        dataframe, sa_column, said, export, columns
                    )
                    df.set_index("DATE", inplace=True)
                    df.sort_index(inplace=True)

                    # transform to single column of values
                    df = df.stack().reset_index()
                    df["DATE"] = df["DATE"].astype(str)
                    df["level_1"] = df["level_1"].astype(str)
                    df["index"] = df["DATE"] + " " + df["level_1"]
                    df.drop(["DATE", "level_1"], axis=1, inplace=True)
                    df.rename(index=str, columns={0: "kw"}, inplace=True)
                    df.set_index("index", inplace=True)
                    df.index = pd.to_datetime(
                        df.index, format="%m/%d/%Y %H:%M"
                    )
                    df.sort_index(inplace=True)
                    df = df.loc[~df.index.duplicated(keep="first")]

                    if export:
                        df["kw"] = df["kw"] * -1.0

                    # convert 15-minute kwh to kw
                    if get_dataframe_period(df) == timedelta(0, 900):
                        df["kw"] = df["kw"] * 4.0

                    Channel.create(
                        export=export,
                        data_unit=DataUnit.objects.get(name="kw"),
                        meter=meter,
                        dataframe=df,
                    )
                print("meter: {}".format(said))
            except Exception as e:
                print(e)
                print("Skipping Import: {}".format(said))
