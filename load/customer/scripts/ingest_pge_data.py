from datetime import time
import pandas as pd

from load.customer.models import Meter, Channel, ChannelIntervalFrame
from reference.reference_model.models import DataUnit


# BEO source file located at https://tvrp.app.box.com/file/420277168014


def filter_dataframe(dataframe, said, export, column_list=[]):
    if export:
        dataframe = dataframe[dataframe["DIR"] == "R"]
    else:
        dataframe = dataframe[dataframe["DIR"] == "D"]

    return dataframe[dataframe["Anon SA_ID"] == said][column_list]


def get_timestamp_columns(dataframe):
    return sorted([x for x in dataframe.columns if isinstance(x, time)])


def get_dataframe_saids(dataframe):
    return set(dataframe["Anon SA_ID"])


def get_rate_plan(dataframe, said):
    rate_plan = dataframe[dataframe["Anon SA_ID"] == said]["RS"].iloc[0]
    if rate_plan != rate_plan:  # check for nan
        rate_plan = None

    return rate_plan


def run(*args):
    """
    Usage:
        - python manage.py runscript load.customer.scripts.ingest_pge_data --script-args EXCEL_FILE SHEET_NAME
    """
    if len(args) != 2:
        print(
            "USAGE `python manage.py runscript "
            "load.customer.scripts.ingest_pge_data "
            "--script-args EXCEL_FILE SHEET_NAME`"
        )
        return
    dataframe = pd.read_excel(open(args[0], "rb"), sheet_name=args[1])

    saids = get_dataframe_saids(dataframe)
    timestamp_columns = get_timestamp_columns(dataframe)
    columns = ["DATE"] + timestamp_columns

    for said in saids:
        rate_plan = get_rate_plan(dataframe, said)

        meter, _ = Meter.objects.get_or_create(
            sa_id=said, rate_plan=rate_plan, state="CA"
        )
        for export in [True, False]:
            df = filter_dataframe(dataframe, said, export, columns)
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
            df.index = pd.to_datetime(df.index)

            if export:
                df["kw"] = df["kw"] * -1.0

            # convert 15-minute kwh to kw
            if "15-minute" in args[1]:
                df["kw"] = df["kw"] * 4.0

            channel, _ = Channel.objects.get_or_create(
                export=export,
                data_unit=DataUnit.objects.get(name="kw"),
                meter=meter,
            )
            channel.intervalframe = ChannelIntervalFrame(channel, df)
            channel.save()
