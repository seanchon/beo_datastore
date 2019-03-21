from datetime import time
import pandas as pd

from interval.models import ServiceDrop, Meter, MeterIntervalFrame
from reference.reference_unit.models import DataUnit


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


def run(*args):
    if len(args) != 2:
        print(
            "USAGE `python manage.py runscript "
            "interval.scripts.ingest_mce_data --script-args FILE SHEET_NAME`"
        )
        return
    dataframe = pd.read_excel(open(args[0], "rb"), sheet_name=args[1])

    saids = get_dataframe_saids(dataframe)
    timestamp_columns = get_timestamp_columns(dataframe)
    columns = ["DATE"] + timestamp_columns

    for said in saids:
        frame = filter_dataframe(dataframe, said, "R", "RS")
        rate_plan = frame[frame.index[0]]
        service_drop, _ = ServiceDrop.objects.get_or_create(
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
            df.rename(index=str, columns={0: "value"}, inplace=True)
            df.set_index("index", inplace=True)
            df.index = pd.to_datetime(df.index)

            if export:
                df["value"] = df["value"] * -1.0

            meter, _ = Meter.objects.get_or_create(
                export=export,
                data_unit=DataUnit.objects.get(name='kwh'),
                service_drop=service_drop,
            )
            meter.intervalframe = MeterIntervalFrame(meter, df)
            meter.save()
