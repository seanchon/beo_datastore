"""
Script to ingest 8,760 data with the following timestamp columns:
    - Date
    - PST Year
    - PST Month
    - PST Day
    - PST Hour
"""


import pandas as pd

from django.db import transaction

from cost.procurement.models import SystemProfile
from reference.auth_user.models import LoadServingEntity


# BEO source file located at https://tvrp.app.box.com/file/503104443108


def run(*args):
    """
    Usage:
        - python manage.py runscript cost.procurement.scripts.ingest_system_profiles --script-args LSE_NAME CSV_FILE
    """
    if len(args) != 2:
        print(
            "USAGE `python manage.py runscript "
            "cost.procurement.scripts.ingest_system_profiles "
            "--script-args LSE_NAME CSV_FILE`"
        )
        return

    try:
        load_serving_entity = LoadServingEntity.objects.get(name=args[0])
    except LoadServingEntity.DoesNotExist:
        print(
            "If desired LSE does not exist, create LoadServingEntity or "
            "ingest rates first. Options for LSE are: \n"
            + LoadServingEntity.menu()
        )
        return

    dataframe = pd.read_csv(open(args[1], "rb"))
    dataframe["PST Hour"] = dataframe["PST Hour"] - 1  # 0 - 23 Convention
    dataframe["Datetime"] = pd.to_datetime(
        dataframe[["PST Day", "PST Month", "PST Year", "PST Hour"]]
        .astype(str)
        .apply(" ".join, 1)
        + (":00:00"),
        format="%d %m %Y %H:%M:%S",
    )
    dataframe.drop(
        columns=["Date", "PST Year", "PST Month", "PST Day", "PST Hour"],
        inplace=True,
    )

    with transaction.atomic():
        for column in [x for x in dataframe.columns if "kWh" in x]:
            print("Creating SystemProfile for {}".format(column))
            system_df = (
                dataframe[["Datetime", column]]
                .set_index("Datetime")
                .rename(columns={column: "kw"})
            )
            system_df["kw"] = pd.to_numeric(
                system_df["kw"].astype(str).str.strip().str.replace(",", "")
            )
            SystemProfile.get_or_create(
                name=column.replace(", kWh", "").strip(),
                load_serving_entity=load_serving_entity,
                dataframe=system_df,
            )
