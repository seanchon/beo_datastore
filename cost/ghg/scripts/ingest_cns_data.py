from datetime import date
import numpy as np
import os
import pandas as pd
import re

from beo_datastore.settings import BASE_DIR

from cost.ghg.models import CleanNetShort, CleanNetShortLookupTable


def get_year_from_filename(filename):
    year_str = re.findall("\d{4}", filename)[0]
    return int(year_str)


def run():
    """
    Usage:
        - python manage.py runscript cost.ghg.scripts.ingest_cns_data
    """
    data_dir = "cost/ghg/scripts/data/"
    for csv_file in [
        "cns_2018.csv",
        "cns_2022.csv",
        "cns_2026.csv",
        "cns_2030.csv",
    ]:
        path = os.path.join(BASE_DIR, data_dir, csv_file)
        dataframe = pd.read_csv(path, index_col=0)
        dataframe.columns = dataframe.columns.astype(np.int64)

        # convert from hours 1 through 24 to 0 through 23
        dataframe.index = dataframe.index - 1

        # convert from tCO2/MWh to tCO2/kWh
        dataframe = dataframe / 1000

        cns, _ = CleanNetShort.objects.get_or_create(
            effective=date(get_year_from_filename(csv_file), 1, 1)
        )
        cns.lookup_table = CleanNetShortLookupTable(
            reference_object=cns, dataframe=dataframe
        )
        cns.save()
