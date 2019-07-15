from datetime import date
import numpy as np
import os
import pandas as pd
import re

from beo_datastore.settings import BASE_DIR

from cost.ghg.models import GHGRate
from reference.reference_model.models import RateUnit


def get_year_from_filename(filename):
    year_str = re.findall("\d{4}", filename)[0]
    return int(year_str)


def create_cns_object(csv_path, year):
    dataframe = pd.read_csv(csv_path, index_col=0)
    dataframe.columns = dataframe.columns.astype(np.int64)

    # convert from hours 1 through 24 to 0 through 23
    dataframe.index = dataframe.index - 1

    # convert from tCO2/MWh to tCO2/kWh
    dataframe = dataframe / 1000

    name = "Clean Net Short"
    effective = date(year, 1, 1)
    if not GHGRate.objects.filter(name=name, effective=effective):
        GHGRate.create(
            name=name,
            effective=effective,
            source="http://www.cpuc.ca.gov/General.aspx?id=6442451195",
            rate_unit=RateUnit.objects.get(
                numerator__name="tCO2", denominator__name="kwh"
            ),
            dataframe=dataframe,
        )


def run():
    """
    Usage:
        - python manage.py runscript cost.ghg.scripts.ingest_ghg_data
    """
    # Ingest Clean Net Short Data
    data_dir = "cost/ghg/scripts/data/"
    for csv_file in [
        "cns_2018.csv",
        "cns_2022.csv",
        "cns_2026.csv",
        "cns_2030.csv",
    ]:
        create_cns_object(
            csv_path=os.path.join(BASE_DIR, data_dir, csv_file),
            year=get_year_from_filename(csv_file),
        )

    # Ingest Natural Gas Constant
    dataframe = pd.DataFrame(
        0.000380, columns=np.array(range(1, 13)), index=np.array(range(0, 24))
    )
    name = "Natural Gas"
    effective = date(2015, 1, 1)
    if not GHGRate.objects.filter(name=name, effective=effective):
        GHGRate.create(
            name=name,
            effective=effective,
            source=(
                "https://www.mcecleanenergy.org/wp-content/uploads/2018/01/"
                "Understanding_MCE_GHG_EmissionFactors_2015.pdf"
            ),
            rate_unit=RateUnit.objects.get(
                numerator__name="tCO2", denominator__name="kwh"
            ),
            dataframe=dataframe,
        )
