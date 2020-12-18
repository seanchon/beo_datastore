import re
from datetime import date
from pathlib import Path

import numpy as np

from beo_datastore.libs.dataframe import read_csv
from beo_datastore.settings import BASE_DIR
from cost.ghg.models import GHGRate
from reference.reference_model.models import RateUnit


CSP_SOURCE_URL = "https://www.cpuc.ca.gov/General.aspx?id=6442459770"


def get_year_from_filename(path):
    return int(re.findall(r"\d{4}", path.name)[0])


def update_or_create_ghg_rates(csv_path, year):
    dataframe = read_csv(csv_path, index_col=0)
    dataframe.columns = dataframe.columns.astype(np.int64)

    # convert from hours 1 through 24 to 0 through 23
    dataframe.index = dataframe.index - 1

    # convert from tCO2/MWh to tCO2/kWh
    dataframe = dataframe / 1000

    name = f"Clean System Power {year}"
    effective = date(year, 1, 1)
    ghg_rate = GHGRate.objects.filter(
        effective=effective, name__icontains="clean net short"
    ).first()

    if ghg_rate:
        ghg_rate.name = name
        ghg_rate.frame.dataframe = dataframe
        ghg_rate.source = CSP_SOURCE_URL
        ghg_rate.save()
        print(f"Updated: {ghg_rate}")
    else:
        ghg_rate, created = GHGRate.get_or_create(
            name=name,
            effective=effective,
            source=CSP_SOURCE_URL,
            rate_unit=RateUnit.objects.get(
                numerator__name="tCO2", denominator__name="kwh"
            ),
            dataframe=dataframe,
        )
        if created:
            print(f"Created: {ghg_rate}")
        else:
            print(f"Already existed: {ghg_rate}")


def run():
    """
    Ingest Clean System Power GHG rates from 288 CSV files.
    - GHGRate instances are uniquely identified by effective date 20XX-01-01
    - Existing "Clean Net Short" GHGRates will have their dataframes updated
      with CSP data of the same year. If no CNS rate is found for a given
      year, a new GHGRate instance will be created

    Usage:
        - python manage.py runscript cost.ghg.scripts.ingest_ghg_data
    """

    data_dir = Path(BASE_DIR + "/cost/ghg/scripts/csp_data/")
    csv_files = data_dir.glob("*.csv")
    for csv_file in csv_files:
        update_or_create_ghg_rates(
            csv_path=csv_file,
            year=get_year_from_filename(csv_file),
        )
