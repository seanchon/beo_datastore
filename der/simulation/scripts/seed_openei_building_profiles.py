import csv
from pathlib import Path
import re

import pandas as pd

from beo_datastore.settings import BASE_DIR
from der.simulation.models import FuelSwitchingStrategy
from navigader_core.load.openei import TMY3Parser


def get_strategy_description(building_profile_name: str, openei_file: Path):
    # Read file URL from first cell of the first row
    reader = csv.reader(open(openei_file))
    file_url = next(reader)[0]

    if "residential" in building_profile_name:
        match = re.match(r"(.*) \(residential", building_profile_name)
        if not match:
            print(
                f"Unable to determine city name of residential file: "
                f"{building_profile_name}"
            )
            return None

        city_name = match.group(1)
        return (
            "Reference load data for an average residential customer in "
            f"{city_name}. Source file can be found here: {file_url}"
        )
    else:
        match = re.match(r"(.*) in (.*) \(New 2004", building_profile_name)
        if not match:
            print(
                f"Unable to determine city name/building type of "
                f"commercial file: {building_profile_name}"
            )
            return None

        building_type = match.group(1)
        city_name = match.group(2)
        return (
            f"Reference load data for an average {building_type} in "
            f"{city_name}. Source file can be found here: {file_url}"
        )


def run():
    """
    Seed the application with public OpenEI hourly load profiles as
    FuelSwitchingStrategy objects

    Usage:
        - python manage.py runscript der.simulation.scripts.seed_openei_building_profiles
    """

    data_dir = Path(
        BASE_DIR + "/der/simulation/fixtures/openei_building_profiles"
    )
    csv_files = data_dir.glob("*.csv")

    for openei_file in csv_files:
        building_profile_name = openei_file.name.replace("_", " ")
        dataframe = pd.read_csv(openei_file, skiprows=1)
        errors, _ = TMY3Parser.validate(dataframe)
        if errors:
            print(f"Failed : {building_profile_name} -> {errors}")
            continue

        strategy, created = FuelSwitchingStrategy.get_or_create(
            name=building_profile_name,
            description=get_strategy_description(
                building_profile_name=building_profile_name,
                openei_file=openei_file,
            ),
            load_serving_entity=None,  # Available to all CCAs.
            dataframe=dataframe,
        )
        if created:
            print(f"Created: {building_profile_name}")
        else:
            print(f"Found existing strategy: {building_profile_name}")
