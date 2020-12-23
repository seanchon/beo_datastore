import csv
import re
from pathlib import Path

import pandas as pd

from beo_datastore.settings import BASE_DIR
from der.simulation.models import (
    FuelSwitchingConfiguration,
    FuelSwitchingStrategy,
)
from navigader_core.load.openei import TMY3Parser

OPENEI_FILES_DIR = "/der/simulation/fixtures/openei_building_profiles"


def create_strategies():
    data_dir = Path(BASE_DIR + OPENEI_FILES_DIR)
    openei_csv_files = data_dir.glob("*.csv")
    print("\nFuel-Switching Strategies:")
    for openei_file in openei_csv_files:
        building_profile_name = openei_file.name
        dataframe = pd.read_csv(openei_file, skiprows=1)
        errors, _ = TMY3Parser.validate(dataframe)
        if errors:
            print(f"Failed : {building_profile_name} -> {errors}")
            continue

        name = building_profile_name.strip().replace("_", " ")
        name = name[0].title() + name[1:]
        strategy, created = FuelSwitchingStrategy.get_or_create(
            name=name,
            description=get_strategy_description(
                building_profile_name=name,
                openei_file=openei_file,
            ),
            load_serving_entity=None,
            dataframe=dataframe,
        )
        prompt(strategy, created)


def create_configurations():
    configurations = {
        "Heat Pump": {
            "space_heating": True,
            "water_heating": False,
        },
        "Heat Pump Water Heater": {
            "space_heating": False,
            "water_heating": True,
        },
        "Heat Pump and Heat Pump Water Heater": {
            "space_heating": True,
            "water_heating": True,
        },
    }

    print("\nFuel-Switching Configurations:")
    for name, options in configurations.items():
        (
            configuration,
            created,
        ) = FuelSwitchingConfiguration.objects.get_or_create(
            name=name,
            space_heating=options.get("space_heating"),
            water_heating=options.get("water_heating"),
            load_serving_entity=None,
        )
        prompt(configuration, created)


def prompt(obj, created):
    if created:
        print(f"Created new strategy:    {obj.name}")
    else:
        print(f"Found existing strategy: {obj.name}")


def get_strategy_description(building_profile_name: str, openei_file: Path):
    # Read file URL from first cell of the first row
    reader = csv.reader(open(openei_file))
    file_url = next(reader)[0]

    if "residential" in building_profile_name:
        match = re.match(r"(.*) \(residential", building_profile_name)
        if not match:
            return None

        city_name = match.group(1)
        return (
            "Reference load data for an average residential customer in "
            f"{city_name}. Source file can be found here: {file_url}"
        )
    else:
        match = re.match(r"(.*) in (.*) \(New 2004", building_profile_name)
        if not match:
            return None

        building_type = match.group(1)
        city_name = match.group(2)
        return (
            f"Reference load data for an average {building_type} in "
            f"{city_name}. Source file can be found here: {file_url}"
        )


def run():
    """
    Seed the application with Fuel Switching Configurations and Strategies.
    Notes:
    - There are only 3 possible configuration options.
    - A set of most relevant OpenEI hourly building load profiles will be
     ingested to create multiple strategy options.

    Usage:
        - python manage.py runscript der.simulation.scripts.seed_fuel_switching
    """
    create_configurations()
    create_strategies()
