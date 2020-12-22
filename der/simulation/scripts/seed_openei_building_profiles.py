from pathlib import Path

import pandas as pd

from beo_datastore.settings import BASE_DIR
from der.simulation.models import FuelSwitchingStrategy
from navigader_core.load.openei import TMY3Parser


def run():
    """
    Seed the application with public OpenEI hourly load profiles as FuelSwitchingStrategy objects

    Usage:
        - python manage.py runscript der.simulation.scripts.seed_openei_building_profiles
    """

    data_dir = Path(
        BASE_DIR + "/der/simulation/fixtures/openei_building_profiles"
    )
    csv_files = data_dir.glob("*.csv")

    for openei_file in csv_files:
        building_profile_name = openei_file.name
        dataframe = pd.read_csv(openei_file)
        errors, _ = TMY3Parser.validate(dataframe)
        if errors:
            print(f"Failed : {building_profile_name} -> {errors}")

        strategy, created = FuelSwitchingStrategy.get_or_create(
            name=building_profile_name,
            description="Preloaded by admin",
            load_serving_entity=None,  # Available to all CCAs.
            dataframe=dataframe,
        )
        if created:
            print(f"Created: {building_profile_name}")
        else:
            print(f"Existed: {building_profile_name}")
