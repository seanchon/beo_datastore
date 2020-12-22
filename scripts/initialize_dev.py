import argparse
import os
import sys

import django
from django.core.management import call_command

# set up Django environment
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "beo_datastore.settings")
django.setup()

from beo_datastore.libs.fixtures import (
    load_base_fixtures_and_intervalframes,
    load_all_fixtures_and_intervalframes,
)


def parse_arguments():
    parser = argparse.ArgumentParser(
        description=(
            """
            Initialize dev environment.

            1. stash local changes
            2. upgrade pip packages
            3. reset database
            4. apply migrations
            5. load seed data
            """
        )
    )
    parser.add_argument(
        "--reset", action="store_true", dest="reset", help="recreate database"
    )
    parser.add_argument(
        "--flush",
        action="store_true",
        dest="flush",
        help="empties but does not delete database",
    )
    parser.add_argument(
        "--seed", action="store_true", dest="seed", help="seed database"
    )
    parser.add_argument(
        "--openei",
        action="store_true",
        dest="open_ei",
        help="import OpenEI data",
    )
    parser.add_argument(
        "--openei-buildings",
        action="store_true",
        dest="open_ei_buildings",
        help="import OpenEI building profiles for fuel switching simulations",
    )
    parser.add_argument(
        "--lse",
        action="store_true",
        dest="add_load_serving_entities",
        help="ingest load service entities",
    )
    return parser.parse_args()


def load_open_ei_reference_meters(state: str = "CA") -> None:
    call_command(
        "runscript",
        "load.openei.scripts.ingest_reference_meters",
        "--script-args",
        state,
    )


def load_open_ei_utility_rates(
    utility_name: str = "Pacific Gas & Electric Co",
) -> None:
    call_command(
        "runscript",
        "cost.utility_rate.scripts.ingest_openei_utility_rates",
        "--script-args",
        utility_name,
    )


def load_open_ei_building_profiles() -> None:
    call_command(
        "runscript",
        "der.simulation.scripts.seed_openei_building_profiles",
    )


def add_load_serving_entities() -> None:
    call_command(
        "runscript",
        "scripts.ingest_load_serving_entities",
        "--silent",
    )


if __name__ == "__main__":
    args = parse_arguments()

    # Drops and recreates the database, closing any active database sessions
    # prior to doing so.
    if args.reset:
        call_command("reset_db", "--close-sessions")
    # Empties the database tables but does not drop the database. This will
    # keep the current migration status
    elif args.flush:
        call_command("flush")

    # Runs the migrations. If the database has not been reset this has no effect
    call_command("migrate")

    # Loads seed data
    if args.seed:
        load_all_fixtures_and_intervalframes()
    # Loads OpenEI data
    if args.open_ei:
        load_base_fixtures_and_intervalframes()
        load_open_ei_reference_meters()
        load_open_ei_utility_rates()
    # LoadOpenEI Building Profiles
    if args.open_ei_buildings:
        load_open_ei_building_profiles()
    # Loads basic data
    if not args.seed and not args.open_ei:
        load_base_fixtures_and_intervalframes()
    if args.add_load_serving_entities:
        add_load_serving_entities()
