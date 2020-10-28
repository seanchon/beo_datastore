import argparse
import django
import os
import sys

from django.core.management import call_command

# set up Django environment
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "beo_datastore.settings")
django.setup()


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
        "--reset", action="store_true", dest="reset", help="recreate database",
    )
    parser.add_argument(
        "--flush",
        action="store_true",
        dest="flush",
        help="empties but does not delete database",
    )
    parser.add_argument(
        "--seed", action="store_true", dest="seed", help="seed database",
    )
    parser.add_argument(
        "--openei",
        action="store_true",
        dest="open_ei",
        help="import OpenEI data",
    )
    return parser.parse_args()


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
        call_command(
            "runscript",
            "beo_datastore.scripts.load_data",
            "--script-args",
            "seed",
        )
    # Loads OpenEI data
    elif args.open_ei:
        call_command(
            "runscript",
            "beo_datastore.scripts.load_data",
            "--script-args",
            "openei",
        )
    # Loads basic data
    else:
        call_command("runscript", "beo_datastore.scripts.load_data")
