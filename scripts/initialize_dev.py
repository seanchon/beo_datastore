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
        "-f",
        "--full",
        action="store_true",
        dest="full",
        help="recreate database",
    )
    parser.add_argument(
        "-t",
        "--test-data",
        action="store_true",
        dest="test_data",
        help="import test data",
    )
    parser.add_argument(
        "-d",
        "--demo-data",
        action="store_true",
        dest="demo_data",
        help="import demo data",
    )
    return parser.parse_args()


if __name__ == "__main__":

    args = parse_arguments()

    # reset database
    if args.full:
        call_command("reset_db", "-c")
    else:
        call_command("flush")

    # apply migrations
    call_command("migrate")

    # load seed data
    if args.test_data:
        call_command(
            "runscript",
            "beo_datastore.scripts.load_data",
            "--script-args",
            "test",
        )
    elif args.demo_data:
        call_command(
            "runscript",
            "beo_datastore.scripts.load_data",
            "--script-args",
            "demo",
        )
    else:
        call_command("runscript", "beo_datastore.scripts.load_data")
