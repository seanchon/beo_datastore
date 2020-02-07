from django.core.management import call_command

from beo_datastore.libs.fixtures import (
    load_base_fixtures,
    load_test_fixtures,
    load_intervalframe_files,
)


def run(*args):
    """
    Usage:
        - python manage.py runscript beo_datastore.scripts.load_data
    """
    load_base_fixtures()
    if len(args) > 0:
        if args[0] == "test":
            load_test_fixtures()
        elif args[0] == "demo":
            load_test_fixtures()
            call_command(
                "runscript",
                "load.openei.scripts.ingest_reference_meters",
                "--script-args",
                "CA",
            )
            call_command(
                "runscript",
                "cost.utility_rate.scripts.ingest_openei_utility_rates",
                "--script-args",
                "Pacific Gas & Electric Co",
            )
    load_intervalframe_files()
