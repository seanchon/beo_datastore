from datetime import datetime
import gzip
import json
import os
import urllib

from cost.utility_rate.models import RateCollection, RatePlan
from reference.reference_model.models import Sector, Utility, VoltageCategory


SOURCE_FILE_URL = "https://openei.org/apps/USURDB/download/usurdb.json.gz"


def retrieve_full_utility_rates():
    """
    Downloads and opens OpenEI Utility Rate Database and returns rates.

    Source: https://openei.org/wiki/Utility_Rate_Database
    :return: list of rates dictionaries
    """
    json_file = os.path.basename(SOURCE_FILE_URL)
    if not os.path.exists(json_file):
        urllib.request.urlretrieve(SOURCE_FILE_URL, json_file)

    with gzip.open(json_file, "rb") as f:
        return json.load(f)


def load_utility_rates(file_location):
    """
    Loads OpenEI formatted rates from file.

    :param file_location: file location
    :return: list of rates dictionaries
    """
    with open(file_location, "rb") as f:
        return json.load(f)


def convert_epoch_to_datetime(epoch_milliseconds):
    """
    Converts epoch time in milliseconds to datetime.
    """
    return datetime.fromtimestamp(epoch_milliseconds / 1000.0)


def run(*args):
    """
    Usage:
        - python manage.py runscript cost.utility_rate.scripts.ingest_openei_utility_rates --script-args UTILITY_NAME (SOURCE)
    """
    if len(args) < 1:
        print(
            "USAGE `python manage.py runscript "
            "cost.utility_rate.scripts.ingest_openei_utility_rates "
            "--script-args UTILITY_NAME (SOURCE)`"
        )
        print("Enter 'help' for UTILITY_NAME to get a list of utilities.")
        print("Example UTILITY_NAME: 'Pacific Gas & Electric Co'")
        print("Specify an optional SOURCE to use a local file.")
        return

    if len(args) == 2:
        source_file = args[1]
    else:
        source_file = None

    if source_file:
        full_utility_rates = load_utility_rates(args[1])
    else:
        full_utility_rates = retrieve_full_utility_rates()

    # return all possible utility names
    if args[0] == "help":
        print(
            "\n".join(
                sorted(
                    list(set([x["utilityName"] for x in full_utility_rates]))
                )
            )
        )
        return

    # filter rates based on provided utility and approved
    rates = [
        x
        for x in full_utility_rates
        if args[0] == x["utilityName"] and x["approved"] is True
    ]

    # ingest rates
    for rate_data in rates:
        utility, _ = Utility.objects.get_or_create(
            name=rate_data.get("utilityName", None)
        )
        sector, _ = Sector.objects.get_or_create(
            name=rate_data.get("sector", None), utility=utility
        )
        if rate_data.get("voltageCategory", None):
            voltage_category, _ = VoltageCategory.objects.get_or_create(
                name=rate_data.get("voltageCategory", None), utility=utility
            )
        else:
            voltage_category = None

        rate_plan, _ = RatePlan.objects.get_or_create(
            name=rate_data.get("rateName", None),
            description=rate_data.get("description", None),
            demand_min=rate_data.get("demandMin", None),
            demand_max=rate_data.get("demandMax", None),
            utility=utility,
            sector=sector,
            voltage_category=voltage_category,
        )

        effective_date_epoch = rate_data["effectiveDate"]["$date"]

        if source_file:
            openei_url = None
        else:
            openei_id = rate_data["_id"]["$oid"]
            openei_url = "https://openei.org/apps/USURDB/rate/view/{}".format(
                openei_id
            )

        RateCollection.objects.get_or_create(
            rate_data=rate_data,
            # Metadata
            openei_url=openei_url,
            utility_url=rate_data.get("sourceReference", None),
            effective_date=convert_epoch_to_datetime(effective_date_epoch),
            rate_plan=rate_plan,
        )
