"""
Script to download a year of CAISO rate data.

Inputs:
  - year: the year of data to download

Outputs:
  - Newly created CAISOReport and CAISORate objects
"""
from datetime import timedelta
from cost.procurement.models import CAISOReport, CAISORate


def run(*args):
    """
    Usage:
        - python manage.py runscript cost.procurement.scripts.ingest_caiso_rate --script-args YEAR
    """
    if len(args) != 1:
        print(
            "USAGE `python manage.py runscript "
            "cost.procurement.scripts.ingest_caiso_rate "
            "--script-args YEAR`"
        )
        return

    try:
        year = int(args[0])
    except (TypeError, ValueError) as e:
        print(e)
        return

    print("Retrieving CAISO report for year {}...".format(year))
    caiso_report, _ = CAISOReport.get_or_create(
        report_name="PRC_LMP",
        year=year,
        query_params={
            "node": "TH_NP15_GEN-APND",
            "market_run_id": "DAM",
            "version": 1,
        },
        chunk_size=timedelta(days=15),
        max_attempts=10,
        destination_directory="/tmp",
    )
    CAISORate.objects.create(
        filters={"DATA_ITEM": "LMP_PRC"}, caiso_report=caiso_report
    )
