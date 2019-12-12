from load.customer.models import Meter
from reference.reference_model.models import LoadServingEntity, OriginFile


def run(*args):
    """
    Usage:
        - python manage.py runscript load.customer.scripts.ingest_item_17 --script-args LSE_NAME CSV_FILE
    """
    if len(args) != 2:
        print(
            "USAGE `python manage.py runscript "
            "load.customer.scripts.ingest_item_17 "
            "--script-args LSE_NAME CSV_FILE`"
        )
        return

    try:
        load_serving_entity = LoadServingEntity.objects.get(name=args[0])
    except LoadServingEntity.DoesNotExist:
        print(
            "If desired LSE does not exist, create LoadServingEntity or "
            "ingest rates first. Options for LSE are: \n"
            + LoadServingEntity.menu()
        )
        return

    origin_file, _ = OriginFile.get_or_create(file_path=args[1])
    Meter.ingest_meters(origin_file, "PG&E", load_serving_entity)
