from django.db import transaction

from load.customer.models import OriginFile
from load.tasks import ingest_origin_file_meters
from reference.auth_user.models import LoadServingEntity


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

    with open(args[1], "rb") as file, transaction.atomic():
        origin_file, _ = OriginFile.get_or_create(
            load_serving_entity=load_serving_entity, file=file
        )
        ingest_origin_file_meters.delay(origin_file.id)
