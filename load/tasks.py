from django.db import transaction

from beo_datastore.celery import app

from load.customer.models import Meter
from reference.reference_model.models import OriginFile


@app.task
def ingest_meters_from_file(origin_file_id):
    """
    Ingest meters associated with origin file.

    :param origin_file_id: OriginFile id
    """
    origin_file = OriginFile.objects.get(id=origin_file_id)
    for sa_id, meter_data in origin_file.meter_data_dict.items():
        with transaction.atomic():
            meter, _ = Meter.objects.get_or_create(
                sa_id=sa_id,
                rate_plan_name=meter_data["rate_plan_name"],
                origin_file=OriginFile(id=origin_file_id),
            )
            for (export, dataframe) in [
                (False, meter_data["import"]),
                (True, meter_data["export"]),
            ]:
                meter.get_or_create_channel(export, dataframe)
