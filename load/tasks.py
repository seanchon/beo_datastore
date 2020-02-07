from functools import reduce

from django.db import transaction

from beo_datastore.celery import app
from beo_datastore.libs.intervalframe import ValidationIntervalFrame

from load.customer.models import CustomerMeter, OriginFile
from reference.reference_model.models import MeterGroup


@app.task
def ingest_meters_from_file(origin_file_id):
    """
    Ingest meters associated with origin file.

    :param origin_file_id: OriginFile id
    """
    origin_file = OriginFile.objects.get(id=origin_file_id)
    for sa_id, meter_data in origin_file.meter_data_dict.items():
        with transaction.atomic():
            meter, _ = CustomerMeter.objects.get_or_create(
                sa_id=sa_id,
                rate_plan_name=meter_data["rate_plan_name"],
                load_serving_entity=origin_file.load_serving_entity,
            )
            meter.meter_groups.add(origin_file)
            for (export, dataframe) in [
                (False, meter_data["import"]),
                (True, meter_data["export"]),
            ]:
                meter.get_or_create_channel(export, dataframe)


@app.task
def aggregate_meter_group_intervalframes(meter_group_id):
    """
    Aggregate all Meter data associated with a MeterGroup.

    :param meter_group_id: MeterGroup id
    """
    meter_group = MeterGroup.objects.get(id=meter_group_id)

    meter_group.intervalframe = reduce(
        lambda x, y: x + y,
        [x.intervalframe for x in meter_group.meters.all()],
        ValidationIntervalFrame(
            dataframe=ValidationIntervalFrame.default_dataframe
        ),
    )
    meter_group.save()
