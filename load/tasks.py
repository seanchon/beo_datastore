from functools import reduce

from django.db import transaction

from beo_datastore.celery import app
from beo_datastore.libs.ingest import reformat_item_17
from beo_datastore.libs.intervalframe import ValidationIntervalFrame
from beo_datastore.libs.utils import chunks

from load.customer.models import CustomerMeter, OriginFile
from reference.reference_model.models import MeterGroup


@app.task
def ingest_origin_file_meters(origin_file_id, chunk_size=10, overwrite=False):
    """
    Performs all necessary ingest steps after an OriginFile has been created.

    1. Ingest OriginFile.file into PostgreSQL.
    2. Ingest individual meters.
    3. Aggregate all meter load into OriginFile.intervalframe.

    :param origin_file_id: OriginFile id
    :param chunk_size: number of meters to process per call to ingest_meters
        (This is helpful in production.)
    :param overwrite: If True, overwrite existing Meter values. If false,
        ignore when there is an existing meter.
    """
    # recreate file database
    if overwrite:
        ingest_origin_file(origin_file_id)

    # retrieve sa_ids to ingest
    origin_file = OriginFile.objects.get(id=origin_file_id)
    if not overwrite:
        existing_sa_ids = origin_file.meters.values_list(
            "customermeter__sa_id", flat=True
        )
        existing_sa_ids = set([str(x) for x in existing_sa_ids])
        sa_ids = set(origin_file.db_get_sa_ids()) - existing_sa_ids
    else:
        sa_ids = set(origin_file.db_get_sa_ids())

    # ingest meters
    for sa_ids in chunks(list(sa_ids), chunk_size):
        ingest_meters.delay(origin_file.id, sa_ids, overwrite)

    # aggregate meter
    # TODO: only run after all meters are ingested
    aggregate_meter_group_intervalframes.delay(origin_file_id)


@app.task
def ingest_origin_file(origin_file_id):
    """
    Ingest OriginFile.file into PostgreSQL.

    :param origin_file_id: OriginFile id
    """
    with transaction.atomic():
        origin_file = OriginFile.objects.get(id=origin_file_id)
        if origin_file.db_exists:
            origin_file.db_drop()
        origin_file.db_create()
        origin_file.db_create_tables()
        origin_file.db_load_intervals()
        origin_file.db_create_indexes()


@app.task
def ingest_meters(origin_file_id, sa_ids, overwrite=False):
    """
    Ingest meter from OriginFile based on SA IDs.

    :param origin_file_id: OriginFile id
    :param sa_ids: list of SA IDs
    :param overwrite: If True, overwrite existing Meter values. If false,
        ignore when there is an existing meter.
    """
    with transaction.atomic():
        # retrieve sa_ids to ingest
        origin_file = OriginFile.objects.get(id=origin_file_id)
        if not overwrite:
            existing_sa_ids = origin_file.meters.values_list(
                "customermeter__sa_id", flat=True
            )
            existing_sa_ids = set([str(x) for x in existing_sa_ids])
            sa_ids = set(sa_ids) - existing_sa_ids

        # ingest meters
        meter_df = origin_file.db_get_meter_dataframe(sa_ids)
        for sa_id in sa_ids:
            sa_id_df = meter_df[
                meter_df[origin_file.db_sa_id_column.strip('"')] == sa_id
            ]
            if len(set(sa_id_df["RS"])) != 1:
                raise LookupError("RS (rate schedule) should be unique.")
            rate_plan_name = set(sa_id_df["RS"]).pop()
            forward_df = reformat_item_17(sa_id_df[sa_id_df["DIR"] == "D"])
            reverse_df = reformat_item_17(sa_id_df[sa_id_df["DIR"] == "R"])
            CustomerMeter.get_or_create(
                origin_file=origin_file,
                sa_id=sa_id,
                rate_plan_name=rate_plan_name,
                forward_df=forward_df,
                reverse_df=reverse_df,
            )


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
