from datetime import datetime, timedelta
from celery.utils.log import get_task_logger

from django.contrib.auth.models import User
from django.core.mail import send_mail
from django.db.models import Count
from django.utils.timezone import now

from beo_datastore.celery import app
from beo_datastore.libs.ingest import reformat_item_17
from beo_datastore.libs.utils import chunks
from beo_datastore.settings import ADMINS, APP_URL

from load.customer.models import CustomerMeter, CustomerPopulation, OriginFile
from reference.reference_model.models import MeterGroup


logger = get_task_logger(__name__)


@app.task(soft_time_limit=1800, max_retries=3)
def ingest_origin_file_meters(origin_file_id, chunk_size=5, overwrite=False):
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
    # retrieve sa_ids to ingest
    origin_file = OriginFile.objects.get(id=origin_file_id)

    # set locked_unlocked_at for rerun_incomplete_origin_file_ingests() check
    origin_file.locked_unlocked_at = now()
    origin_file.save()

    # recreate file database
    if not origin_file.db_exists or overwrite:
        ingest_origin_file(origin_file_id)

    if not overwrite:
        existing_sa_ids = origin_file.meters.values_list(
            "customermeter__sa_id", flat=True
        )
        existing_sa_ids = set([str(x) for x in existing_sa_ids])
        sa_ids = set(origin_file.db_get_sa_ids()) - existing_sa_ids
    else:
        sa_ids = set(origin_file.db_get_sa_ids())

    # aggregate meter
    aggregate_meter_group_intervalframes.delay(origin_file_id)

    # ingest meters
    for sa_ids in chunks(list(sa_ids), chunk_size):
        ingest_meters.delay(origin_file.id, sa_ids, overwrite)

    if origin_file.has_completed:
        origin_file.mark_complete()


@app.task
def ingest_origin_file(origin_file_id):
    """
    Ingest OriginFile.file into PostgreSQL.

    :param origin_file_id: OriginFile id
    """
    origin_file = OriginFile.objects.get(id=origin_file_id)
    origin_file.db_drop()
    origin_file.db_create()
    origin_file.db_create_tables()
    origin_file.db_load_intervals()
    origin_file.db_create_indexes()

    # store number of unique SA IDs
    origin_file.expected_meter_count = len(origin_file.db_get_sa_ids())
    origin_file.save()


@app.task(soft_time_limit=1800, max_retries=3)
def ingest_meters(origin_file_id, sa_ids, overwrite=False):
    """
    Ingest meter from OriginFile based on SA IDs.

    :param origin_file_id: OriginFile id
    :param sa_ids: list of SA IDs
    :param overwrite: If True, overwrite existing Meter values. If false,
        ignore when there is an existing meter.
    """
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
        try:
            sa_id_df = meter_df[
                meter_df[origin_file.db_sa_id_column.strip('"')] == sa_id
            ]
            if len(set(sa_id_df["RS"])) != 1:
                multiple_rate_plans = True
            else:
                multiple_rate_plans = False
            rate_plan_name = sa_id_df["RS"].iloc[-1]  # get most-recent RS
            forward_df = reformat_item_17(sa_id_df[sa_id_df["DIR"] == "D"])
            reverse_df = reformat_item_17(sa_id_df[sa_id_df["DIR"] == "R"])
            CustomerMeter.get_or_create(
                origin_file=origin_file,
                sa_id=sa_id,
                rate_plan_name=rate_plan_name,
                multiple_rate_plans=multiple_rate_plans,
                forward_df=forward_df,
                reverse_df=reverse_df,
            )
        except Exception as e:
            # Log failed meter ingests, but continue processing other meters.
            logger.exception(e)

    if origin_file.has_completed:
        origin_file.mark_complete()


@app.task(soft_time_limit=1800, max_retries=3)
def aggregate_meter_group_intervalframes(
    meter_group_id, in_db=True, overwrite=False
):
    """
    Aggregate all Meter data associated with a MeterGroup.

    :param meter_group_id: MeterGroup id
    :param in_db: If True, attempt in database aggregation.
    """
    locked = MeterGroup.is_locked(meter_group_id)
    completed = MeterGroup.is_completed(meter_group_id)

    if locked or (completed and not overwrite):
        # do nothing if job is running or completed
        return

    meter_group = MeterGroup.objects.get(id=meter_group_id)

    if in_db and isinstance(meter_group, OriginFile) and meter_group.db_exists:
        meter_group.db_aggregate_meter_intervalframes()
    else:
        meter_group.aggregate_meter_intervalframe()
    meter_group.build_aggregate_metrics()

    if meter_group.has_completed:
        meter_group.mark_complete()


@app.task()
def rerun_incomplete_origin_file_ingests(older_than_minutes: int):
    """
    Scans for and re-runs incomplete OriginFile ingests where:
        - meter_group.completed is False
        - meter_group.locked_unlocked_at timestamp is older_than_minutes, which
          specifies that the meter_group has not ingested in older_than_minutes.

    This is meant as a celery periodic task:
        - see: APP_URL/admin/django_celery_beat/periodictask/

    Email is sent to application ADMINS noting which ingests have been re-run.
    """
    incomplete_origin_files = OriginFile.objects.filter(
        completed=False,
        locked_unlocked_at__lte=(
            now() - timedelta(minutes=older_than_minutes)
        ),
    )
    incomplete_ids = incomplete_origin_files.values_list("id", flat=True)

    for origin_file in incomplete_origin_files:
        if origin_file.meters.count() > origin_file.expected_meter_count:
            # TODO: delete this call after all duplicates are gone
            deduplicate_meters.delay(origin_file.id)
        ingest_origin_file_meters.delay(origin_file.id)

    if incomplete_ids:
        send_mail(
            subject="OriginFile ingests re-run on {}".format(APP_URL),
            message="The following OriginFiles were re-ingested:\n{}".format(
                "\n".join([str(x) for x in incomplete_ids])
            ),
            from_email=None,
            recipient_list=[x[1] for x in ADMINS],
            fail_silently=False,
        )


@app.task
def deduplicate_meters(origin_file_id):
    """
    Addresses an older ingest bug that in some cases ingested the same meter
    twice. The root cause has to do with the mishandling multiple "RS" values
    in the original file.

    https://github.com/TerraVerdeRenewablePartners/beo_datastore/blob/66b17fb7a47d04cb517635ae31376d4ee5beee83/load/tasks.py#L106
    """
    # TODO: delete this task after all duplicates are gone

    origin_file = OriginFile.objects.get(id=origin_file_id)

    duplicate_ids = (
        origin_file.meters.instance_of(CustomerMeter)
        .values("customermeter__sa_id")
        .annotate(total=Count("customermeter__sa_id"))
        .order_by("total")
        .filter(total__gt=1)
        .values_list("customermeter__sa_id", flat=True)
    )

    for duplicate_id in duplicate_ids:
        # keep only most-recent meter
        duplicate_meters = origin_file.meters.filter(
            customermeter__sa_id=duplicate_id
        ).order_by("-created_at")[1:]
        origin_file.meters.remove(*duplicate_meters)


@app.task(soft_time_limit=180)
def delete_old_origin_file_databases(days=30):
    """
    Delete OriginFile databases older than number of "days" and including all
    orphaned OriginFile databases.

    :param days: integer
    """
    OriginFile.db_bulk_delete_origin_file_dbs(
        older_than=(datetime.utcnow() - timedelta(days=days))
    )


@app.task(soft_time_limit=180)
def create_clusters(customer_population_id, owner_id):
    """
    Create CustomerCluster objects using k-means clustering.
    """
    customer_population = CustomerPopulation.objects.get(
        id=customer_population_id
    )
    owner = User.objects.get(id=owner_id)

    customer_population.generate(owner)

    for cluster in customer_population.customer_clusters.all():
        aggregate_meter_group_intervalframes.delay(cluster.id)
