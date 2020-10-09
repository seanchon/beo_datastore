from datetime import timedelta
from pytz import timezone

from celery.utils.log import get_task_logger

from django.core.mail import send_mail
from django.utils.timezone import now

from beo_datastore.celery import app
from beo_datastore.settings import ADMINS, APP_URL

from cost.procurement.models import CAISOReport
from cost.study.models import Scenario
from reference.reference_model.models import Meter


logger = get_task_logger(__name__)


@app.task(soft_time_limit=1800, max_retries=3)
def run_scenario(scenario_id):
    """
    Perform a run() operation on a Scenario.
    """
    scenario = Scenario.objects.get(id=scenario_id)

    # set locked_unlocked_at for rerun_incomplete_scenarios() check
    scenario.locked_unlocked_at = now()
    scenario.save()

    for meter in scenario.meters.all():
        run_simulation_and_cost.delay(
            scenario_id=scenario.id, meter_id=meter.id
        )


@app.task(soft_time_limit=120, max_retries=3)
def run_simulation_and_cost(scenario_id, meter_id):
    """
    Run a single Meter's DERSimulation and cost calculations with a Scenario.
    """
    scenario = Scenario.objects.get(id=scenario_id)
    scenario.run_single_meter_simulation_and_cost(
        meter=Meter.objects.get(id=meter_id)
    )

    if (
        not Scenario.is_locked(scenario_id)
        and scenario.simulations_completed
        and scenario.cost_calculations_completed
    ):
        generate_intervalframe_and_reports.delay(scenario.id)


@app.task(soft_time_limit=1800, max_retries=3)
def generate_intervalframe_and_reports(scenario_id, overwrite=False):
    """
    Store post_der_intervalframe as meter_intervalframe

    :param scenario_id: Scenario id
    :param force: force recomputation
    """
    locked = Scenario.is_locked(scenario_id)
    completed = Scenario.is_completed(scenario_id)

    if locked or (completed and not overwrite):
        # do nothing if job is running or completed
        return

    scenario = Scenario.objects.get(id=scenario_id)
    scenario.aggregate_meter_intervalframe()
    scenario.generate_reports()

    if scenario.has_completed:
        scenario.mark_complete()


@app.task()
def rerun_incomplete_scenarios(older_than_minutes: int):
    """
    Scans for and re-runs incomplete scenarios where:
        - scenario.completed is False
        - scenario.locked_unlocked_at timestamp is older_than_minutes, which
          specifies that the scenario has not completed in older_than_minutes.

    This is meant as a celery periodic task:
        - see: APP_URL/admin/django_celery_beat/periodictask/

    Email is sent to application ADMINS noting which scenario have been re-run.
    """
    incomplete_scenarios = Scenario.objects.filter(
        completed=False,
        locked_unlocked_at__lte=(
            now() - timedelta(minutes=older_than_minutes)
        ),
    )
    incomplete_ids = incomplete_scenarios.values_list("id", flat=True)

    for scenario in incomplete_scenarios:
        run_scenario.delay(scenario.id)

    if incomplete_ids:
        send_mail(
            subject="Scenarios re-run on {}".format(APP_URL),
            message="The following scenarios were re-run:\n{}".format(
                "\n".join([str(x) for x in incomplete_ids])
            ),
            from_email=None,
            recipient_list=[x[1] for x in ADMINS],
            fail_silently=False,
        )


@app.task(soft_time_limit=1800)
def create_caiso_report(
    report_name,
    year,
    query_params,
    overwrite=False,
    chunk_size=timedelta(days=1),
    max_attempts=3,
    destination_directory="caiso_downloads",
    timezone_=timezone("US/Pacific"),
):
    """
    Run CAISOReport.get_or_create().

    :param report_name: see pyoasis.utils.get_report_names()
    :param year: int
    :param query_params: see pyoasis.utils.get_report_params()
    :param overwrite: True to fetch new reports (default: False)
    :param chunk_size: length of report to request (timedelta)
    :param max_attempts: number of back-off attempts (int)
    :param destination_directory: directory to store temporary files
    :param timezone_: pytz.timezone object used for naive start and
        end_limit datetime objects
    """
    CAISOReport.get_or_create(
        report_name=report_name,
        year=year,
        query_params=query_params,
        overwrite=overwrite,
        chunk_size=chunk_size,
        max_attempts=max_attempts,
        destination_directory=destination_directory,
        timezone_=timezone_,
    )
