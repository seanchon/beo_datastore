from datetime import timedelta
from pytz import timezone

from celery.utils.log import get_task_logger

from beo_datastore.celery import app

from cost.procurement.models import CAISOReport
from cost.study.models import MultipleScenarioStudy, SingleScenarioStudy
from reference.reference_model.models import Meter, Study


logger = get_task_logger(__name__)


@app.task(soft_time_limit=1800, max_retries=3)
def run_study(study_id):
    """
    Perform a run() operation on a Study.
    """
    study = Study.objects.get(id=study_id)
    study.initialize()  # attach all Meters to Study
    if isinstance(study, SingleScenarioStudy):
        for meter in study.meters.all():
            run_simulation_and_cost.delay(
                single_scenario_study_id=study.id, meter_id=meter.id
            )
    elif isinstance(study, MultipleScenarioStudy):
        for single_scenario_study in study.single_scenario_studies.all():
            for meter in single_scenario_study.meters.all():
                run_simulation_and_cost.delay(
                    single_scenario_study_id=single_scenario_study.id,
                    meter_id=meter.id,
                )


@app.task(soft_time_limit=120, max_retries=3)
def run_simulation_and_cost(single_scenario_study_id, meter_id):
    """
    Run a single Meter's DERSimulation and cost calculations with a
    SingleScenarioStudy.
    """
    study = SingleScenarioStudy.objects.get(id=single_scenario_study_id)
    study.run_single_meter_simulation_and_cost(
        meter=Meter.objects.get(id=meter_id)
    )

    if (
        study.der_simulation_count == study.expected_der_simulation_count
    ) and study.meter_intervalframe.dataframe.empty:
        # all DERSimulation objects created, cache meter_intervalframe
        aggregate_study_intervalframes.delay(study.id)


@app.task(soft_time_limit=1800)
def aggregate_study_intervalframes(study_id, force=False):
    """
    Store post_der_intervalframe as meter_intervalframe

    :param study_id: Study id
    :param force: force recomputation
    """
    study = Study.objects.get(id=study_id)
    if study.meter_intervalframe.dataframe.empty or force:
        study.intervalframe.dataframe = study.post_der_intervalframe.dataframe
        study.save()


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
