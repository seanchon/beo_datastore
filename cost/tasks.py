from celery.utils.log import get_task_logger

from beo_datastore.celery import app

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

    if study.der_simulation_count == study.expected_der_simulation_count:
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
