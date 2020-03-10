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
    Run a single Meter's DERSimultion and cost calculations with a
    SingleScenarioStudy.
    """
    single_scenario_study = SingleScenarioStudy.objects.get(
        id=single_scenario_study_id
    )
    single_scenario_study.run_single_meter_simulation_and_cost(
        meter=Meter.objects.get(id=meter_id)
    )
