from celery.utils.log import get_task_logger

from beo_datastore.celery import app

from reference.reference_model.models import Study


logger = get_task_logger(__name__)


@app.task(soft_time_limit=1800, max_retries=3)
def run_study(study_id):
    """
    Perform a run() operation on a Study.
    """
    study = Study.objects.get(id=study_id)
    study.run(multiprocess=False)
