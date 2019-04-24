import os
from shutil import copyfile, rmtree

from django.core.management import call_command

from beo_datastore.settings import MEDIA_ROOT

from cost.ghg.models import GHGRate, GHGRateLookupTable
from load.customer.models import Meter, MeterIntervalFrame
from load.openei.models import (
    ReferenceBuilding,
    ReferenceBuildingIntervalFrame,
)


def load_all_fixtures():
    """
    Loads base fixtures in defined order.
    """
    call_command(
        "loaddata",
        "reference_model",
        "customer",
        "ghg",
        "openei",
        "utility_rate",
    )


def load_intervalframe_files():
    """
    Loads parquet fixtures to MEDIA_ROOT.
    """
    if not os.path.exists(MEDIA_ROOT):
        os.mkdir(MEDIA_ROOT)

    for (reference_model, frame_model, fixture_dir) in [
        (GHGRate, GHGRateLookupTable, "cost/ghg/fixtures/"),
        (Meter, MeterIntervalFrame, "load/customer/fixtures/"),
        (
            ReferenceBuilding,
            ReferenceBuildingIntervalFrame,
            "load/openei/fixtures/",
        ),
    ]:
        for object in reference_model.objects.all():
            if not os.path.exists(frame_model.file_directory):
                os.mkdir(frame_model.file_directory)
            intervalframe_file = os.path.join(
                fixture_dir, frame_model.get_filename(object)
            )
            if os.path.exists(intervalframe_file):
                copyfile(intervalframe_file, frame_model.get_file_path(object))


def flush_intervalframe_files():
    """
    Deletes parquet fixtures from MEDIA_ROOT.
    """
    # remove empty MEDIA_ROOT
    if os.path.exists(MEDIA_ROOT) and "media_root_test" in MEDIA_ROOT:
        rmtree(MEDIA_ROOT)
