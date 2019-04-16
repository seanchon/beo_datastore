import os
from shutil import copyfile, rmtree

from beo_datastore.settings import MEDIA_ROOT

from cost.ghg.models import CleanNetShort, CleanNetShortLookupTable
from load.customer.models import Meter, MeterIntervalFrame
from load.openei.models import (
    ReferenceBuilding,
    ReferenceBuildingIntervalFrame,
)


def load_intervalframe_fixtures():
    """
    Loads parquet fixtures to MEDIA_ROOT.
    """
    if not os.path.exists(MEDIA_ROOT):
        os.mkdir(MEDIA_ROOT)

    for (reference_model, frame_model, fixture_dir) in [
        (CleanNetShort, CleanNetShortLookupTable, "cost/ghg/fixtures/"),
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
            copyfile(
                os.path.join(fixture_dir, frame_model.get_filename(object)),
                frame_model.get_file_path(object),
            )


def flush_intervalframe_fixtures():
    """
    Deletes parquet fixtures from MEDIA_ROOT.
    """
    # remove empty MEDIA_ROOT
    if os.path.exists(MEDIA_ROOT) and "media_root_test" in MEDIA_ROOT:
        rmtree(MEDIA_ROOT)
