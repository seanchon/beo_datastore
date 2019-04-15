import os
from shutil import copyfile

from beo_datastore.settings import MEDIA_ROOT

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

    for meter in Meter.objects.all():
        if not os.path.exists(MeterIntervalFrame.file_directory):
            os.mkdir(MeterIntervalFrame.file_directory)
        copyfile(
            os.path.join(
                "load/customer/fixtures/",
                MeterIntervalFrame.get_filename(meter),
            ),
            MeterIntervalFrame.get_file_path(meter),
        )

    for reference_building in ReferenceBuilding.objects.all():
        if not os.path.exists(ReferenceBuildingIntervalFrame.file_directory):
            os.mkdir(ReferenceBuildingIntervalFrame.file_directory)
        copyfile(
            os.path.join(
                "load/openei/fixtures/",
                ReferenceBuildingIntervalFrame.get_filename(
                    reference_building
                ),
            ),
            ReferenceBuildingIntervalFrame.get_file_path(reference_building),
        )


def flush_intervalframe_fixtures():
    """
    Deletes parquet fixtures from MEDIA_ROOT.
    """
    # delete all Meter and ReferenceBuilding objects and associated files
    Meter.objects.all().delete()
    if os.path.exists(MeterIntervalFrame.file_directory):
        os.rmdir(MeterIntervalFrame.file_directory)

    ReferenceBuilding.objects.all().delete()
    if os.path.exists(ReferenceBuildingIntervalFrame.file_directory):
        os.rmdir(ReferenceBuildingIntervalFrame.file_directory)

    # remove empty MEDIA_ROOT
    if os.path.exists(MEDIA_ROOT):
        os.rmdir(MEDIA_ROOT)
