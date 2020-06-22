import os
from shutil import rmtree

from django.core.management import call_command

from beo_datastore.settings import MEDIA_ROOT
from beo_datastore.libs.utils import mkdir_p

from cost.ghg.models import GHGRate, GHGRateFrame288
from cost.procurement.models import CAISOReport, CAISOReportDataFrame
from load.customer.models import Channel, ChannelIntervalFrame
from load.openei.models import ReferenceMeter, ReferenceMeterIntervalFrame


def load_base_fixtures():
    """
    Loads base fixtures in defined order.
    """
    call_command("loaddata", "reference_model", "ghg")


def load_test_fixtures():
    """
    Loads test fixtures in defined order.
    """
    call_command(
        "loaddata", "customer", "openei", "utility_rate", "caiso_rate"
    )


def load_intervalframe_files():
    """
    Loads parquet fixtures to MEDIA_ROOT.
    """
    for (reference_model, frame_model, fixture_dir) in [
        (GHGRate, GHGRateFrame288, "cost/ghg/fixtures/"),
        (Channel, ChannelIntervalFrame, "load/customer/fixtures/"),
        (ReferenceMeter, ReferenceMeterIntervalFrame, "load/openei/fixtures/"),
        (CAISOReport, CAISOReportDataFrame, "cost/procurement/fixtures/"),
    ]:
        for object in reference_model.objects.all():
            mkdir_p(frame_model.file_directory)
            intervalframe_file = os.path.join(
                fixture_dir, frame_model.get_filename(object)
            )
            object.frame = frame_model.get_frame_from_file(
                reference_object=object, file_path=intervalframe_file
            )
            object.save()


def flush_intervalframe_files():
    """
    Deletes parquet fixtures from MEDIA_ROOT.
    """
    # remove empty MEDIA_ROOT
    if "media_root_test" in MEDIA_ROOT:
        try:
            rmtree(MEDIA_ROOT)
        except OSError:
            pass
