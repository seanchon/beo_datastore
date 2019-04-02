import os
from shutil import copyfile, rmtree

from django.test import TestCase

from beo_datastore.libs.intervalframe import Frame288
from beo_datastore.settings import BASE_DIR, MEDIA_ROOT

from interval.models import Meter, MeterIntervalFrame


class MeterFrame288(Frame288):
    """
    Model for handling Meter IntervalFrames, which have timestamps and values.
    """

    reference_model = Meter
    file_directory = os.path.join(MEDIA_ROOT, "meters")
    file_prefix = "meter_288_"


class TestDataFrameFile(TestCase):
    fixtures = ["reference_unit", "interval"]

    def setUp(self):
        """
        Copy parquet (dataframe) files to test MEDIA_ROOT.
        """
        if not os.path.exists(MEDIA_ROOT):
            os.mkdir(MEDIA_ROOT)
        if not os.path.exists(MeterIntervalFrame.file_directory):
            os.mkdir(MeterIntervalFrame.file_directory)

        test_data_dir = os.path.join(
            BASE_DIR, "interval", "tests", "meter_data"
        )

        src = os.path.join(test_data_dir, "MeterIntervalFrame_1.parquet")
        self.dst = os.path.join(
            MeterIntervalFrame.file_directory, "MeterIntervalFrame_1.parquet"
        )
        copyfile(src, self.dst)

    def tearDown(self):
        """
        Remove test MEDIA_ROOT and contents.
        """
        if "test_data" in MEDIA_ROOT:
            rmtree(MEDIA_ROOT)

    def test_load_intervalframe(self):
        """
        Test load IntervalFrame from file.
        """
        meter = Meter.objects.first()
        self.assertTrue(meter.intervalframe)
        self.assertFalse(meter.intervalframe.dataframe.empty)

    def test_create_288_intervalframe(self):
        """
        Tests the creation of Frame288.
        """
        meter = Meter.objects.first()
        frame_288 = MeterFrame288(meter, meter.average_288_dataframe)
        self.assertFalse(frame_288.dataframe.empty)
