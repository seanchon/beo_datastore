from django.test import TestCase

from beo_datastore.libs.fixtures import (
    flush_intervalframe_files,
    load_intervalframe_files,
)

from load.customer.models import Meter


class TestDataFrameFile(TestCase):
    fixtures = ["reference_model", "customer"]

    def setUp(self):
        """
        Copy parquet (dataframe) files to test MEDIA_ROOT.
        """
        load_intervalframe_files()

    def tearDown(self):
        """
        Remove test MEDIA_ROOT and contents.
        """
        flush_intervalframe_files()

    def test_load_intervalframe(self):
        """
        Test load IntervalFrameFile from file.
        """
        meter = Meter.objects.first()
        self.assertTrue(meter.intervalframe)
        self.assertFalse(meter.intervalframe.dataframe.empty)

    def test_create_288_intervalframe(self):
        """
        Tests the creation of 288 frames.
        """
        meter = Meter.objects.first()
        self.assertFalse(meter.intervalframe.average_288_dataframe.empty)
        self.assertFalse(meter.intervalframe.maximum_288_dataframe.empty)
        self.assertFalse(meter.intervalframe.count_288_dataframe.empty)

    def test_delete_288_frame(self):
        """
        Test the creation of 288 frames after IntervalFrameFile is deleted.
        """
        meter = Meter.objects.first()
        meter.intervalframe.delete()
        meter = Meter.objects.first()
        self.assertTrue(meter.intervalframe.dataframe.empty)
        self.assertFalse(meter.intervalframe.average_288_dataframe.empty)
        self.assertFalse(meter.intervalframe.maximum_288_dataframe.empty)
        self.assertFalse(meter.intervalframe.count_288_dataframe.empty)
