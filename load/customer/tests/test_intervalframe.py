from django.test import TestCase

from beo_datastore.libs.fixtures import (
    flush_intervalframe_files,
    load_intervalframe_files,
)

from load.customer.models import Channel


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
        channel = Channel.objects.first()
        self.assertTrue(channel.intervalframe)
        self.assertFalse(channel.intervalframe.dataframe.empty)

    def test_create_288_intervalframe(self):
        """
        Tests the creation of 288 frames.
        """
        channel = Channel.objects.first()
        self.assertFalse(channel.intervalframe.average_288_dataframe.empty)
        self.assertFalse(channel.intervalframe.maximum_288_dataframe.empty)
        self.assertFalse(channel.intervalframe.count_288_dataframe.empty)

    def test_delete_288_frame(self):
        """
        Test the creation of 288 frames after IntervalFrameFile is deleted.
        """
        channel = Channel.objects.first()
        channel.intervalframe.delete()
        channel = Channel.objects.first()
        self.assertTrue(channel.intervalframe.dataframe.empty)
        self.assertFalse(channel.intervalframe.average_288_dataframe.empty)
        self.assertFalse(channel.intervalframe.maximum_288_dataframe.empty)
        self.assertFalse(channel.intervalframe.count_288_dataframe.empty)
