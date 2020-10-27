from django.test import TestCase

from navigader_core.load.intervalframe import (
    EnergyIntervalFrame,
    PowerIntervalFrame,
)

from beo_datastore.libs.fixtures import (
    flush_intervalframe_files,
    load_intervalframe_files,
)

from load.customer.models import Channel


class TestIntervalFrame(TestCase):
    def test_null_intervalframe(self):
        """
        Test null cases for EnergyIntervalFrame and PowerIntervalFrame
        transforms.
        """
        self.assertEqual(
            EnergyIntervalFrame().power_intervalframe, PowerIntervalFrame()
        )
        self.assertEqual(
            PowerIntervalFrame().energy_intervalframe, EnergyIntervalFrame()
        )


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

    def test_create_288_intervalframe(self):
        """
        Test the creation of 288 frames.
        """
        channel = Channel.objects.first()
        self.assertFalse(channel.intervalframe.count_frame288.dataframe.empty)

    def test_read_intervalframe(self):
        """
        Test read PowerIntervalFrameFile from file.
        """
        channel = Channel.objects.first()
        self.assertTrue(channel.intervalframe)
        self.assertFalse(channel.intervalframe.dataframe.empty)

    def test_update_intervalframe(self):
        """
        Test read PowerIntervalFrameFile from file.
        """
        channel_1 = Channel.objects.get(id=1)
        channel_2 = Channel.objects.get(id=2)
        self.assertFalse(
            channel_1.intervalframe.dataframe.equals(
                channel_2.intervalframe.dataframe
            )
        )
        channel_1.intervalframe.dataframe = channel_2.intervalframe.dataframe
        channel_1.intervalframe.save()
        self.assertTrue(
            channel_1.intervalframe.dataframe.equals(
                channel_2.intervalframe.dataframe
            )
        )

    def test_delete_288_frame(self):
        """
        Test the creation of default 288 frames after PowerIntervalFrameFile is
        deleted.
        """
        channel = Channel.objects.first()
        channel.intervalframe.delete()
        channel = Channel.objects.first()
        self.assertTrue(channel.intervalframe.dataframe.empty)
        self.assertFalse(channel.intervalframe.count_frame288.dataframe.empty)
