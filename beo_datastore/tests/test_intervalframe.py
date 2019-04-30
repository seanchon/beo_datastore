from datetime import datetime
import pandas as pd

from django.test import TestCase

from beo_datastore.libs.fixtures import (
    flush_intervalframe_files,
    load_intervalframe_files,
)
from beo_datastore.libs.intervalframe import ValidationIntervalFrame

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
        Test the creation of 288 frames.
        """
        channel = Channel.objects.first()
        self.assertFalse(channel.intervalframe.count_frame288.dataframe.empty)

    def test_delete_288_frame(self):
        """
        Test the creation of default 288 frames after IntervalFrameFile is
        deleted.
        """
        channel = Channel.objects.first()
        channel.intervalframe.delete()
        channel = Channel.objects.first()
        self.assertTrue(channel.intervalframe.dataframe.empty)
        self.assertFalse(channel.intervalframe.count_frame288.dataframe.empty)


class Test288Computation(TestCase):
    def setUp(self):
        """
        Create the following two ValidationIntervalFrames for 2000/01/01:
            - 1-hour intervals: 1kW constant
            - 15-minute intervals: 1kW constant
        """
        dataframe_60 = pd.DataFrame(
            1,
            columns=["kw"],
            index=pd.date_range(
                start=datetime(2000, 1, 1),
                end=datetime(2000, 1, 1, 23),
                freq="1H",
            ),
        )
        self.intervalframe_60 = ValidationIntervalFrame(dataframe_60)

        dataframe_15 = pd.DataFrame(
            1,
            columns=["kw"],
            index=pd.date_range(
                start=datetime(2000, 1, 1),
                end=datetime(2000, 1, 1, 23, 59),
                freq="15min",
            ),
        )
        self.intervalframe_15 = ValidationIntervalFrame(dataframe_15)

    def test_average_frame288(self):
        """
        Test the average value is 1kWh at every hour in January.
        """
        self.assertEqual(
            {1}, set(self.intervalframe_60.average_frame288.dataframe[1])
        )
        self.assertEqual(
            {1}, set(self.intervalframe_15.average_frame288.dataframe[1])
        )

    def test_minimum_frame288(self):
        """
        Test the minimum value is 1kW at every hour in January.
        """
        self.assertEqual(
            {1}, set(self.intervalframe_60.minimum_frame288.dataframe[1])
        )
        self.assertEqual(
            {1}, set(self.intervalframe_15.minimum_frame288.dataframe[1])
        )

    def test_maximum_frame288(self):
        """
        Test the maximum value is 1kW at every hour in January.
        """
        self.assertEqual(
            {1}, set(self.intervalframe_60.maximum_frame288.dataframe[1])
        )
        self.assertEqual(
            {1}, set(self.intervalframe_15.maximum_frame288.dataframe[1])
        )

    def test_total_frame288(self):
        """
        Test the total value is 1kWh at every hour in January.
        """
        self.assertEqual(
            {1}, set(self.intervalframe_60.total_frame288.dataframe[1])
        )
        self.assertEqual(
            {1}, set(self.intervalframe_15.total_frame288.dataframe[1])
        )

    def test_count_frame288(self):
        """
        Test count at every hour in January is correct.
        """
        self.assertEqual(
            {1}, set(self.intervalframe_60.count_frame288.dataframe[1])
        )
        self.assertEqual(
            {4}, set(self.intervalframe_15.count_frame288.dataframe[1])
        )
