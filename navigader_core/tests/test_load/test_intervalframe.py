from datetime import datetime
import pandas as pd

from unittest import TestCase

from navigader_core.load.intervalframe import (
    EnergyIntervalFrame,
    PowerIntervalFrame,
)


class Test288Computation(TestCase):
    def setUp(self):
        """
        Create four ValidationIntervalFrames representing the same readings.

        Create the following two PowerIntervalFrames for 2000/01/01:
            - 1-hour intervals: 1kW constant
            - 15-minute intervals: 1kW constant

        Create the following two EnergyIntervalFrames for 2000/01/01:
            - 1-hour intervals: 1kWh constant
            - 15-minute intervals: 0.25kWh constant
        """
        START = datetime(2000, 1, 1)
        END = datetime(2000, 1, 1, 23, 59)
        RANGE_1_HOUR = pd.date_range(start=START, end=END, freq="1H")
        RANGE_15_MIN = pd.date_range(start=START, end=END, freq="15min")

        power_60 = pd.DataFrame(1, columns=["kw"], index=RANGE_1_HOUR)
        self.power_60 = PowerIntervalFrame(dataframe=power_60)

        power_15 = pd.DataFrame(1, columns=["kw"], index=RANGE_15_MIN)
        self.power_15 = PowerIntervalFrame(dataframe=power_15)

        energy_60 = pd.DataFrame(1, columns=["kwh"], index=RANGE_1_HOUR)
        self.energy_60 = EnergyIntervalFrame(dataframe=energy_60)

        energy_15 = pd.DataFrame(0.25, columns=["kwh"], index=RANGE_15_MIN)
        self.energy_15 = EnergyIntervalFrame(dataframe=energy_15)

    def test_average_frame288(self):
        """
        Test the average value is 1kW at every hour in January.
        """
        self.assertEqual({1}, set(self.power_60.average_frame288.dataframe[1]))
        self.assertEqual({1}, set(self.power_15.average_frame288.dataframe[1]))
        self.assertEqual(
            {1}, set(self.energy_60.average_frame288.dataframe[1])
        )
        self.assertEqual(
            {1}, set(self.energy_15.average_frame288.dataframe[1])
        )

    def test_minimum_frame288(self):
        """
        Test the minimum value is 1kW at every hour in January.
        """
        self.assertEqual({1}, set(self.power_60.minimum_frame288.dataframe[1]))
        self.assertEqual({1}, set(self.power_15.minimum_frame288.dataframe[1]))
        self.assertEqual(
            {1}, set(self.energy_60.minimum_frame288.dataframe[1])
        )
        self.assertEqual(
            {1}, set(self.energy_15.minimum_frame288.dataframe[1])
        )

    def test_maximum_frame288(self):
        """
        Test the maximum value is 1kW at every hour in January.
        """
        self.assertEqual({1}, set(self.power_60.maximum_frame288.dataframe[1]))
        self.assertEqual({1}, set(self.power_15.maximum_frame288.dataframe[1]))
        self.assertEqual(
            {1}, set(self.energy_60.maximum_frame288.dataframe[1])
        )
        self.assertEqual(
            {1}, set(self.energy_15.maximum_frame288.dataframe[1])
        )

    def test_total_frame288(self):
        """
        Test the total value is 1kWh at every hour in January.
        """
        self.assertEqual({1}, set(self.power_60.total_frame288.dataframe[1]))
        self.assertEqual({1}, set(self.power_15.total_frame288.dataframe[1]))
        self.assertEqual({1}, set(self.energy_60.total_frame288.dataframe[1]))
        self.assertEqual({1}, set(self.energy_15.total_frame288.dataframe[1]))

    def test_count_frame288(self):
        """
        Test count at every hour in January is correct.
        """
        self.assertEqual({1}, set(self.power_60.count_frame288.dataframe[1]))
        self.assertEqual({4}, set(self.power_15.count_frame288.dataframe[1]))
        self.assertEqual({1}, set(self.energy_60.count_frame288.dataframe[1]))
        self.assertEqual({4}, set(self.energy_15.count_frame288.dataframe[1]))
