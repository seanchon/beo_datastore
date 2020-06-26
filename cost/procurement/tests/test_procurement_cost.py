from datetime import datetime, timedelta
import numpy as np
import pandas as pd

from django.test import TestCase

from beo_datastore.libs.battery import Battery
from beo_datastore.libs.battery_schedule import create_fixed_schedule
from beo_datastore.libs.controller import (
    AggregateBatterySimulation,
    AggregateProcurementCostCalculation,
)
from beo_datastore.libs.intervalframe import (
    EnergyIntervalFrame,
    PowerIntervalFrame,
)
from beo_datastore.libs.procurement import (
    ProcurementCostIntervalFrame,
    ProcurementRateIntervalFrame,
)


class TestProcurementCost(TestCase):
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
        START = datetime(2020, 1, 1)
        END = datetime(2020, 1, 1, 23, 59)
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

        # 15-minute procurement rates incrementing from $1 to $96.
        procurement_rate_15 = pd.DataFrame(
            np.arange(1, 97, 1), columns=["$/kwh"], index=RANGE_15_MIN
        )
        self.procurement_rate_15 = ProcurementRateIntervalFrame(
            dataframe=procurement_rate_15
        )
        # Daily cost should equal ((1 + 96) * (96 / 2)) / 4 = $1,164
        self.procurement_cost = procurement_rate_15["$/kwh"].sum() / 4

    def test_procurement_cost_intervalframe(self):
        """
        Test that the same procurement cost is calculated using each of the
        four PowerIntervalFrame(s)/EnergyIntervalFrame(s) and
        ProcurementRateIntervalFrame.
        """
        for intervalframe in [
            self.power_60,
            self.power_15,
            self.energy_60,
            self.energy_15,
        ]:
            procurement_cost = self.procurement_rate_15.get_procurement_cost_intervalframe(
                intervalframe=intervalframe
            )
            self.assertEqual(
                procurement_cost.dataframe["$"].sum(), self.procurement_cost
            )

    def test_aggregate_procurement_cost_calculation(self):
        """
        Test that the same AggregateProcurementCostCalculation is instantiated
        using each of the four PowerIntervalFrame(s)/EnergyIntervalFrame(s) and
        ProcurementRateIntervalFrame. A dummy Battery with a
        """
        # create AggregateBatterySimulation using no-operation battery and
        # no-operation battery strategy
        agg_simulation = AggregateBatterySimulation.create(
            battery=Battery(
                rating=0, discharge_duration=timedelta(0, 3600), efficiency=1.0
            ),
            start=datetime(2020, 1, 1),
            end_limit=datetime(2020, 1, 2),
            meter_dict={
                1: self.power_60,
                2: self.power_15,
                3: self.energy_60,
                4: self.energy_15,
            },
            charge_schedule=create_fixed_schedule(
                start_hour=0,
                end_limit_hour=0,
                power_limit_1=0,
                power_limit_2=0,
            ),
            discharge_schedule=create_fixed_schedule(
                start_hour=0,
                end_limit_hour=0,
                power_limit_1=0,
                power_limit_2=0,
            ),
        )

        procurement_cost_calculation = AggregateProcurementCostCalculation(
            agg_simulation=agg_simulation,
            procurement_rate_intervalframe=self.procurement_rate_15,
        )

        # sum total of procurement costs should be $1,164 * 4 = $4,656
        self.assertEqual(
            procurement_cost_calculation.pre_DER_total,
            self.procurement_cost * 4,
        )
        self.assertEqual(
            procurement_cost_calculation.post_DER_total,
            self.procurement_cost * 4,
        )

    def test_null_procurement_intervalframe(self):
        """
        Test null cases for ProcurementRateIntervalFrame and
        ProcurementCostIntervalFrame transforms.
        """
        procurement_rate = ProcurementRateIntervalFrame()
        procurement_cost = procurement_rate.get_procurement_cost_intervalframe(
            self.power_60
        )
        self.assertEqual(procurement_cost, ProcurementCostIntervalFrame())
        self.assertEqual(
            procurement_cost.get_procurement_rate_intervalframe(),
            ProcurementRateIntervalFrame(),
        )
