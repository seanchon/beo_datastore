from datetime import datetime, timedelta
import numpy as np
import pandas as pd

from django.test import TestCase

from navigader_core.der.schedule_utils import create_diurnal_schedule
from navigader_core.cost.controller import AggregateProcurementCostCalculation
from navigader_core.der.battery import (
    Battery,
    BatterySimulationBuilder,
    BatteryStrategy,
)
from navigader_core.der.builder import DERSimulationDirector
from navigader_core.load.intervalframe import (
    EnergyIntervalFrame,
    PowerIntervalFrame,
)
from navigader_core.cost.procurement import (
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
        der = Battery(
            rating=0, discharge_duration=timedelta(0, 3600), efficiency=1.0
        )
        der_strategy = BatteryStrategy(
            charge_schedule=create_diurnal_schedule(
                start_hour=0,
                end_limit_hour=0,
                power_limit_1=0,
                power_limit_2=0,
            ),
            discharge_schedule=create_diurnal_schedule(
                start_hour=0,
                end_limit_hour=0,
                power_limit_1=0,
                power_limit_2=0,
            ),
        )
        builder = BatterySimulationBuilder(der=der, der_strategy=der_strategy)
        director = DERSimulationDirector(builder=builder)

        agg_simulation = director.run_many_simulations(
            start=datetime(2020, 1, 1),
            end_limit=datetime(2020, 1, 2),
            intervalframe_dict={
                1: self.power_60,
                2: self.power_15,
                3: self.energy_60,
                4: self.energy_15,
            },
        )

        procurement_cost_calculation = AggregateProcurementCostCalculation(
            agg_simulation=agg_simulation, rate_data=self.procurement_rate_15
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
