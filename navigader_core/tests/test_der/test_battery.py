from datetime import datetime, timedelta
import pandas as pd
from unittest import TestCase

from navigader_core.der.schedule_utils import create_diurnal_schedule
from navigader_core.der.battery import (
    Battery,
    BatterySimulationBuilder,
    BatteryStrategy,
)
from navigader_core.der.builder import DERSimulationDirector
from navigader_core.load.intervalframe import PowerIntervalFrame


class TestBattery(TestCase):
    """
    Tests battery simulation and storage.
    """

    fixtures = ["reference_model"]

    def setUp(self):
        """
        Test battery operations under:

        1. The following hypothetical load conditions:
            - 2018/01/01 midnight to 6 a.m.: -5kW
            - 2018/01/01 6 a.m. to noon: 10kW
        2. The following battery specifications:
            - rating: 5 kW
            - discharge duration: 2 hours
            - efficiency: 50%
            - initial charge 0 kWh
        3. The following charge/discharge strategy:
            - always attempt to charge on negative kW readings
            - always attempt to discharge when load is above 5 kW
        """
        self.intervalframe = PowerIntervalFrame(
            pd.DataFrame(
                zip(
                    [datetime(2018, 1, 1, x) for x in range(0, 12)],
                    [-5 for _ in range(0, 6)] + [10 for _ in range(0, 6)],
                )
            )
            .rename(columns={0: "index", 1: "kw"})
            .set_index("index")
        )

        self.battery = Battery(
            rating=5, discharge_duration=timedelta(hours=2), efficiency=0.5
        )
        # always attempt to charge on negative kW readings
        self.charge_schedule = create_diurnal_schedule(
            start_hour=0, end_limit_hour=0, power_limit_1=0, power_limit_2=0
        )
        # always attempt to discharge when load is above 5 kW
        self.discharge_schedule = create_diurnal_schedule(
            start_hour=0, end_limit_hour=0, power_limit_1=5, power_limit_2=5
        )

        # run battery simulation
        builder = BatterySimulationBuilder(
            der=self.battery,
            der_strategy=BatteryStrategy(
                charge_schedule=self.charge_schedule,
                discharge_schedule=self.discharge_schedule,
            ),
        )
        self.director = DERSimulationDirector(builder=builder)
        self.simulation = self.director.run_single_simulation(
            intervalframe=self.intervalframe
        )

    def test_battery_operations(self):
        """
        Test battery operations at each hour.
        """
        # power
        self.assertEqual(
            list(self.simulation.der_intervalframe.dataframe.kw.values),
            [6.25, 6.25, 0.0, 0.0, 0.0, 0.0, -3.75, -3.75, 0.0, 0.0, 0.0, 0.0],
        )
        # charge
        self.assertEqual(
            list(self.simulation.der_intervalframe.dataframe.charge.values),
            [5.0, 10.0, 10.0, 10.0, 10.0, 10.0, 5.0, 0.0, 0.0, 0.0, 0.0, 0.0],
        )
        # capacity
        self.assertEqual(
            list(self.simulation.der_intervalframe.dataframe.capacity.values),
            [
                10.0,
                10.0,
                10.0,
                10.0,
                10.0,
                10.0,
                10.0,
                10.0,
                10.0,
                10.0,
                10.0,
                10.0,
            ],
        )

    def test_aggregate_battery_operations(self):
        """
        Test battery operations at each hour after adding two batteries
        together.
        """
        aggregate_battery_intervalframe = (
            self.simulation.der_intervalframe
            + self.simulation.der_intervalframe
        )
        # power doubles
        self.assertEqual(
            list(aggregate_battery_intervalframe.dataframe.kw.values),
            [12.5, 12.5, 0.0, 0.0, 0.0, 0.0, -7.5, -7.5, 0.0, 0.0, 0.0, 0.0],
        )
        # charge doubles
        self.assertEqual(
            list(aggregate_battery_intervalframe.dataframe.charge.values),
            [10.0, 20.0, 20.0, 20.0, 20.0, 20.0, 10.0, 0.0, 0.0, 0.0, 0.0, 0.0],
        )
        # capacity doubles
        self.assertEqual(
            list(aggregate_battery_intervalframe.dataframe.capacity.values),
            [
                20.0,
                20.0,
                20.0,
                20.0,
                20.0,
                20.0,
                20.0,
                20.0,
                20.0,
                20.0,
                20.0,
                20.0,
            ],
        )

    def test_zero_period_simulation(self):
        """
        Bug fix to catch ZeroDivisionError.
        """
        single_intervalframe = PowerIntervalFrame(
            pd.DataFrame(
                zip(
                    [datetime(2018, 1, 1, x) for x in range(0, 1)],
                    [-5 for x in range(0, 1)],
                )
            )
            .rename(columns={0: "index", 1: "kw"})
            .set_index("index")
        )

        self.director.run_single_simulation(intervalframe=single_intervalframe)
