from datetime import datetime, timedelta
import pandas as pd
from unittest import TestCase

from beo_datastore.libs.battery import Battery, FixedScheduleBatterySimulation
from beo_datastore.libs.battery_schedule import create_fixed_schedule
from beo_datastore.libs.intervalframe import ValidationIntervalFrame


class TestBattery(TestCase):
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
        intervalframe = ValidationIntervalFrame(
            pd.DataFrame(
                zip(
                    [datetime(2018, 1, 1, x) for x in range(0, 12)],
                    [-5 for x in range(0, 6)] + [10 for x in range(0, 6)],
                )
            )
            .set_index(0)
            .rename(columns={1: "kw"})
        )

        self.battery = Battery(
            rating=5, discharge_duration=timedelta(hours=2), efficiency=0.5
        )
        # always attempt to charge on negative kW readings
        self.charge_schedule = create_fixed_schedule(
            start_hour=0, end_limit_hour=0, power_limit_1=0, power_limit_2=0
        )
        # always attempt to discharge when load is above 5 kW
        self.discharge_schedule = create_fixed_schedule(
            start_hour=0, end_limit_hour=0, power_limit_1=5, power_limit_2=5
        )

        self.simulation = FixedScheduleBatterySimulation(
            battery=self.battery,
            load_intervalframe=intervalframe,
            charge_schedule=self.charge_schedule,
            discharge_schedule=self.discharge_schedule,
        )
        self.simulation.generate_full_sequence()

    def test_battery_operations(self):
        """
        Test battery operations at each hour.
        """
        # power
        self.assertEqual(
            list(self.simulation.battery_intervalframe.dataframe.kw.values),
            [5.0, 5.0, 5.0, 5.0, 0.0, 0.0, -5.0, -5.0, 0.0, 0.0, 0.0, 0.0],
        )
        # charge
        self.assertEqual(
            list(
                self.simulation.battery_intervalframe.dataframe.charge.values
            ),
            [2.5, 5.0, 7.5, 10.0, 10.0, 10.0, 5.0, 0.0, 0.0, 0.0, 0.0, 0.0],
        )
        # capacity
        self.assertEqual(
            list(
                self.simulation.battery_intervalframe.dataframe.capacity.values
            ),
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
            self.simulation.battery_intervalframe
            + self.simulation.battery_intervalframe
        )
        # power doubles
        self.assertEqual(
            list(aggregate_battery_intervalframe.dataframe.kw.values),
            [
                10.0,
                10.0,
                10.0,
                10.0,
                0.0,
                0.0,
                -10.0,
                -10.0,
                0.0,
                0.0,
                0.0,
                0.0,
            ],
        )
        # charge doubles
        self.assertEqual(
            list(aggregate_battery_intervalframe.dataframe.charge.values),
            [5.0, 10.0, 15.0, 20.0, 20.0, 20.0, 10.0, 0.0, 0.0, 0.0, 0.0, 0.0],
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
