from datetime import datetime, timedelta
import pandas as pd

from django.test import TestCase

from beo_datastore.libs.battery import Battery, FixedScheduleBatterySimulation
from beo_datastore.libs.battery_schedule import create_fixed_schedule
from beo_datastore.libs.fixtures import flush_intervalframe_files
from beo_datastore.libs.intervalframe import ValidationIntervalFrame

from der.simulation.models import StoredBatterySimulation
from load.customer.models import CustomerMeter, Channel
from reference.reference_model.models import DataUnit, LoadServingEntity


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
        self.intervalframe = ValidationIntervalFrame(
            pd.DataFrame(
                zip(
                    [datetime(2018, 1, 1, x) for x in range(0, 12)],
                    [-5 for x in range(0, 6)] + [10 for x in range(0, 6)],
                )
            )
            .rename(columns={0: "index", 1: "kw"})
            .set_index("index")
        )

        # create test meter
        self.meter = CustomerMeter.objects.create(
            sa_id="123",
            rate_plan_name=None,
            load_serving_entity=LoadServingEntity.objects.first(),
            import_hash=self.intervalframe.__hash__(),
        )
        Channel.create(
            export=False,
            data_unit=DataUnit.objects.get(name="kw"),
            meter=self.meter,
            dataframe=self.intervalframe.dataframe,
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

        # run battery simulation
        self.simulation = FixedScheduleBatterySimulation(
            battery=self.battery,
            load_intervalframe=self.intervalframe,
            charge_schedule=self.charge_schedule,
            discharge_schedule=self.discharge_schedule,
        )
        self.simulation.generate_full_sequence()

    def tearDown(self):
        flush_intervalframe_files()

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

    def test_stored_simulation(self):
        """
        Test the retrieval of battery simulation elements from disk/database.
        """
        StoredBatterySimulation.get_or_create_from_objects(
            meter=self.meter, simulation=self.simulation
        )

        # retrieve simulation from disk
        stored_simulation = StoredBatterySimulation.objects.last()
        self.assertEqual(
            stored_simulation.simulation.charge_schedule,
            self.simulation.charge_schedule,
        )
        self.assertEqual(
            stored_simulation.simulation.discharge_schedule,
            self.simulation.discharge_schedule,
        )
        self.assertEqual(
            stored_simulation.simulation.battery, self.simulation.battery
        )
        self.assertEqual(
            stored_simulation.simulation.battery_intervalframe,
            self.simulation.battery_intervalframe,
        )

    def test_stored_aggregate_simulation(self):
        """
        Test the retreival of aggregate battery simulations from disk/database.
        """
        StoredBatterySimulation.generate(
            battery=self.battery,
            start=self.intervalframe.start_datetime,
            end_limit=self.intervalframe.end_limit_datetime,
            meter_set={self.meter},
            charge_schedule=self.charge_schedule,
            discharge_schedule=self.discharge_schedule,
            multiprocess=False,
        )

        # retrieve aggregate simulation from disk
        stored_simulations = StoredBatterySimulation.generate(
            battery=self.battery,
            start=self.intervalframe.start_datetime,
            end_limit=self.intervalframe.end_limit_datetime,
            meter_set={self.meter},
            charge_schedule=self.charge_schedule,
            discharge_schedule=self.discharge_schedule,
        )

        # test same intervalframes
        self.assertEqual(
            stored_simulations.first().pre_intervalframe,
            self.simulation.pre_intervalframe,
        )
        self.assertEqual(
            stored_simulations.first().post_intervalframe,
            self.simulation.post_intervalframe,
        )
