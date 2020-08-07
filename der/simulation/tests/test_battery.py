from datetime import datetime, timedelta
import pandas as pd

from django.test import TestCase

from beo_datastore.libs.battery_schedule import create_fixed_schedule
from beo_datastore.libs.der.battery import (
    Battery,
    BatterySimulationBuilder,
    BatteryStrategy,
)
from beo_datastore.libs.der.builder import DERSimulationDirector
from beo_datastore.libs.fixtures import flush_intervalframe_files
from beo_datastore.libs.intervalframe import PowerIntervalFrame

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
        self.intervalframe = PowerIntervalFrame(
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

    def tearDown(self):
        flush_intervalframe_files()

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
            stored_simulation.simulation.der_strategy.charge_schedule,
            self.simulation.der_strategy.charge_schedule,
        )
        self.assertEqual(
            stored_simulation.simulation.der_strategy.discharge_schedule,
            self.simulation.der_strategy.discharge_schedule,
        )
        self.assertEqual(stored_simulation.simulation.der, self.simulation.der)
        self.assertEqual(
            stored_simulation.simulation.der_intervalframe,
            self.simulation.der_intervalframe,
        )

    def test_stored_aggregate_simulation(self):
        """
        Test the retreival of aggregate battery simulations from disk/database.
        """
        StoredBatterySimulation.generate(
            der=self.battery,
            start=self.intervalframe.start_datetime,
            end_limit=self.intervalframe.end_limit_datetime,
            meter_set={self.meter},
            charge_schedule=self.charge_schedule,
            discharge_schedule=self.discharge_schedule,
            multiprocess=False,
        )

        # retrieve aggregate simulation from disk
        stored_simulations = StoredBatterySimulation.generate(
            der=self.battery,
            start=self.intervalframe.start_datetime,
            end_limit=self.intervalframe.end_limit_datetime,
            meter_set={self.meter},
            charge_schedule=self.charge_schedule,
            discharge_schedule=self.discharge_schedule,
        )

        # test same intervalframes
        self.assertEqual(
            stored_simulations.first().pre_der_intervalframe,
            self.simulation.pre_der_intervalframe,
        )
        self.assertEqual(
            stored_simulations.first().post_der_intervalframe,
            self.simulation.post_der_intervalframe,
        )
