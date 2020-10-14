from datetime import datetime, timedelta
import pandas as pd

from django.test import TestCase

from beo_datastore.libs.der.schedule_utils import create_diurnal_schedule
from beo_datastore.libs.der.battery import (
    Battery as pyBattery,
    BatterySimulationBuilder,
    BatteryStrategy as pyBatteryStrategy,
)
from beo_datastore.libs.der.builder import DERSimulationDirector
from beo_datastore.libs.fixtures import flush_intervalframe_files
from beo_datastore.libs.load.intervalframe import PowerIntervalFrame

from der.simulation.models import StoredBatterySimulation
from load.customer.models import CustomerMeter, Channel
from reference.reference_model.models import DataUnit
from reference.auth_user.models import LoadServingEntity


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

        self.battery = pyBattery(
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

        self.battery_strategy = pyBatteryStrategy(
            charge_schedule=self.charge_schedule,
            discharge_schedule=self.discharge_schedule,
        )

        # run battery simulation
        builder = BatterySimulationBuilder(
            der=self.battery, der_strategy=self.battery_strategy
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
            meter=self.meter, der_product=self.simulation
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
        Test the retrieval of aggregate battery simulations from disk/database.
        """
        StoredBatterySimulation.generate(
            der=self.battery,
            der_strategy=self.battery_strategy,
            start=self.intervalframe.start_datetime,
            end_limit=self.intervalframe.end_limit_datetime,
            meter_set={self.meter},
            multiprocess=False,
        )

        # retrieve aggregate simulation from disk
        stored_simulations = StoredBatterySimulation.generate(
            der=self.battery,
            der_strategy=self.battery_strategy,
            start=self.intervalframe.start_datetime,
            end_limit=self.intervalframe.end_limit_datetime,
            meter_set={self.meter},
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
