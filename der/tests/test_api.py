from datetime import datetime
from faker import Factory

from rest_framework.test import APITestCase

from django.contrib.auth.models import User

from beo_datastore.libs.api.tests import BasicAuthenticationTestMixin
from beo_datastore.libs.fixtures import (
    flush_intervalframe_files,
    load_intervalframe_files,
)
from cost.ghg.models import GHGRate
from der.simulation.models import (
    BatteryConfiguration,
    BatteryStrategy,
    StoredBatterySimulation,
)
from load.customer.models import CustomerMeter
from reference.reference_model.models import Meter, MeterGroup


class TestEndpointsDER(APITestCase, BasicAuthenticationTestMixin):
    """
    Ensures endpoints are only accessible to logged-in users and are rendered
    without errors.
    """

    fixtures = ["reference_model", "customer", "ghg"]

    def setUp(self):
        """
        Initialize endpoints to test and loads parquet files.
        """
        load_intervalframe_files()

        # create fake API user
        faker = Factory.create()
        self.user = User.objects.create(
            username=faker.user_name(), email=faker.email(), is_superuser=False
        )

        # test following endpoints using BasicAuthenticationTestMixin
        self.endpoints = [
            "/v1/der/configuration/?include[]=data",
            "/v1/der/simulation/?data_types=average",
            "/v1/der/strategy/?include[]=data",
        ]

        # create battery
        configuration, _ = BatteryConfiguration.objects.get_or_create(
            rating=150, discharge_duration_hours=4, efficiency=0.9
        )

        # create a battery strategy from a GHGRate
        ghg_rate = GHGRate.objects.get(
            name="Clean Net Short", effective__year=2030
        )
        frame288 = ghg_rate.frame288
        strategy = BatteryStrategy.generate(
            frame288_name="Clean Net Short 2030",
            frame288=frame288,
            level=8,  # Clean Net Short tables have lots of values per month, so this value should be set higher
            minimize=True,  # objective is to minimize GHG
            discharge_threshold=0,  # only discharge down to 0kW
        )

        # create MeterGroup
        meter_group = MeterGroup.objects.create()
        meter_group.meters.add(*Meter.objects.all())
        meter_group.owners.add(self.user)

        # create a battery simulation
        StoredBatterySimulation.generate(
            battery=configuration.battery,
            start=datetime(2018, 1, 1),
            end_limit=datetime(2018, 1, 2),
            meter_set=CustomerMeter.objects.all(),
            charge_schedule=strategy.charge_schedule.frame288,
            discharge_schedule=strategy.discharge_schedule.frame288,
        )

    def tearDown(self):
        flush_intervalframe_files()
