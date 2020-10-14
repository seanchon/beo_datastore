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
    EVSEConfiguration,
    SolarPVConfiguration,
    SolarPVStrategy,
    StoredBatterySimulation,
)
from der.simulation.scripts.generate_der_strategy import (
    generate_commuter_evse_strategy,
    generate_ghg_reduction_battery_strategy,
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
        strategy = generate_ghg_reduction_battery_strategy(
            name="2018 System Load",
            charge_grid=True,
            discharge_grid=False,
            ghg_rate=ghg_rate,
        )

        # create MeterGroup
        meter_group = MeterGroup.objects.create()
        meter_group.meters.add(*Meter.objects.all())
        meter_group.owners.add(self.user)

        # create a battery simulation
        StoredBatterySimulation.generate(
            der=configuration.der,
            der_strategy=strategy.der_strategy,
            start=datetime(2018, 1, 1),
            end_limit=datetime(2018, 1, 2),
            meter_set=CustomerMeter.objects.all(),
        )

    def test_battery_strategy_serializer(self):
        """
        Tests that the BatteryStrategySerializer returns battery-specific fields
        """
        strategy = BatteryStrategy.objects.first()
        self.client.force_authenticate(user=self.user)
        response = self.client.get(
            f"/v1/der/strategy/{strategy.id}/?include[]=data", format="json"
        )

        data = response.data["der_strategy"]["data"]
        self.assertIn("charge_schedule_frame", data)
        self.assertIn("discharge_schedule_frame", data)

    def test_battery_configuration_serializer(self):
        """
        Tests that the BatteryConfigurationSerializer returns battery-specific
        fields
        """
        configuration = BatteryConfiguration.objects.first()
        self.client.force_authenticate(user=self.user)
        response = self.client.get(
            f"/v1/der/configuration/{configuration.id}/?include[]=data",
            format="json",
        )

        data = response.data["der_configuration"]["data"]
        self.assertEqual(
            data,
            {"rating": 150, "discharge_duration_hours": 4, "efficiency": 0.9},
        )

    def test_solar_strategy_serializer(self):
        """
        Tests that the SolarPVStrategySerializer returns solar-specific fields
        """
        parameters = {"serviceable_load_ratio": 0.85}
        strategy = SolarPVStrategy.objects.create(parameters=parameters)
        self.client.force_authenticate(user=self.user)
        response = self.client.get(
            f"/v1/der/strategy/{strategy.id}/?include[]=data", format="json"
        )

        data = response.data["der_strategy"]["data"]
        self.assertIn("parameters", data)
        self.assertEqual(data["parameters"], parameters)

    def test_solar_configuration_serializer(self):
        """
        Tests that the SolarPVConfigurationSerializer returns solar-specific
        fields
        """
        parameters = {"serviceable_load_ratio": 0.85}
        configuration, _ = SolarPVConfiguration.objects.get_or_create(
            parameters=parameters, stored_response={"foo": "bar"}
        )

        self.client.force_authenticate(user=self.user)
        response = self.client.get(
            f"/v1/der/configuration/{configuration.id}/?include[]=data",
            format="json",
        )

        data = response.data["der_configuration"]["data"]
        self.assertEqual(data["parameters"], parameters)

    def test_evse_strategy_serializer(self):
        """
        Tests that the EVSEStrategySerializer returns EVSE-specific fields
        """
        strategy = generate_commuter_evse_strategy(
            charge_off_nem=True,
            drive_in_hour=8,
            drive_home_hour=17,
            distance=20,
            name="Test EVSE strategy",
        )

        self.client.force_authenticate(user=self.user)
        response = self.client.get(
            f"/v1/der/strategy/{strategy.id}/?include[]=data", format="json"
        )

        data = response.data["der_strategy"]["data"]
        self.assertIn("charge_schedule", data)
        self.assertIn("drive_schedule", data)

    def test_evse_configuration_serializer(self):
        """
        Tests that the EVSEConfigurationSerializer returns EVSE-specific fields
        """
        configuration, _ = EVSEConfiguration.objects.get_or_create(
            ev_mpkwh=15,
            ev_mpg_eq=20,
            ev_capacity=300,
            ev_efficiency=0.87,
            evse_rating=30.0,
            ev_count=15,
            evse_count=5,
        )

        self.client.force_authenticate(user=self.user)
        response = self.client.get(
            f"/v1/der/configuration/{configuration.id}/?include[]=data",
            format="json",
        )

        data = response.data["der_configuration"]["data"]
        self.assertEqual(
            data,
            {
                "ev_mpkwh": 15,
                "ev_mpg_eq": 20,
                "ev_capacity": 300,
                "ev_efficiency": 0.87,
                "evse_rating": 30.0,
                "ev_count": 15,
                "evse_count": 5,
            },
        )

    def tearDown(self):
        flush_intervalframe_files()
