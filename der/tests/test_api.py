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
    EVSEStrategy,
    SolarPVConfiguration,
    SolarPVStrategy,
    StoredBatterySimulation,
)
from der.simulation.scripts.generate_der_strategy import (
    generate_commuter_evse_strategy,
    generate_ghg_reduction_battery_strategy,
)
from load.customer.models import CustomerMeter
from reference.auth_user.models import LoadServingEntity
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
            username=faker.user_name(),
            email=faker.email(domain="mcecleanenergy.org"),
            is_superuser=False,
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
            der_configuration=configuration,
            der_strategy=strategy,
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
        self.assertEqual(data, parameters)

    def test_solar_configuration_serializer(self):
        """
        Tests that the SolarPVConfigurationSerializer returns solar-specific
        fields
        """
        parameters = {
            "address": "94107",
            "array_type": 0,
            "azimuth": 180,
            "tilt": 7,
        }

        configuration, _ = SolarPVConfiguration.objects.get_or_create(
            parameters=parameters, stored_response={"foo": "bar"}
        )

        self.client.force_authenticate(user=self.user)
        response = self.client.get(
            f"/v1/der/configuration/{configuration.id}/?include[]=data",
            format="json",
        )

        data = response.data["der_configuration"]["data"]
        self.assertEqual(data, parameters)

    def test_evse_strategy_serializer(self):
        """
        Tests that the EVSEStrategySerializer returns EVSE-specific fields
        """
        strategy = generate_commuter_evse_strategy(
            charge_off_nem=True,
            start_charge_hour=8,
            end_charge_hour=17,
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
            evse_rating=30.0,
            ev_count=15,
            evse_count=5,
            evse_utilization=0.8,
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
                "evse_rating": 30.0,
                "ev_count": 15,
                "evse_count": 5,
                "evse_utilization": 0.8,
            },
        )

    def test_configuration_filtering(self):
        """
        Tests that DERConfigurationViewSet can filter by DER type and LSE
        """
        evse_attrs = {
            "ev_mpkwh": 15,
            "evse_rating": 30.0,
            "ev_count": 15,
            "evse_count": 5,
            "evse_utilization": 0.8,
        }

        mce = LoadServingEntity.objects.get(name__icontains="MCE")
        pge = LoadServingEntity.objects.get(name__icontains="Pacific Gas")

        # One configuration with no LSE (available to everyone)
        EVSEConfiguration.objects.get_or_create(**evse_attrs)

        # One configuration associated with MCE
        EVSEConfiguration.objects.get_or_create(
            **evse_attrs, load_serving_entity=mce
        )

        # One configuration associated with PG&E. This should not be accessible
        # to the MCE user.
        configuration_in_pge, _ = EVSEConfiguration.objects.get_or_create(
            **evse_attrs, load_serving_entity=pge
        )

        self.client.force_authenticate(user=self.user)

        # 3 configurations, 2 EVSE, 1 battery
        response = self.client.get("/v1/der/configuration/", format="json")
        self.assertEqual(response.data["count"], 3)

        # 2 EVSE configurations
        response = self.client.get(
            "/v1/der/configuration/?der_type=EVSE", format="json"
        )
        self.assertEqual(response.data["count"], 2)
        self.assertNotIn(
            str(configuration_in_pge.id),
            (
                obj["id"]
                for obj in response.data["results"]["der_configurations"]
            ),
        )

    def test_strategy_filtering(self):
        """
        Tests that DERStrategyViewSet can filter by DER type and LSE
        """
        mce = LoadServingEntity.objects.get(name__icontains="MCE")
        pge = LoadServingEntity.objects.get(name__icontains="Pacific Gas")

        evse_attrs = {
            "charge_off_nem": True,
            "description": "Test EVSE strategy",
            "start_charge_hour": 8,
            "end_charge_hour": 17,
            "distance": 15,
            "name": "Test EVSE strategy",
        }

        # One strategy with no LSE (available to everyone) and one associated
        # with MCE
        EVSEStrategy.generate(**evse_attrs)
        EVSEStrategy.generate(**evse_attrs, load_serving_entity=mce)

        # One strategy associated with PG&E. This should not be accessible to
        # the MCE user.
        strategy_in_pge = EVSEStrategy.generate(
            **evse_attrs, load_serving_entity=pge
        )

        self.client.force_authenticate(user=self.user)

        # 3 strategies, 2 EVSE, 1 battery
        response = self.client.get("/v1/der/strategy/", format="json")
        self.assertEqual(response.data["count"], 3)

        # 2 EVSE strategies
        response = self.client.get(
            "/v1/der/strategy/?der_type=EVSE", format="json"
        )
        self.assertEqual(response.data["count"], 2)
        self.assertNotIn(
            str(strategy_in_pge.id),
            (obj["id"] for obj in response.data["results"]["der_strategies"]),
        )

    def tearDown(self):
        flush_intervalframe_files()
