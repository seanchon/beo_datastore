from faker import Factory
from unittest import mock

from rest_framework import status
from rest_framework.test import APITestCase

from django.contrib.auth.models import User

from der.simulation.models import (
    SolarPVConfiguration,
    SolarPVStrategy,
    SolarPVSimulation,
)
from beo_datastore.libs.der.solar import (
    SolarPV as pySolarPV,
    SolarPVStrategy as pySolarPVStrategy,
)
from beo_datastore.libs.fixtures import (
    flush_intervalframe_files,
    load_intervalframe_files,
)
from beo_datastore.libs.test_mock import mocked_pvwatts_requests_get
from load.customer.models import CustomerMeter, MeterGroup

# SolarPV Configuration
ARRAY_TYPE = 0
AZIMUTH = 180
ADDRESS = 94518
TILT = 20

# SolarPVStrategy
SERVICEABLE_LOAD_RATIO = 0.85


class TestSolarPV(APITestCase):
    fixtures = ["reference_model", "customer", "ghg", "utility_rate"]

    @mock.patch("requests.get", side_effect=mocked_pvwatts_requests_get)
    def setUp(self, mock_get):
        """
        Test SolarPV Django object storage.
        """
        # Copy parquet (dataframe) files to test MEDIA_ROOT.
        load_intervalframe_files()

        self.solar_pv = pySolarPV(
            api_key="ABCDEFG",
            array_type=ARRAY_TYPE,
            azimuth=AZIMUTH,
            address=ADDRESS,
            tilt=TILT,
        )
        self.solar_pv_strategy = pySolarPVStrategy(
            serviceable_load_ratio=SERVICEABLE_LOAD_RATIO
        )

    def tearDown(self):
        """
        Remove test MEDIA_ROOT and contents.
        """
        flush_intervalframe_files()

    @mock.patch("requests.get", side_effect=mocked_pvwatts_requests_get)
    def test_solar_simulation(self, mock_get):
        """
        Test creation of SolarPV Django objects.
        """
        self.assertEqual(SolarPVConfiguration.objects.count(), 0)
        self.assertEqual(SolarPVStrategy.objects.count(), 0)
        self.assertEqual(SolarPVSimulation.objects.count(), 0)

        meter = CustomerMeter.objects.first()
        SolarPVSimulation.generate(
            der=self.solar_pv,
            der_strategy=self.solar_pv_strategy,
            start=meter.intervalframe.start_datetime,
            end_limit=meter.intervalframe.end_limit_datetime,
            meter_set={meter},
        )

        # assert all objects stored to database
        self.assertEqual(SolarPVConfiguration.objects.count(), 1)
        self.assertEqual(SolarPVStrategy.objects.count(), 1)
        self.assertEqual(SolarPVSimulation.objects.count(), 1)

    @mock.patch("requests.get", side_effect=mocked_pvwatts_requests_get)
    def test_solar_api_simulation(self, mock_get):
        """
        Test creation of SolarPV Django objects via POST v1/cost/scenario.
        """
        post_endpoint = "/v1/cost/scenario/"

        # create fake API user
        faker = Factory.create()
        user = User.objects.create(
            username=faker.user_name(), email=faker.email(), is_superuser=False
        )
        self.client.force_authenticate(user=user)

        # associate user with meter_group LSE
        meter_group = MeterGroup.objects.first()
        user.profile.load_serving_entity = meter_group.load_serving_entity
        user.profile.save()

        configuration, _ = SolarPVConfiguration.get_or_create_from_object(
            solar_pv=self.solar_pv
        )
        strategy, _ = SolarPVStrategy.get_or_create_from_object(
            solar_pv_strategy=self.solar_pv_strategy
        )

        data = {
            "cost_functions": {},
            "name": "test",
            "meter_group_ids": [str(meter_group.id)],
            "ders": [
                {
                    "der_configuration_id": str(configuration.id),
                    "der_strategy_id": str(strategy.id),
                }
            ],
        }

        response = self.client.post(post_endpoint, data, format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED)
        self.assertEqual(
            meter_group.meters.count(), response.data["der_simulation_count"]
        )
