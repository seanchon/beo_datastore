from datetime import datetime
from faker import Factory
import itertools

from rest_framework import status
from rest_framework.test import APITestCase

from django.contrib.auth.models import User

from beo_datastore.libs.api.tests import BasicAuthenticationTestMixin
from beo_datastore.libs.fixtures import (
    flush_intervalframe_files,
    load_intervalframe_files,
)
from cost.ghg.models import GHGRate
from cost.study.models import SingleScenarioStudy, MultipleScenarioStudy
from cost.utility_rate.models import RatePlan
from der.simulation.models import BatteryConfiguration, BatteryStrategy
from der.simulation.scripts.generate_der_strategy import (
    generate_bill_reduction_battery_strategy,
)
from load.customer.models import OriginFile
from reference.reference_model.models import MeterGroup


class TestEndpointsCost(APITestCase, BasicAuthenticationTestMixin):
    """
    Ensures endpoints are only accessible to logged-in users and are rendered
    without errors.
    """

    fixtures = ["reference_model", "customer", "ghg", "utility_rate"]

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

        self.endpoints = [
            "/v1/cost/study/?include[]={}".format(x)
            for x in [
                "ders",
                "der_simulations",
                "meters",
                "meter_groups",
                "metadata",
                "report",
                "report_summary",
            ]
        ]

        # create MeterGroup
        meter_group = OriginFile.objects.first()
        meter_group.owners.add(self.user)

        # create battery
        configuration, _ = BatteryConfiguration.objects.get_or_create(
            rating=150, discharge_duration_hours=4, efficiency=0.9
        )

        # create a battery strategy from a RatePlan
        rate_plan = RatePlan.objects.first()
        battery_strategy = generate_bill_reduction_battery_strategy(
            name="E-19",
            charge_grid=True,
            discharge_grid=False,
            rate_plan=rate_plan,
        )

        # create Study
        single, _ = SingleScenarioStudy.objects.get_or_create(
            start=datetime(2018, 1, 1),
            end_limit=datetime(2018, 1, 1, 1),
            der_strategy=battery_strategy,
            der_configuration=configuration,
            meter_group=meter_group,
            rate_plan=RatePlan.objects.first(),
        )
        single.ghg_rates.add(*GHGRate.objects.all())

        multi = MultipleScenarioStudy.objects.create()
        multi.single_scenario_studies.add(single)
        multi.run()

    def tearDown(self):
        flush_intervalframe_files()

    def test_post_duplicate_multiple_scenario_study(self):
        """
        Test new objects created on POST to /cost/multiple_scenario_study/.
        """
        post_endpoint = "/v1/cost/multiple_scenario_study/"
        self.client.force_authenticate(user=self.user)

        # Delete all Study objects
        SingleScenarioStudy.objects.all().delete()
        MultipleScenarioStudy.objects.all().delete()

        meter_group = MeterGroup.objects.first()
        configuration = BatteryConfiguration.objects.first()
        strategy = BatteryStrategy.objects.first()

        data = {
            "name": "test",
            "meter_group_ids": [str(meter_group.id)],
            "ders": [
                {
                    "der_configuration_id": str(configuration.id),
                    "der_strategy_id": str(strategy.id),
                }
            ],
        }

        # 0 count
        self.assertEqual(SingleScenarioStudy.objects.count(), 0)
        self.assertEqual(MultipleScenarioStudy.objects.count(), 0)

        # 1 count
        response = self.client.post(post_endpoint, data, format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED)
        self.assertEqual(SingleScenarioStudy.objects.count(), 1)
        self.assertEqual(MultipleScenarioStudy.objects.count(), 1)

        # 1 count - do not create duplicates
        response = self.client.post(post_endpoint, data, format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED)
        self.assertEqual(SingleScenarioStudy.objects.count(), 1)
        self.assertEqual(MultipleScenarioStudy.objects.count(), 1)

    def test_study_ownership(self):
        """
        Test Study only appears for owner of MeterGroup.
        """
        get_endpoint = "/v1/cost/study/"
        self.client.force_authenticate(user=self.user)

        # 1 SingleScenarioStudy, 1 MultipleScenarioStudy related to MeterGroup
        response = self.client.get(get_endpoint)
        self.assertEqual(len(response.data["results"]["studies"]), 2)

        # 0 SingleScenarioStudy, 0 MultipleScenarioStudy
        self.user.meter_groups.clear()
        response = self.client.get(get_endpoint)
        self.assertEqual(len(response.data["results"]["studies"]), 0)


class TestEndpointsGHGRate(APITestCase, BasicAuthenticationTestMixin):
    """
    Ensures endpoints are only accessible to logged-in users and are rendered
    without errors.
    """

    fixtures = ["reference_model", "ghg"]

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

        self.endpoints = [
            "/v1/cost/ghg_rate/?include[]=data"
            "&start=2020-01-01T00:00:00"
            "&end_limit=2020-03-01T00:00:00&period=1H"
            "&data_format={}".format(x)
            for x in ["interval", "288"]
        ]

    def test_ghg_rate_no_data(self):
        """
        Tests that the `GHGRate` object is not serialized with its data if not
        requested
        """
        get_endpoint = "/v1/cost/ghg_rate/"
        self.client.force_authenticate(user=self.user)
        response = self.client.get(get_endpoint)

        ghg_rates = response.data["results"]["ghg_rates"]
        for ghg_rate in ghg_rates:
            self.assertIsNone(ghg_rate.get("data"))

    def test_ghg_rate_data_288(self):
        """
        Tests that the `GHGRate` object is serialized with frame 288 data when
        the `data_format` is set to `288`
        """
        get_endpoint = "/v1/cost/ghg_rate/?include[]=data&data_format=288"
        self.client.force_authenticate(user=self.user)
        response = self.client.get(get_endpoint)

        ghg_rates = response.data["results"]["ghg_rates"]
        for ghg_rate in ghg_rates:
            ghg_rate_data = ghg_rate["data"]
            self.assertEqual(ghg_rate_data.size, 288)

    def test_ghg_rate_data_interval(self):
        """
        Tests that the `GHGRate` object is serialized with interval data when
        the `data_format` is set to `interval`
        """
        get_endpoint = (
            "/v1/cost/ghg_rate/?"
            "include[]=data&"
            "data_format=interval&"
            "start=1/1/2020&"
            "end_limit=2/1/2020&"
            "period=1H"
        )
        self.client.force_authenticate(user=self.user)
        response = self.client.get(get_endpoint)

        ghg_rates = response.data["results"]["ghg_rates"]
        for ghg_rate in ghg_rates:
            ghg_rate_data = ghg_rate["data"]
            self.assertEqual(ghg_rate_data.size, 24 * 31 * 2)


class TestEndpointsCAISORate(APITestCase, BasicAuthenticationTestMixin):
    """
    Ensures endpoints are only accessible to logged-in users and are rendered
    without errors.
    """

    fixtures = ["reference_model", "caiso_rate"]

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

        periods = [15, 60]
        data_types = [
            "default",
            "average",
            "minimum",
            "maximum",
            "total",
            "count",
        ]

        self.endpoints = [
            "/v1/cost/caiso_rate/?data_types={}&period={}".format(
                data_type, period
            )
            for data_type, period in itertools.product(data_types, periods)
        ]


class TestEndpointsUtilityRatePlan(APITestCase, BasicAuthenticationTestMixin):
    """
    Ensures endpoints are only accessible to logged-in users and are rendered
    without errors.
    """

    fixtures = ["reference_model", "utility_rate"]

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

        self.endpoints = [
            "/v1/cost/rate_plan/?include[]={}".format(related_field)
            for related_field in [
                "rate_collections",
                "load_serving_entity",
                "sector",
                "voltage_category",
            ]
        ]


class TestEndpointsUtilityRateCollection(
    APITestCase, BasicAuthenticationTestMixin
):
    """
    Ensures endpoints are only accessible to logged-in users and are rendered
    without errors.
    """

    fixtures = ["reference_model", "utility_rate"]

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

        self.endpoints = [
            "/v1/cost/rate_collection/?include[]={}".format(related_field)
            for related_field in [
                "rate_data",
                "openei_url",
                "utility_url",
                "effective_date",
            ]
        ]
