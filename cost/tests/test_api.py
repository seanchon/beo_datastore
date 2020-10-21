from datetime import datetime
from faker import Factory
import itertools
import json

from rest_framework import status
from rest_framework.test import APITestCase

from django.contrib.auth.models import User

from beo_datastore.libs.api.tests import BasicAuthenticationTestMixin
from beo_datastore.libs.fixtures import (
    flush_intervalframe_files,
    load_intervalframe_files,
)
from cost.ghg.models import GHGRate
from cost.procurement.models import CAISORate
from cost.study.models import Scenario
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

    fixtures = [
        "reference_model",
        "customer",
        "ghg",
        "utility_rate",
        "caiso_rate",
    ]

    def setUp(self):
        """
        Initialize endpoints to test and loads parquet files.
        """
        load_intervalframe_files()

        # create fake API user
        faker = Factory.create()
        self.user = User.objects.create(
            username=faker.user_name(),
            email=faker.email(domain="@pge.com"),
            is_superuser=False,
        )

        self.endpoints = [
            "/v1/cost/scenario/?include[]={}".format(x)
            for x in [
                "ders",
                "der_simulations",
                "meters",
                "meter_group",
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

        # create Scenario
        scenario, _ = Scenario.objects.get_or_create(
            start=datetime(2018, 1, 1),
            end_limit=datetime(2018, 1, 1, 1),
            der_strategy=battery_strategy,
            der_configuration=configuration,
            meter_group=meter_group,
            rate_plan=RatePlan.objects.first(),
        )
        scenario.ghg_rate = GHGRate.objects.first()
        scenario.save()
        scenario.run()

    def tearDown(self):
        flush_intervalframe_files()

    def test_post_duplicate_scenario(self):
        """
        Test new objects created on POST to /cost/scenario/.
        """
        post_endpoint = "/v1/cost/scenario/"
        self.client.force_authenticate(user=self.user)

        # Delete all Scenario objects
        Scenario.objects.all().delete()

        meter_group = MeterGroup.objects.first()
        configuration = BatteryConfiguration.objects.first()
        strategy = BatteryStrategy.objects.first()

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

        # 0 count
        self.assertEqual(Scenario.objects.count(), 0)

        # 1 count
        response = self.client.post(post_endpoint, data, format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED)
        self.assertEqual(Scenario.objects.count(), 1)

        # 1 count - do not create duplicates
        response = self.client.post(post_endpoint, data, format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED)
        self.assertEqual(Scenario.objects.count(), 1)

    def test_scenario_ownership(self):
        """
        Test Scenario only appears for owner of MeterGroup.
        """
        get_endpoint = "/v1/cost/scenario/"
        self.client.force_authenticate(user=self.user)

        # 1 Scenario related to MeterGroup
        response = self.client.get(get_endpoint)
        self.assertEqual(len(response.data["results"]["scenarios"]), 1)

        # 0 Scenario
        self.user.meter_groups.clear()
        response = self.client.get(get_endpoint)
        self.assertEqual(len(response.data["results"]["scenarios"]), 0)

    def test_scenario_creation_assigns_cost_functions(self):
        """
        Tests that cost functions are correctly assigned to the scenario upon
        creation
        """
        self.client.force_authenticate(user=self.user)

        # Delete all Scenario objects
        Scenario.objects.all().delete()

        meter_group = MeterGroup.objects.first()
        configuration = BatteryConfiguration.objects.first()
        strategy = BatteryStrategy.objects.first()
        rate_plan = RatePlan.objects.first()
        ghg_rate = GHGRate.objects.first()
        procurement_rate = CAISORate.objects.first()

        data = {
            "cost_functions": {
                "rate_plan": rate_plan.id,
                "ghg_rate": ghg_rate.id,
                "procurement_rate": procurement_rate.id,
            },
            "name": "test",
            "meter_group_ids": [str(meter_group.id)],
            "ders": [
                {
                    "der_configuration_id": str(configuration.id),
                    "der_strategy_id": str(strategy.id),
                }
            ],
        }

        # Create the scenario
        post_endpoint = "/v1/cost/scenario/"
        response = self.client.post(post_endpoint, data, format="json")

        # Assert the scenario has been assigned the correct rates
        scenario = Scenario.objects.get(id=response.data["id"])
        self.assertEqual(scenario.rate_plan, rate_plan)
        self.assertEqual(scenario.ghg_rate, ghg_rate)
        self.assertEqual(scenario.procurement_rate, procurement_rate)


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
        Initialize endpoints
        """

        # create fake API user
        faker = Factory.create()
        self.user = User.objects.create(
            username=faker.user_name(), email=faker.email(), is_superuser=False
        )

        self.endpoints = [
            "/v1/cost/rate_plan/?include[]={}".format(related_field)
            for related_field in [
                "rate_collections.*",
                "load_serving_entity.*",
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
        Initialize user and endpoint
        """

        # create fake API user
        faker = Factory.create()
        self.user = User.objects.create(
            username=faker.user_name(), email=faker.email(), is_superuser=False
        )

        self.endpoints = [
            "/v1/cost/rate_collection/",
            "/v1/cost/rate_collection/?include[]=rate_plan",
        ]

        self.csv_files = [
            "cost/utility_rate/tests/data/{}.csv".format(name)
            for name in [
                "COMMERCIAL RATES",
                "General Service Single Phase: Option A",
                "Medium Commercial",
                "Residential Rate",
                "Small Commercial - SC Single-Phase",
            ]
        ]

    def test_create_with_file(self):
        """
        Loop through sample CSV files and ensure they create a RateCollection
        object
        """
        self.client.force_authenticate(user=self.user)
        rate_plan = RatePlan.objects.first()
        for filename in self.csv_files:
            with open(filename) as fp:
                resp = self.client.post(
                    "/v1/cost/rate_collection/",
                    {"rate_data_csv": fp, "rate_plan": rate_plan.id},
                )
                self.assertEqual(resp.status_code, 201)

    def test_create_with_json(self):
        """
        Loop through the many different rate_data objects in the test file
        and ensure that those post requests are successful as well.
        """
        self.client.force_authenticate(user=self.user)
        rate_plan = RatePlan.objects.first()
        with open("cost/utility_rate/tests/data/openei_test_file.json") as fp:
            json_list = json.load(fp)
            for rate_data in json_list:
                body = {"rate_data": rate_data, "rate_plan": rate_plan.id}
                if "effectiveDate" not in rate_data:
                    body["effective_date"] = datetime(2020, 3, 1).date()
                if not rate_data["sourceReference"].startswith("http"):
                    body["utility_url"] = "http://www.example.com"
                resp = self.client.post(
                    "/v1/cost/rate_collection/", body, format="json"
                )
                self.assertEqual(resp.status_code, 201)
