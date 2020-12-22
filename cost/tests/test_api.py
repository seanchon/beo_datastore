from datetime import datetime, timedelta
from faker import Factory
import itertools
import json
from unittest.mock import patch
import pandas as pd

from rest_framework import status
from rest_framework.test import APITestCase

from django.contrib.auth.models import User

from beo_datastore.libs.tests import BasicAuthenticationTestMixin
from beo_datastore.libs.fixtures import (
    flush_intervalframe_files,
    load_intervalframe_files,
)
from beo_datastore.libs.intervalframe_file import (
    ProcurementRateIntervalFrameFile,
)
from cost.ghg.models import GHGRate
from cost.procurement.models import CAISORate, SystemProfile
from cost.study.models import Scenario
from cost.utility_rate.models import RateCollection, RatePlan
from der.simulation.models import BatteryConfiguration
from der.simulation.scripts.generate_der_strategy import (
    generate_bill_reduction_battery_strategy,
)
from load.customer.models import OriginFile
from reference.auth_user.models import LoadServingEntity


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
        "system_profile",
    ]

    endpoint = "/v1/cost/scenario/"
    endpoints = [
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

        # create MeterGroup
        self.meter_group = OriginFile.objects.first()
        self.meter_group.owners.add(self.user)

        # Make cost function objects to use in scenarios
        self.procurement_rate = CAISORate.objects.first()
        self.procurement_rate.intervalframe = ProcurementRateIntervalFrameFile(
            pd.read_parquet(
                "cost/procurement/fixtures/ProcurementIntervalFrame_2.parquet"
            )
        )
        self.shift_year(self.procurement_rate, -1)
        self.ghg_rate = GHGRate.objects.first()
        self.rate_plan = RatePlan.objects.create(
            load_serving_entity=self.meter_group.load_serving_entity,
            name="ABC",
            sector="Residential",
        )
        self.rate_plan.rate_collections.set([RateCollection.objects.first()])
        self.system_profile = SystemProfile.objects.filter(
            load_serving_entity=self.user.profile.load_serving_entity
        ).first()

        # create battery
        self.configuration, _ = BatteryConfiguration.objects.get_or_create(
            rating=150, discharge_duration_hours=4, efficiency=0.9
        )

        # create a battery strategy from a RatePlan
        self.strategy = generate_bill_reduction_battery_strategy(
            name="E-19",
            charge_grid=True,
            discharge_grid=False,
            rate_plan=self.rate_plan,
        )

        # create Scenario
        scenario, _ = Scenario.objects.get_or_create(
            start=datetime(2018, 1, 1),
            end_limit=datetime(2018, 1, 1, 1),
            der_strategy=self.strategy,
            der_configuration=self.configuration,
            meter_group=self.meter_group,
            rate_plan=self.rate_plan,
            ghg_rate=self.ghg_rate,
        )

    def tearDown(self):
        flush_intervalframe_files()

    def shift_year(self, obj, years=1):
        df = obj.intervalframe.dataframe
        df.index += timedelta(365 * years)
        obj.intervalframe.dataframe = df
        obj.intervalframe.save()
        obj.save()

    def make_request_data(self, cost_functions=None):
        """
        Helper method to create the POST request parameters
        """
        if cost_functions is None:
            cost_functions = {}

        return {
            "cost_functions": cost_functions,
            "name": "test",
            "meter_group_ids": [str(self.meter_group.id)],
            "ders": [
                {
                    "der_configuration_id": str(self.configuration.id),
                    "der_strategy_id": str(self.strategy.id),
                }
            ],
        }

    def test_post_scenario(self):
        """
        Tests that a scenario is run on POST
        """
        self.client.force_authenticate(user=self.user)

        # Create the scenario
        data = self.make_request_data()
        response = self.client.post(self.endpoint, data, format="json")

        # Assert the scenario has been assigned the correct rates
        scenario = Scenario.objects.get(id=response.data["id"])
        self.assertTrue(scenario.has_completed)

    @patch("cost.views.run_scenario")
    def test_post_duplicate_scenario(self, _):
        """
        Test new objects created on POST to /cost/scenario/.
        """
        self.client.force_authenticate(user=self.user)

        # Delete all Scenario objects
        Scenario.objects.all().delete()

        # First request should create a scenario
        data = self.make_request_data()
        response = self.client.post(self.endpoint, data, format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED)
        self.assertEqual(Scenario.objects.count(), 1)

        # Second request should not create a duplicate
        response = self.client.post(self.endpoint, data, format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED)
        self.assertEqual(Scenario.objects.count(), 1)

    def test_scenario_ownership(self):
        """
        Test Scenario only appears for owner of MeterGroup.
        """
        self.client.force_authenticate(user=self.user)

        # 1 Scenario related to MeterGroup
        response = self.client.get(self.endpoint)
        self.assertEqual(len(response.data["results"]["scenarios"]), 1)

        # 0 Scenario
        self.user.meter_groups.clear()
        response = self.client.get(self.endpoint)
        self.assertEqual(len(response.data["results"]["scenarios"]), 0)

    @patch("cost.views.run_scenario")
    def test_scenario_creation_assigns_cost_functions(self, _):
        """
        Tests that cost functions are correctly assigned to the scenario upon
        creation
        """
        self.client.force_authenticate(user=self.user)

        data = self.make_request_data(
            {
                "rate_plan": self.rate_plan.id,
                "ghg_rate": self.ghg_rate.id,
                "procurement_rate": self.procurement_rate.id,
                "system_profile": self.system_profile.id,
            }
        )

        # Create the scenario
        response = self.client.post(self.endpoint, data, format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED)
        scenario = Scenario.objects.get(id=response.data["id"])

        # Assert the scenario has been assigned the correct rates
        self.assertEqual(scenario.rate_plan, self.rate_plan)
        self.assertEqual(scenario.ghg_rate, self.ghg_rate)
        self.assertEqual(scenario.procurement_rate, self.procurement_rate)
        self.assertEqual(scenario.system_profile, self.system_profile)

    @patch("cost.views.run_scenario")
    def test_scenario_creation_validates_year_alignment(self, _):
        """
        Tests that cost functions are correctly assigned to the scenario upon
        creation
        """
        self.client.force_authenticate(user=self.user)

        data = self.make_request_data(
            {
                "rate_plan": self.rate_plan.id,
                "ghg_rate": self.ghg_rate.id,
                "procurement_rate": self.procurement_rate.id,
                "system_profile": self.system_profile.id,
            }
        )

        self.shift_year(self.system_profile, 1)

        # Create the scenario
        response = self.client.post(self.endpoint, data, format="json")
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)

        self.shift_year(self.system_profile, -1)
        data = self.make_request_data(
            {
                "rate_plan": self.rate_plan.id,
                "ghg_rate": self.ghg_rate.id,
                "procurement_rate": self.procurement_rate.id,
                "system_profile": self.system_profile.id,
            }
        )

        self.shift_year(self.procurement_rate, 1)

        # Create the scenario
        response = self.client.post(self.endpoint, data, format="json")
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)

        self.shift_year(self.procurement_rate, -1)

    @patch("cost.views.run_scenario")
    @patch.object(OriginFile, "primary_linked_rate_plan_name", new="ABC")
    def test_scenario_creation_automatic_rate_plan_assignment(self, _):
        """
        Tests that a scenario will be assigned the proper rate plan when "auto"
        is passed
        """
        self.client.force_authenticate(user=self.user)

        # Create the scenario
        data = self.make_request_data({"rate_plan": "auto"})
        response = self.client.post(self.endpoint, data, format="json")
        scenario = Scenario.objects.get(id=response.data["id"])

        # Assert the scenario has been assigned the correct rates
        self.assertEqual(scenario.rate_plan, self.rate_plan)


class CostFunctionTestMixin:
    """
    Tests common cost function viewset functionality
    """

    # Must be overridden in child classes
    url_component = None

    def setUp(self):
        faker = Factory.create()
        self.user = User.objects.create(
            username=faker.user_name(),
            email=faker.email(domain="@terraverde.energy"),
            is_superuser=False,
        )

        self.mce = LoadServingEntity.objects.get(name__icontains="MCE")
        self.pge = LoadServingEntity.objects.get(name__icontains="Pacific Gas")

    def test_retrieve_diff_lse(self):
        """
        Tests that a user cannot retrieve a cost function object if it is within
        an LSE different from the current user's
        """
        cost_fn = self.make_cost_fn(lse=self.mce)
        self.client.force_authenticate(user=self.user)
        uri = f"/v1/cost/{self.url_component}/{cost_fn.id}/"
        response = self.client.get(uri, format="json")
        self.assertEqual(response.status_code, status.HTTP_404_NOT_FOUND)

    def test_retrieve_same_lse(self):
        """
        Tests that a user can retrieve a cost function object within the user's
        LSE
        """
        cost_fn = self.make_cost_fn(lse=self.pge)
        self.client.force_authenticate(user=self.user)
        uri = f"/v1/cost/{self.url_component}/{cost_fn.id}/"
        response = self.client.get(uri, format="json")
        self.assertEqual(response.status_code, status.HTTP_200_OK)

    def test_deletion_diff_lse(self):
        """
        Tests that a user cannot delete a CAISORate object if it is within an
        LSE different from the current user's
        """
        cost_fn = self.make_cost_fn(lse=self.mce)
        self.client.force_authenticate(user=self.user)
        uri = f"/v1/cost/{self.url_component}/{cost_fn.id}/"
        response = self.client.delete(uri, format="json")
        self.assertEqual(response.status_code, status.HTTP_404_NOT_FOUND)

    def test_deletion_same_lse(self):
        """
        Tests that a user can delete a CAISORate object within the user's LSE
        """
        cost_fn = self.make_cost_fn(lse=self.pge)
        self.client.force_authenticate(user=self.user)
        uri = f"/v1/cost/{self.url_component}/{cost_fn.id}/"
        response = self.client.delete(uri, format="json")
        self.assertEqual(response.status_code, status.HTTP_204_NO_CONTENT)

    def make_cost_fn(self, lse: LoadServingEntity = None):
        raise NotImplementedError


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


class TestEndpointsCAISORate(
    APITestCase, CostFunctionTestMixin, BasicAuthenticationTestMixin
):
    """
    Ensures endpoints are only accessible to logged-in users and are rendered
    without errors.
    """

    fixtures = ["reference_model", "caiso_rate"]
    url_component = "caiso_rate"

    def setUp(self):
        """
        Initialize endpoints to test and loads parquet files.
        """
        CostFunctionTestMixin.setUp(self)
        load_intervalframe_files()

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

    def test_retrieve_no_lse(self):
        """
        Tests that a user can retrieve a cost function object if it is a public
        object (i.e., if it has no associated LSE)

        TODO: if rate plans, rate collections and system profiles become
              public objects (i.e. if their `load_serving_entity` field becomes
              nullable) this method should be moved to CostFunctionTestMixin
        """
        cost_fn = self.make_cost_fn()
        self.client.force_authenticate(user=self.user)
        uri = f"/v1/cost/{self.url_component}/{cost_fn.id}/"
        response = self.client.get(uri, format="json")
        self.assertEqual(response.status_code, status.HTTP_200_OK)

    def test_deletion_no_lse(self):
        """
        Tests that a user cannot delete a CAISORate object if it is a public
        object (i.e., if it has no associated LSE)

        TODO: if rate plans, rate collections and system profiles become
              public objects (i.e. if their `load_serving_entity` field becomes
              nullable) this method should be moved to CostFunctionTestMixin
        """
        cost_fn = self.make_cost_fn()
        self.client.force_authenticate(user=self.user)
        uri = f"/v1/cost/{self.url_component}/{cost_fn.id}/"
        response = self.client.delete(uri, format="json")
        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN)

    def make_cost_fn(self, lse: LoadServingEntity = None):
        return CAISORate.create(
            dataframe=pd.DataFrame(), load_serving_entity=lse
        )


class TestEndpointsUtilityRatePlan(
    APITestCase, CostFunctionTestMixin, BasicAuthenticationTestMixin
):
    """
    Ensures endpoints are only accessible to logged-in users and are rendered
    without errors.
    """

    fixtures = ["reference_model", "utility_rate"]
    url_component = "rate_plan"

    def setUp(self):
        """
        Initialize endpoints
        """
        CostFunctionTestMixin.setUp(self)
        self.endpoints = [
            "/v1/cost/rate_plan/?include[]={}".format(related_field)
            for related_field in [
                "rate_collections.*",
                "load_serving_entity.*",
            ]
        ]

    def make_cost_fn(self, lse: LoadServingEntity = None):
        return RatePlan.objects.create(
            load_serving_entity=lse, name="Test rate plan", sector="Residential"
        )


class TestEndpointsUtilityRateCollection(
    APITestCase, CostFunctionTestMixin, BasicAuthenticationTestMixin
):
    """
    Ensures endpoints are only accessible to logged-in users and are rendered
    without errors.
    """

    fixtures = ["reference_model", "utility_rate"]
    url_component = "rate_collection"

    def setUp(self):
        """
        Initialize user and endpoint
        """
        CostFunctionTestMixin.setUp(self)
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

    def make_cost_fn(self, lse: LoadServingEntity = None):
        """
        Returns a RateCollection object associated with the given
        LoadServingEntity
        """
        faker = Factory.create()
        rate_plan = RatePlan.objects.create(
            load_serving_entity=lse, name="Test rate plan", sector="Residential"
        )
        return RateCollection.objects.create(
            effective_date=faker.date(),
            rate_data={"data": None},
            rate_plan=rate_plan,
            utility_url=faker.url(),
        )
