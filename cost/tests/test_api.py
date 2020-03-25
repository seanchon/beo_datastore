from datetime import datetime
from faker import Factory

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

        # test following endpoints using BasicAuthenticationTestMixin
        self.endpoints = [
            "/v1/cost/study/"
            + "?include[]=ders"
            + "&include[]=der_simulations"
            + "?include[]=meters"
            + "?include[]=meter_groups"
            + "?include[]=metadata"
            + "?include[]=report"
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
        frame288 = rate_plan.get_rate_frame288_by_year(
            2018, "energy", "weekday"
        )

        battery_strategy = BatteryStrategy.generate(
            frame288_name="rate plan energy weekday",
            frame288=frame288,
            level=1,  # rate plans typically have only a few different rate levels per month, so this value is typically 1
            minimize=True,  # objective is to minimize bill
            discharge_threshold=0,  # only discharge down to 0kW
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
        self.assertEqual(len(response.data["studies"]), 2)

        # 0 SingleScenarioStudy, 0 MultipleScenarioStudy
        self.user.meter_groups.clear()
        response = self.client.get(get_endpoint)
        self.assertEqual(len(response.data["studies"]), 0)
