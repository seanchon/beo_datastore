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
from cost.study.models import SingleScenarioStudy, MultipleScenarioStudy
from cost.utility_rate.models import RatePlan
from der.simulation.models import BatteryConfiguration, BatteryStrategy
from reference.reference_model.models import Meter, MeterGroup


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
        self.endpoints = ["/v1/cost/study/"]

        # create MeterGroup
        meter_group = MeterGroup.objects.create()
        meter_group.meters.add(*Meter.objects.all())

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
            end_limit=datetime(2019, 1, 1),
            der_strategy=battery_strategy,
            der_configuration=configuration,
            rate_plan=RatePlan.objects.first(),
        )
        single.ghg_rates.add(*GHGRate.objects.all())

        multi = MultipleScenarioStudy.objects.create()
        multi.single_scenario_studies.add(single)
        multi.run()

    def tearDown(self):
        flush_intervalframe_files()