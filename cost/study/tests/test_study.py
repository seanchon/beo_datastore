from datetime import datetime
import numpy as np

from django.test import TestCase

from beo_datastore.libs.fixtures import (
    flush_intervalframe_files,
    load_intervalframe_files,
)

from cost.ghg.models import GHGRate
from cost.study.models import MultipleScenarioStudy, SingleScenarioStudy
from cost.utility_rate.models import RatePlan
from der.simulation.models import BatteryConfiguration, BatteryStrategy
from load.customer.models import CustomerPopulation, CustomerMeter


class TestStudy(TestCase):
    """
    Test the creation of a DER study, which is a four-step process.
        1. Create and choose k-means clusters (load)
        2. Create and choose battery and strategy (DER)
        3. Create and choose cost functions (cost)
        4. Run study.

    This test ensures that following demo scripts are functional.
        - demo/1_create_kmeans_clusters.ipynb
        - demo/2_create_battery_strategy.ipynb
        - demo/3_multi_scenario_optimization.ipynb
    """

    fixtures = ["reference_model", "customer", "ghg", "utility_rate"]

    def setUp(self):
        """
        Copy parquet (dataframe) files to test MEDIA_ROOT.
        """
        load_intervalframe_files()

    def tearDown(self):
        """
        Remove test MEDIA_ROOT and contents.
        """
        flush_intervalframe_files()

    def test_study(self):
        meters = CustomerMeter.objects.filter(rate_plan_name__contains="EV")

        # 1. Create and choose k-means clusters (load)
        number_of_clusters = 1
        customer_population = CustomerPopulation.generate(
            name="Django Test",
            meters=meters,
            frame288_type="average_frame288",
            number_of_clusters=number_of_clusters,
            normalize=True,
        )
        self.assertEqual(customer_population.meter_count, meters.count())
        self.assertEqual(
            customer_population.customer_clusters.count(), number_of_clusters
        )

        # 2. Create and choose battery and strategy (DER)
        battery_configuration, _ = BatteryConfiguration.objects.get_or_create(
            rating=5, discharge_duration_hours=2, efficiency=0.9
        )

        # minimize bill - charge from grid, no exporting
        rate_plan = RatePlan.objects.get(name__contains="EV")
        frame288 = rate_plan.get_rate_frame288_by_year(
            2018, "energy", "weekday"
        )

        battery_strategy = BatteryStrategy.generate(
            frame288_name="E-19 energy weekday",
            frame288=frame288,
            level=1,
            minimize=True,
            discharge_threshold=0,
        )

        # 3. Create and choose cost functions (cost)
        # 4. Run study.
        multi = MultipleScenarioStudy.objects.create()
        single, _ = SingleScenarioStudy.objects.get_or_create(
            start=datetime(2018, 1, 1),
            end_limit=datetime(2018, 1, 2),
            der_strategy=battery_strategy,
            der_configuration=battery_configuration,
            rate_plan=rate_plan,
            load_serving_entity=meters.first().load_serving_entity,
        )
        single.meter_groups.add(*customer_population.customer_clusters.all())
        single.ghg_rates.add(*GHGRate.objects.filter(name="Clean Net Short"))
        multi.single_scenario_studies.add(single)
        multi.run()

        # all meters found in report
        self.assertEqual(len(multi.report), customer_population.meter_count)
        # battery modifies load
        self.assertNotEqual(np.mean(multi.report["UsageDelta"]), 0)
