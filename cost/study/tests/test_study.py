from datetime import datetime
import numpy as np

from django.test import TestCase

from beo_datastore.libs.fixtures import (
    flush_intervalframe_files,
    load_intervalframe_files,
)

from cost.ghg.models import GHGRate
from cost.procurement.models import CAISORate
from cost.study.models import MultipleScenarioStudy, SingleScenarioStudy
from cost.tasks import run_study
from cost.utility_rate.models import RatePlan
from der.simulation.models import BatteryConfiguration
from der.simulation.scripts.generate_der_strategy import (
    generate_ra_reduction_battery_strategy,
)
from load.customer.models import CustomerPopulation, OriginFile


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

    fixtures = [
        "reference_model",
        "customer",
        "ghg",
        "utility_rate",
        "caiso_rate",
    ]

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
        meter_group = OriginFile.objects.first()

        # 1. Create and choose k-means clusters (load)
        number_of_clusters = 1
        customer_population = CustomerPopulation.objects.create(
            name="Django Test",
            frame288_type="average_frame288",
            number_of_clusters=number_of_clusters,
            normalize=True,
            meter_group=meter_group,
        )
        customer_population.generate()
        self.assertEqual(
            customer_population.meter_count, meter_group.meters.count()
        )
        self.assertEqual(
            customer_population.customer_clusters.count(), number_of_clusters
        )

        # 2. Create and choose battery and strategy (DER)
        battery_configuration, _ = BatteryConfiguration.objects.get_or_create(
            rating=5, discharge_duration_hours=2, efficiency=0.9
        )

        # minimize RA (use meter as proxy for system profile)
        # charge from grid, no exporting
        battery_strategy = generate_ra_reduction_battery_strategy(
            name="2018 System Load",
            charge_grid=True,
            discharge_grid=False,
            system_profile=meter_group.meters.first(),
        )

        # 3. Create and choose cost functions (cost)
        # 4. Run study.
        multi = MultipleScenarioStudy.objects.create()
        single, _ = SingleScenarioStudy.objects.get_or_create(
            start=datetime(2018, 1, 1),
            end_limit=datetime(2018, 1, 2),
            der_strategy=battery_strategy,
            der_configuration=battery_configuration,
            meter_group=customer_population.customer_clusters.first(),
            rate_plan=RatePlan.objects.get(name__contains="EV"),
        )
        single.ghg_rates.add(*GHGRate.objects.filter(name="Clean Net Short"))
        single.caiso_rates.add(*CAISORate.objects.all())
        multi.single_scenario_studies.add(single)
        run_study(multi.id)

        # all meters found in report
        self.assertEqual(
            len(multi.report),
            customer_population.customer_clusters.first().meters.count(),
        )
        # index is named "ID"
        self.assertEqual(multi.report.index.name, "ID")
        self.assertEqual(multi.report_summary.index.name, "ID")
        # battery modifies load
        self.assertNotEqual(np.mean(multi.report["UsageDelta"]), 0)
        # report columns all exist
        self.assertEqual(
            set(multi.report.columns),
            {
                "UsagePreDER",
                "UsagePostDER",
                "UsageDelta",
                "BillRevenuePreDER",
                "BillRevenuePostDER",
                "BillRevenueDelta",
                "CleanNetShort2018PreDER",
                "CleanNetShort2018PostDER",
                "CleanNetShort2018Delta",
                "CleanNetShort2022PreDER",
                "CleanNetShort2022PostDER",
                "CleanNetShort2022Delta",
                "CleanNetShort2026PreDER",
                "CleanNetShort2026PostDER",
                "CleanNetShort2026Delta",
                "CleanNetShort2030PreDER",
                "CleanNetShort2030PostDER",
                "CleanNetShort2030Delta",
                "ExpensePreDER",
                "ExpensePostDER",
                "ExpenseDelta",
                "PRC_LMP2018PreDER",
                "PRC_LMP2018PostDER",
                "PRC_LMP2018Delta",
                "ProfitPreDER",
                "ProfitPostDER",
                "ProfitDelta",
                "SA ID",
                "MeterRatePlan",
                "SingleScenarioStudy",
            },
        )
