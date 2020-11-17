from datetime import datetime
import numpy as np
from pandas.testing import assert_frame_equal
from unittest import mock

from django.test import TestCase

from navigader_core.cost.controller import AggregateResourceAdequacyCalculation
from navigader_core.der.builder import AggregateDERProduct, DERProduct
from navigader_core.der.solar import (
    SolarPV as pySolarPV,
    SolarPVStrategy as pySolarPVStrategy,
)
from navigader_core.tests.mock_response import mocked_pvwatts_requests_get

from beo_datastore.libs.fixtures import (
    flush_intervalframe_files,
    load_intervalframe_files,
)

from cost.ghg.models import GHGRate
from cost.procurement.models import CAISORate, SystemProfile
from cost.study.models import Scenario
from cost.tasks import run_scenario
from cost.utility_rate.models import RatePlan
from der.simulation.models import (
    BatteryConfiguration,
    SolarPVConfiguration,
    SolarPVStrategy,
)
from der.simulation.scripts.generate_der_strategy import (
    generate_ra_reduction_battery_strategy,
)
from load.customer.models import CustomerPopulation, OriginFile


# SolarPV Configuration
ARRAY_TYPE = 0
AZIMUTH = 180
ADDRESS = 94518
TILT = 20

# SolarPVStrategy
SERVICEABLE_LOAD_RATIO = 0.85

# Scenario Report
SCENARIO_REPORT_COLUMNS = {
    "UsagePreDER",
    "UsagePostDER",
    "UsageDelta",
    "BillRevenuePreDER",
    "BillRevenuePostDER",
    "BillRevenueDelta",
    "GHGPreDER",
    "GHGPostDER",
    "GHGDelta",
    "ExpensePreDER",
    "ExpensePostDER",
    "ExpenseDelta",
    "ProcurementCostPreDER",
    "ProcurementCostPostDER",
    "ProcurementCostDelta",
    "ProfitPreDER",
    "ProfitPostDER",
    "ProfitDelta",
    "RAPostDER",
    "RACostPreDER",
    "RADelta",
    "RACostDelta",
    "RAPreDER",
    "RACostPostDER",
    "SA ID",
    "MeterRatePlan",
    "ScenarioID",
}


class TestScenario(TestCase):
    """
    Test the creation of a DER scenario, which is a four-step process.
        1. Create and choose k-means clusters (load)
        2. Create and choose battery and strategy (DER)
        3. Create and choose cost functions (cost)
        4. Run scenario.
    """

    fixtures = [
        "reference_model",
        "customer",
        "ghg",
        "utility_rate",
        "caiso_rate",
        "system_profile",
    ]

    @mock.patch("requests.get", side_effect=mocked_pvwatts_requests_get)
    def setUp(self, mock_get):
        """
        Copy parquet (dataframe) files to test MEDIA_ROOT.
        """
        load_intervalframe_files()

        self.meter_group = OriginFile.objects.first()

        # Create and choose k-means clusters
        number_of_clusters = 1
        self.customer_population = CustomerPopulation.objects.create(
            name="Django Test",
            frame288_type="average_frame288",
            number_of_clusters=number_of_clusters,
            normalize=True,
            meter_group=self.meter_group,
        )
        self.customer_population.generate()
        self.assertEqual(
            self.customer_population.meter_count,
            self.meter_group.meters.count(),
        )
        self.assertEqual(
            self.customer_population.customer_clusters.count(),
            number_of_clusters,
        )

        # Create BatteryConfiguration and BatteryStrategy
        (
            self.battery_configuration,
            _,
        ) = BatteryConfiguration.objects.get_or_create(
            rating=5, discharge_duration_hours=2, efficiency=0.9
        )
        self.battery_strategy = generate_ra_reduction_battery_strategy(
            name="2018 System Load",
            charge_grid=True,
            discharge_grid=False,
            system_profile=self.meter_group.meters.first(),
        )

        # Create SolarPVConfiguration and SolarPVStrategy
        (
            self.solar_pv_configuration,
            _,
        ) = SolarPVConfiguration.get_or_create_from_object(
            solar_pv=pySolarPV(
                api_key="ABCDEFG",
                array_type=ARRAY_TYPE,
                azimuth=AZIMUTH,
                address=ADDRESS,
                tilt=TILT,
            )
        )
        self.solar_pv_strategy, _ = SolarPVStrategy.get_or_create_from_object(
            solar_pv_strategy=pySolarPVStrategy(
                serviceable_load_ratio=SERVICEABLE_LOAD_RATIO
            )
        )

    def tearDown(self):
        """
        Remove test MEDIA_ROOT and contents.
        """
        flush_intervalframe_files()

    def create_and_run_scenario(
        self, meter_group, der_strategy, der_configuration, stacked
    ) -> Scenario:
        """
        Create and run scenario with preset configuration.
        """
        scenario, _ = Scenario.objects.get_or_create(
            start=datetime(2018, 6, 1),
            end_limit=datetime(2018, 6, 2),
            der_strategy=der_strategy,
            der_configuration=der_configuration,
            meter_group=meter_group,
            rate_plan=RatePlan.objects.get(name__contains="EV"),
            stacked=stacked,
        )
        scenario.ghg_rate = GHGRate.objects.first()
        scenario.procurement_rate = CAISORate.objects.first()
        scenario.system_profile = SystemProfile.objects.first()
        scenario.save()
        run_scenario(scenario.id)

        return Scenario.objects.get(id=scenario.id)

    def test_scenario(self):
        scenario = self.create_and_run_scenario(
            meter_group=self.customer_population.customer_clusters.first(),
            der_configuration=self.battery_configuration,
            der_strategy=self.battery_strategy,
            stacked=False,
        )

        # all meters found in report
        self.assertEqual(
            len(scenario.report),
            self.customer_population.customer_clusters.first().meters.count(),
        )
        # index is named "ID"
        self.assertEqual(scenario.report.index.name, "ID")
        self.assertEqual(scenario.report_summary.index.name, "ID")
        # battery modifies load
        self.assertNotEqual(np.mean(scenario.report["UsageDelta"]), 0)
        # report columns all exist
        self.assertEqual(set(scenario.report.columns), SCENARIO_REPORT_COLUMNS)

    @mock.patch("requests.get", side_effect=mocked_pvwatts_requests_get)
    def test_stacked_scenario(self, mock_get):
        solar_scenario = self.create_and_run_scenario(
            meter_group=self.meter_group,
            der_configuration=self.solar_pv_configuration,
            der_strategy=self.solar_pv_strategy,
            stacked=False,
        )

        battery_scenario = self.create_and_run_scenario(
            meter_group=solar_scenario,
            der_configuration=self.battery_configuration,
            der_strategy=self.battery_strategy,
            stacked=False,
        )

        stacked_scenario = self.create_and_run_scenario(
            meter_group=solar_scenario,
            der_configuration=self.battery_configuration,
            der_strategy=self.battery_strategy,
            stacked=True,
        )

        for scenario in [solar_scenario, battery_scenario, stacked_scenario]:
            # all meters found in report
            self.assertEqual(
                len(scenario.report),
                self.customer_population.customer_clusters.first().meters.count(),
            )
            # DER modifies load
            self.assertNotEqual(np.mean(scenario.report["UsageDelta"]), 0)
            # report columns all exist
            self.assertEqual(
                set(scenario.report.columns), SCENARIO_REPORT_COLUMNS
            )

        # check stacked_scenario against solar_scenario and battery_scenario
        self.assertEqual(
            stacked_scenario.pre_der_intervalframe,
            solar_scenario.pre_der_intervalframe,
        )
        assert_frame_equal(
            stacked_scenario.post_der_intervalframe.dataframe,
            battery_scenario.post_der_intervalframe.dataframe,
        )
        stacked_der_intervalframe = (
            solar_scenario.der_intervalframe
            + battery_scenario.der_intervalframe
        )
        assert_frame_equal(
            stacked_scenario.der_intervalframe.dataframe,
            stacked_der_intervalframe.dataframe,
        )

        # compare report_summary deltas based on energy calculations
        # solar + battery energy deltas should equal stacked energy deltas
        energy_calculation_rows = [
            "BillRevenueDelta",
            "GHGDelta",
            "ProcurementCostDelta",
            "UsageDelta",
        ]
        solar_deltas = solar_scenario.report_summary[
            solar_scenario.report_summary.isin(energy_calculation_rows)
        ]
        battery_deltas = battery_scenario.report_summary[
            battery_scenario.report_summary.isin(energy_calculation_rows)
        ]
        stacked_deltas = stacked_scenario.report_summary[
            stacked_scenario.report_summary.isin(energy_calculation_rows)
        ]
        assert_frame_equal(solar_deltas + battery_deltas, stacked_deltas)

        # compare stacked aggregate RA calculation to aggregate RA calculation
        # across solar and battery
        der_product = DERProduct(
            der=battery_scenario.der_configuration.der,
            der_strategy=battery_scenario.der_strategy.der_strategy,
            pre_der_intervalframe=solar_scenario.pre_der_intervalframe,
            der_intervalframe=(
                solar_scenario.der_intervalframe
                + battery_scenario.der_intervalframe
            ),
            post_der_intervalframe=battery_scenario.post_der_intervalframe,
        )
        agg_simulation = AggregateDERProduct(
            der_products={battery_scenario.id: der_product}
        )
        agg_ra_calculation = AggregateResourceAdequacyCalculation(
            agg_simulation=agg_simulation,
            rate_data=battery_scenario.system_profile.intervalframe,
        )
        ra_delta = stacked_scenario.report_summary[
            stacked_scenario.report_summary.index == "RADelta"
        ]["0"].values[0]
        self.assertAlmostEqual(ra_delta, agg_ra_calculation.net_impact, 6)
