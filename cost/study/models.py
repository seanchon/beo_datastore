from collections import namedtuple
from functools import reduce
import json
from jsonfield import JSONField
import os
import pandas as pd
from pathlib import Path
import re

from django.core.exceptions import ValidationError
from django.db import models
from django.db.models.functions import Coalesce
from django.utils.functional import cached_property

from navigader_core.der.builder import AggregateDERProduct, DERProduct
from navigader_core.cost.controller import AggregateResourceAdequacyCalculation
from navigader_core.load.dataframe import add_interval_dataframe
from navigader_core.load.intervalframe import PowerIntervalFrame

from beo_datastore.libs.intervalframe_file import PowerIntervalFrameFile
from beo_datastore.libs.models import IntervalFrameFileMixin, nested_getattr
from beo_datastore.libs.views import dataframe_to_html
from beo_datastore.settings import MEDIA_ROOT

from cost.ghg.models import GHGRate, StoredGHGCalculation
from cost.procurement.models import (
    CAISORate,
    SystemProfile,
    StoredProcurementCostCalculation,
    StoredResourceAdequacyCalculation,
)
from cost.utility_rate.models import RatePlan, StoredBillCalculation
from der.simulation.models import (
    EVSESimulation,
    SolarPVSimulation,
    StoredBatterySimulation,
)
from load.customer.models import OriginFile
from load.tasks import aggregate_meter_group_intervalframes
from reference.reference_model.models import (
    DERConfiguration,
    DERStrategy,
    DERSimulation,
    Meter,
    MeterGroup,
)
from reference.auth_user.models import LoadServingEntity


class ScenarioIntervalFrame(PowerIntervalFrameFile):
    """
    Model for storing Scenario IntervalFrameFiles, which have timestamps and
    aggregate meter readings.
    """

    # directory for parquet file storage
    file_directory = os.path.join(MEDIA_ROOT, "scenario")


class Scenario(IntervalFrameFileMixin, MeterGroup):
    """
    Container for DERSimulations.

    Steps to create a scenario:
    1. Create Scenario object
    2. Add MeterGroups, GHGRates, and SystemProfiles
    3. run() to generate all simulations
    4. display reports
    """

    start = models.DateTimeField()
    end_limit = models.DateTimeField()
    der_strategy = models.ForeignKey(
        to=DERStrategy, related_name="scenarios", on_delete=models.CASCADE
    )
    der_configuration = models.ForeignKey(
        to=DERConfiguration, related_name="scenarios", on_delete=models.CASCADE
    )
    meter_group = models.ForeignKey(
        to=MeterGroup, related_name="scenarios", on_delete=models.CASCADE
    )
    # Constrains Meters and RatePlan to belong to LSE. If null is True, any
    # Meter and RatePlan can be used in optimization.
    load_serving_entity = models.ForeignKey(
        to=LoadServingEntity,
        related_name="scenarios",
        on_delete=models.CASCADE,
        blank=True,
        null=True,
    )
    rate_plan = models.ForeignKey(
        to=RatePlan,
        related_name="scenarios",
        on_delete=models.SET_NULL,
        blank=True,
        null=True,
    )
    ghg_rate = models.ForeignKey(
        to=GHGRate,
        related_name="scenarios",
        on_delete=models.SET_NULL,
        blank=True,
        null=True,
    )
    system_profile = models.ForeignKey(
        to=SystemProfile,
        related_name="scenarios",
        on_delete=models.SET_NULL,
        blank=True,
        null=True,
    )
    procurement_rate = models.ForeignKey(
        to=CAISORate,
        related_name="scenarios",
        on_delete=models.SET_NULL,
        blank=True,
        null=True,
    )
    stacked = models.BooleanField(default=True)
    _report = JSONField(blank=True, null=True, default={})
    _report_summary = JSONField(blank=True, null=True, default={})

    # Required by IntervalFrameFileMixin.
    frame_file_class = ScenarioIntervalFrame

    # Exclude fields from __repr__
    repr_exclude_fields = ["_report", "_report_summary"]

    class Meta:
        ordering = ["-created_at"]

    @staticmethod
    def standardize_delta_df(df: pd.DataFrame):
        """
        Takes a dataframe with "AbcPreDER", "AbcPostDER", and "AbcDelta" columns
        and returns a the same dataframe with columns renamed to  "pre", "post"
        and "delta". This is particularly useful for combining scenario
        reporting fields

        Ex.
            SingleScenarioStudy.standardize_delta_df(pd.DataFrame(
                [[0, 1, 2], [3, 4, 5]],
                columns=["ExpensePreDER", "ExpensePostDER", "ExpenseDelta"]
            ))

            >>  index  |  pre  |  post  |  delta
                    0  |    0  |     1  |      2
                    1  |    3  |     4  |      5

        :param df: dataframe to standardize
        """
        # get the "Abc" prefix
        prefix = None
        for column in df.columns:
            match = re.match(r"(.*)PreDER", column)
            if match:
                prefix = match.group(1)
                break

        # dataframe doesn't have correct columns
        if prefix is None:
            return pd.DataFrame(columns=["pre", "post", "delta"])

        return df.rename(
            columns={
                "{}PreDER".format(prefix): "pre",
                "{}PostDER".format(prefix): "post",
                "{}Delta".format(prefix): "delta",
            }
        )

    def clean(self, *args, **kwargs):
        if (
            self.rate_plan
            and self.load_serving_entity
            and self.rate_plan not in self.load_serving_entity.rate_plans.all()
        ):
            raise ValidationError(
                "RatePlan assignment is limited by those belonging to the "
                "LoadServingEntity."
            )

        if self.der_strategy and (
            self.der_configuration.der_type != self.der_strategy.der_type
        ):
            raise ValidationError(
                "der_configuration.der_type must match der_strategy.der_type"
            )

        super().clean(*args, **kwargs)

    @property
    def expected_der_simulation_count(self):
        """
        Number of expected DERSimulation objects with regards to any applied
        filter_by_query() or filter_by_transform() operations.
        """
        return self.meter_group.meters.count()

    @property
    def der_simulation_count(self):
        """
        Number of created DERSimulation objects.
        """
        return self.der_simulations.count()

    @property
    def simulations_completed(self):
        """
        True if all DERSimulations have completed.
        """
        return self.der_simulation_count == self.expected_der_simulation_count

    @property
    def expected_cost_calculation_count(self):
        """
        Number of expected cost calculations based on the number of
        cost-function rates muliplied by the number of DER simulations.
        """
        cost_fns = [
            self.rate_plan,
            self.ghg_rate,
            self.system_profile,
            self.procurement_rate,
        ]

        num_cost_fns = sum(map(bool, cost_fns))
        return num_cost_fns * self.expected_der_simulation_count

    @property
    def cost_calculation_count(self):
        """
        Number of completed cost calculations.
        """
        cost_calculation_count = self.bill_calculations.count()
        cost_calculation_count += self.ghg_calculations.count()
        cost_calculation_count += self.resource_adequacy_calculations.count()
        cost_calculation_count += self.procurement_calculations.count()

        return cost_calculation_count

    @property
    def cost_calculations_completed(self):
        """
        Return True if all cost calculations have completed.
        """
        return (
            self.expected_cost_calculation_count == self.cost_calculation_count
        )

    @property
    def has_completed(self):
        """
        True if all DERSimulations have been completed,
        self.meter_intervalframe has been aggregated, and reports generated.
        """
        return (
            self.simulations_completed
            and self.cost_calculations_completed
            and not self.meter_intervalframe.dataframe.empty
            and not self.report.empty
            and not self.report_summary.empty
        )

    @property
    def meter_intervalframe(self):
        """
        Cached PowerIntervalFrame representing entire self.meter_group
        buildings' load after running many DERSimulations.
        """
        return self.intervalframe

    @property
    def pre_der_intervalframe(self):
        """
        PowerIntervalFrame representing the associated MeterGroup's
        meter_intervalframe.
        """
        if self.stacked and isinstance(self.meter_group, Scenario):
            return self.meter_group.pre_der_intervalframe
        else:
            return self.meter_group.meter_intervalframe

    @property
    def der_intervalframe(self):
        """
        PowerIntervalFrame representing all associated DERSimulation
        PowerIntervalFrames combined.
        """
        inverse_pre_der_dataframe = self.pre_der_intervalframe.dataframe * -1

        return PowerIntervalFrame(
            dataframe=add_interval_dataframe(
                self.meter_intervalframe.dataframe, inverse_pre_der_dataframe
            )
        )

    @property
    def post_der_intervalframe(self):
        """
        PowerIntervalFrame representing aggregate readings of all meters
        after running DER simulations.
        """
        return self.intervalframe

    @property
    def meter_groups(self):
        """
        QuerySet alias of related MeterGroup.
        """
        return MeterGroup.objects.filter(id=self.meter_group.id)

    @property
    def ders(self):
        """
        Return list of dicts corresponding to the DERConfiguration objects and
        DERStrategy objects contained within.

        Ex.
        [
            {
                "der_configuration": <DERConfiguration>,
                "der_strategy": <DERStrategy>
            }
        ]
        """
        return [
            {
                "der_configuration": self.der_configuration,
                "der_strategy": self.der_strategy,
            }
        ]

    @property
    def meters(self):
        """
        Since a Scenario is also a MeterGroup, it's meters should be the same
        as its der_simulations for subsequent Scenarios.
        """
        return self.der_simulations

    @cached_property
    def der_simulations(self):
        """
        Associated DERSimulation queryset.
        """
        return DERSimulation.objects.filter(
            meter__in=self.meter_group.meters.all(),
            start=self.start,
            end_limit=self.end_limit,
            der_configuration=self.der_configuration,
            der_strategy=self.der_strategy,
        )

    @property
    def report(self):
        """
        Report containing meter SA IDs, DERConfiguration details, RatePlan
        details, and all cost impacts.
        """
        report = pd.DataFrame(self._report)
        report.index.names = ["ID"]  # rename index to "ID"
        return report

    @property
    def exportable_report(self):
        """
        Organizes the report summary for exporting
        """
        df = self.report
        df["Scenario Name"] = self.name
        first_cols = ["Scenario Name", "SA ID"]
        columns = first_cols + [c for c in df.columns if c not in first_cols]
        return df[columns]

    @property
    def report_summary(self):
        """
        Report summary containing RA and non-RA totals for each column of
        self.report.
        """
        report_summary = pd.DataFrame(self._report_summary)
        report_summary.index.names = ["ID"]  # rename index to "ID"
        return report_summary

    @property
    def exportable_report_summary(self):
        """
        Organizes the report summary for exporting
        """
        df = self.report_summary.transpose()
        df["Scenario Name"] = self.name
        first_cols = ["Scenario Name"]
        columns = first_cols + [c for c in df.columns if c not in first_cols]
        return df[columns]

    def generate_reports(self):
        """
        Get and store report only if all DERSimulations have run and
        meter_intervalframe has been aggregated.
        """
        if (
            self.simulations_completed
            and self.cost_calculations_completed
            and not self.meter_intervalframe.dataframe.empty
        ):
            with self.lock():
                self._report = json.loads(self.get_report().to_json())
                self._report_summary = json.loads(
                    self.get_report_summary().to_json()
                )
                self.save()

    def get_report(self):
        """
        All usage and cost reports stitched into a single report.
        """
        report = (
            self.usage_report.join(self.revenue_report, how="outer")
            .join(self.expense_report, how="outer")
            .join(self.profit_report, how="outer")
            .join(self.ghg_report, how="outer")
            .join(self.resource_adequacy_report, how="outer")
            .join(self.procurement_report, how="outer")
            .join(self.meter_report, how="outer")
        )
        report["ScenarioID"] = str(self.id)
        report.index = report.index.astype(str)

        return report

    def get_report_summary(self):
        """
        All usage and cost report summaries stitched into a single report
        summary.
        """
        return (
            self.linear_report_summary()
            .append(self.ra_report_summary)
            .append(self.expense_report_summary)
            .append(self.profit_report_summary)
        )

    def linear_report_summary(self, columns=None):
        """
        pandas DataFrame with totals for all values of the report that can be
        computed by linearly adding the corresponding values for each
        individual DERSimulation

        :param columns: the columns to include in the summary. If not provided,
          defaults to all columns with the text "PreDER", "PostDER" or "Delta"
          in their names
        """
        report = self.report

        # exclude nonlinear columns
        nonlinear_columns = [
            "ExpensePreDER",
            "ExpensePostDER",
            "ExpenseDelta",
            "ProfitPreDER",
            "ProfitPostDER",
            "ProfitDelta",
            "RAPreDER",
            "RAPostDER",
            "RADelta",
            "RACostPreDER",
            "RACostPostDER",
            "RACostDelta",
        ]

        if columns is None:
            columns = [
                x
                for x in report.columns
                if ("PreDER" in x or "PostDER" in x or "Delta" in x)
                and x not in nonlinear_columns
            ]

        return pd.DataFrame(self.report[columns].sum())

    @cached_property
    def ra_report_summary(self):
        """
        pandas DataFrame with RA totals for each column of report.
        Filtering disabled.
        """
        ra_calculation = self.agg_ra_calculation
        if ra_calculation is None:
            return pd.DataFrame()

        return pd.DataFrame(
            {
                "RAPreDER": [ra_calculation.pre_DER_total],
                "RAPostDER": [ra_calculation.post_DER_total],
                "RADelta": [ra_calculation.net_impact],
                "RACostPreDER": [ra_calculation.pre_DER_total_cost],
                "RACostPostDER": [ra_calculation.post_DER_total_cost],
                "RACostDelta": [ra_calculation.net_impact_cost],
            }
        ).transpose()

    @cached_property
    def expense_report_summary(self):
        """
        Return pandas DataFrame with expenses totals
        """
        ra_calculation = self.agg_ra_calculation
        if ra_calculation is None:
            return pd.DataFrame()

        procurement_aggregations = self.procurement_calculations.aggregate(
            pre_der=Coalesce(models.Sum("pre_DER_total"), 0),
            post_der=Coalesce(models.Sum("post_DER_total"), 0),
        )

        ra_cost_pre = ra_calculation.pre_DER_total_cost
        ra_cost_post = ra_calculation.post_DER_total_cost
        expenses_pre = ra_cost_pre + procurement_aggregations["pre_der"]
        expenses_post = ra_cost_post + procurement_aggregations["post_der"]

        return pd.DataFrame(
            {
                "ExpensePreDER": [expenses_pre],
                "ExpensePostDER": [expenses_post],
                "ExpenseDelta": [expenses_post - expenses_pre],
            }
        ).transpose()

    @cached_property
    def profit_report_summary(self):
        """
        Return pandas dataframe with profits, calculated as revenues - expenses
        """
        expenses = self.standardize_delta_df(
            self.expense_report_summary.transpose()
        )

        missing_expenses = expenses.empty
        missing_revenue = "BillRevenueDelta" not in self.report.columns

        # If there's no revenue info or no expense info, return an empty frame
        if missing_expenses or missing_revenue:
            return pd.DataFrame()

        revenues = self.standardize_delta_df(
            self.linear_report_summary(
                ["BillRevenuePreDER", "BillRevenuePostDER", "BillRevenueDelta"]
            ).transpose()
        )

        return (
            revenues.add(expenses.multiply(-1), fill_value=0)
            .rename(
                columns={
                    "pre": "ProfitPreDER",
                    "post": "ProfitPostDER",
                    "delta": "ProfitDelta",
                }
            )
            .transpose()
        )

    @cached_property
    def agg_simulation(self):
        """
        Return AggregateDERProduct equivalent of self.

        AggregateDERProduct with the same parameters can be added to
        one another and can be used for aggregate "cost calculations" found in
        beo_datastore/libs/controller.py.
        """
        return AggregateDERProduct(der_products={self.id: self.der_product})

    @cached_property
    def agg_ra_calculation(self):
        """
        Return AggregateResourceAdequacyCalculation equivalent of self, or None
        if no system profile is associated with the study
        """
        if not self.system_profile:
            return None

        # compute RA on all meters combined
        return AggregateResourceAdequacyCalculation(
            agg_simulation=self.agg_simulation,
            rate_data=self.system_profile.intervalframe,
        )

    @cached_property
    def der_product(self):
        """
        Return DERProduct equivalent of self.
        """
        return DERProduct(
            der=self.der_configuration.der,
            der_strategy=self.der_strategy.der_strategy,
            pre_der_intervalframe=self.pre_der_intervalframe,
            der_intervalframe=self.der_intervalframe,
            post_der_intervalframe=self.post_der_intervalframe,
        )

    @property
    def report_html_table(self):
        """
        Return Django-formatted HTML detailed report.
        """
        return dataframe_to_html(self.report)

    @property
    def bill_calculations(self):
        """
        Return StoredBillCalculations related to self.
        """
        return StoredBillCalculation.objects.select_related(
            "der_simulation__meter"
        ).filter(
            der_simulation__in=self.der_simulations,
            rate_plan=self.rate_plan,
            stacked=self.stacked,
        )

    @property
    def ghg_calculations(self):
        """"
        Return StoredGHGCalculations related to self.
        """
        return StoredGHGCalculation.objects.select_related(
            "der_simulation__meter"
        ).filter(
            der_simulation__in=self.der_simulations,
            ghg_rate=self.ghg_rate,
            stacked=self.stacked,
        )

    @property
    def resource_adequacy_calculations(self):
        """
        Return StoredResourceAdequacyCalculations related to self.
        """
        return StoredResourceAdequacyCalculation.objects.select_related(
            "der_simulation__meter"
        ).filter(
            der_simulation__in=self.der_simulations,
            system_profile=self.system_profile,
            stacked=self.stacked,
        )

    @property
    def procurement_calculations(self):
        """
        Return StoredProcurementCostCalculations related to self.
        """
        return StoredProcurementCostCalculation.objects.select_related(
            "der_simulation__meter"
        ).filter(
            der_simulation__in=self.der_simulations,
            caiso_rate=self.procurement_rate,
            stacked=self.stacked,
        )

    @property
    def usage_report(self):
        """
        Return pandas DataFrame with meter SA IDs and usage deltas.
        """
        if self.stacked:
            return DERSimulation.get_report(
                (x.stacked_der_simulation for x in self.der_simulations)
            )
        else:
            return DERSimulation.get_report(self.der_simulations)

    @cached_property
    def revenue_report(self):
        """
        Return pandas DataFrame with meter SA IDs and bill deltas. This is
        cached because it's reused in the `profits_report`
        """
        return StoredBillCalculation.get_report(self.bill_calculations)

    @cached_property
    def expense_report(self):
        """
        Return pandas DataFrame with meter SA IDs and expenses, defined as RA
        expenses plus procurement cost expenses. This is cached because it's
        reused in the `profits_report`
        """
        Expense = namedtuple("Expense", ["pre", "post", "delta"])
        default_expense = Expense(0, 0, 0)

        ra_expenses = {
            x.der_simulation.meter.id: Expense(
                x.pre_der_total_cost, x.post_der_total_cost, x.net_impact_cost
            )
            for x in self.resource_adequacy_calculations
        }

        procurement_expenses = {
            x.der_simulation.meter.id: Expense(
                x.pre_DER_total, x.post_DER_total, x.net_impact
            )
            for x in self.procurement_calculations
        }

        df_rows = []
        for meter_id in self.meter_group.meters.values_list("id", flat=True):
            ra_expense_missing = meter_id not in ra_expenses
            procurement_expense_missing = meter_id not in procurement_expenses

            # if we don't have either an RA calculation or a procurement
            # calculation then we have no data at all for the meter, so continue
            if ra_expense_missing and procurement_expense_missing:
                continue

            # otherwise retrieve the data for each expense category
            ra_expense = ra_expenses.get(meter_id, default_expense)
            procurement_expense = procurement_expenses.get(
                meter_id, default_expense
            )

            df_rows.append(
                [
                    meter_id,
                    procurement_expense.pre + ra_expense.pre,
                    procurement_expense.post + ra_expense.post,
                    procurement_expense.delta + ra_expense.delta,
                ]
            )

        return pd.DataFrame(
            df_rows,
            columns=["ID", "ExpensePreDER", "ExpensePostDER", "ExpenseDelta"],
        ).set_index("ID")

    @property
    def profit_report(self):
        """
        Return pandas DataFrame with meter SA IDs and profits, defined as
        revenues minus expenses.
        """
        revenue_report = self.standardize_delta_df(self.revenue_report)
        expense_report = self.standardize_delta_df(self.expense_report)
        return revenue_report.add(
            expense_report.multiply(-1), fill_value=0
        ).rename(
            columns={
                "pre": "ProfitPreDER",
                "post": "ProfitPostDER",
                "delta": "ProfitDelta",
            }
        )

    @property
    def ghg_report(self):
        """
        Return pandas DataFrame with meter SA IDs and GHG deltas from all
        associated GHGRates.
        """
        return StoredGHGCalculation.get_report(self.ghg_calculations)

    @property
    def resource_adequacy_report(self):
        """
        Return pandas DataFrame with meter SA IDs and RA deltas from all
        associated GHGRates.
        """
        return StoredResourceAdequacyCalculation.get_report(
            self.resource_adequacy_calculations
        )

    @property
    def procurement_report(self):
        """
        Return pandas DataFrame with meter SA IDs and procurement deltas from
        all associated CAISORates.
        """
        return StoredProcurementCostCalculation.get_report(
            self.procurement_calculations
        )

    @property
    def meter_report(self):
        """
        Return pandas DataFrame with Meter SA IDs and RatePlans.
        """
        return Meter.get_report(
            Meter.objects.filter(
                id__in=self.meter_group.meters.values_list("id")
            ),
            column_map={"sa_id": "SA ID", "rate_plan_name": "MeterRatePlan"},
        )

    @property
    def der_simulation_class(self):
        """
        Returns the DERSimulation class to use with the scenario's DER
        """
        der_type = self.der_configuration.der_type
        if der_type == "Battery":
            return StoredBatterySimulation
        elif der_type == "EVSE":
            return EVSESimulation
        elif der_type == "SolarPV":
            return SolarPVSimulation
        else:
            raise RuntimeError(
                "DERConfiguration has unrecognized der_type: {}".format(
                    der_type
                )
            )

    def run_single_meter_simulation_and_cost(self, meter):
        """
        Run a single Meter's DERSimulation and cost calculations.
        """
        der_simulation_set = self.der_simulation_class.generate(
            der_configuration=self.der_configuration,
            der_strategy=self.der_strategy,
            start=self.start,
            end_limit=self.end_limit,
            meter_set={meter},
        )

        if self.rate_plan:
            StoredBillCalculation.generate(
                der_simulation_set=der_simulation_set,
                rate_plan=self.rate_plan,
                stacked=self.stacked,
            )

        if self.ghg_rate:
            StoredGHGCalculation.generate(
                der_simulation_set=der_simulation_set,
                ghg_rate=self.ghg_rate,
                stacked=self.stacked,
            )

        if self.system_profile:
            StoredResourceAdequacyCalculation.generate(
                der_simulation_set=der_simulation_set,
                system_profile=self.system_profile,
                stacked=self.stacked,
            )

        if self.procurement_rate:
            StoredProcurementCostCalculation.generate(
                der_simulation_set=der_simulation_set,
                caiso_rate=self.procurement_rate,
                stacked=self.stacked,
            )

    def run(self):
        """
        Run related DERSimulations, StoredBillCalculations and
        StoredGHGCalculations.

        Note: Meters and GHGRates need to be added to object prior
        to optimization.

        :param multiprocess: True to multiprocess
        """
        for meter in self.meter_group.meters.all():
            self.run_single_meter_simulation_and_cost(meter=meter)

    def get_aggregate_der_intervalframe(self):
        """
        Return dynamically calculated PowerIntervalFrame representing aggregate
        readings of DERSimulations within a Scenario.
        """
        if self.stacked:
            frame_attr = "stacked_der_simulation.der_intervalframe"
        else:
            frame_attr = "der_intervalframe"

        return reduce(
            lambda x, y: x + y,
            (nested_getattr(x, frame_attr) for x in self.der_simulations.all()),
            PowerIntervalFrame(),
        )

    def aggregate_meter_intervalframe(self):
        """
        Only aggretate meter_intervalframe if all self.der_simulations have
        been run.
        """
        if self.simulations_completed:
            with self.lock():
                der_intervalframe = self.get_aggregate_der_intervalframe()
                self.intervalframe = (
                    self.pre_der_intervalframe + der_intervalframe
                )
                self.save()

    def make_origin_file(self):
        """
        Makes an origin file from the post-DER simulation data, to be used for
        making compound simulations with multiple DERs.

        NOTE: This is a temporary method for use until a scenario can be the
        starting point for the creation of a new scenario.
        """
        # spoof file
        blank_file = "/tmp/null.csv"
        Path(blank_file).touch()

        meter_group = self.meter_group
        origin_file, _ = OriginFile.get_or_create(
            name=self.name,
            load_serving_entity=meter_group.load_serving_entity,
            file=open(blank_file, "rb"),
        )

        origin_file.expected_meter_count = meter_group.meters.count()
        origin_file.meters.add(*self.der_simulations)
        origin_file.owners.add(*meter_group.owners.all())
        origin_file.save()

        aggregate_meter_group_intervalframes.delay(origin_file.id)

        return origin_file

    def assign_cost_functions(self, cost_functions):
        """
        Assigns cost functions to the scenario

        :param cost_functions: dictionary mapping a cost function-type to the ID
          of the cost function to apply. Expected keys are "rate_plan",
          "ghg_rate", "procurement_rate" and "system_profile". No key is
          required
        """
        if "rate_plan" in cost_functions:
            try:
                self.rate_plan = RatePlan.objects.get(
                    id=cost_functions["rate_plan"],
                    load_serving_entity=self.load_serving_entity,
                )
            except RatePlan.DoesNotExist:
                pass

        if "ghg_rate" in cost_functions:
            try:
                self.ghg_rate = GHGRate.objects.get(
                    id=cost_functions["ghg_rate"]
                )
            except GHGRate.DoesNotExist:
                pass

        if "procurement_rate" in cost_functions:
            try:
                self.procurement_rate = CAISORate.objects.get(
                    id=cost_functions["procurement_rate"]
                )
            except CAISORate.DoesNotExist:
                pass

        if "system_profile" in cost_functions:
            try:
                self.system_profile = SystemProfile.objects.get(
                    id=cost_functions["system_profile"],
                    load_serving_entity=self.load_serving_entity,
                )
            except SystemProfile.DoesNotExist:
                pass

        # save the above changes
        self.save()
