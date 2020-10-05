from collections import namedtuple
from functools import reduce
import json
from jsonfield import JSONField
import os
import pandas as pd
import re

from django.core.exceptions import ValidationError
from django.db import models
from django.db.models.functions import Coalesce
from django.db.models.signals import m2m_changed
from django.dispatch import receiver
from django.utils.functional import cached_property

from beo_datastore.libs.der.builder import AggregateDERProduct, DERProduct
from beo_datastore.libs.controller import AggregateResourceAdequacyCalculation
from beo_datastore.libs.dataframe import add_interval_dataframe
from beo_datastore.libs.intervalframe import PowerIntervalFrame
from beo_datastore.libs.intervalframe_file import PowerIntervalFrameFile
from beo_datastore.libs.models import IntervalFrameFileMixin
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
from load.customer.models import CustomerMeter, OriginFile
from load.tasks import aggregate_meter_group_intervalframes
from reference.reference_model.models import (
    DERConfiguration,
    DERStrategy,
    DERSimulation,
    Meter,
    MeterGroup,
    Study,
)
from reference.auth_user.models import LoadServingEntity


class StudyIntervalFrame(PowerIntervalFrameFile):
    """
    Model for storing Study IntervalFrameFiles, which have timestamps and
    aggregate meter readings.
    """

    # directory for parquet file storage
    file_directory = os.path.join(MEDIA_ROOT, "study")


class SingleScenarioStudy(IntervalFrameFileMixin, Study):
    """
    Container for a single-scenario study.

    Steps to create a single-scenario study:
    1. Create SingleScenarioStudy object
    2. Add MeterGroups, GHGRates, and SystemProfiles
    3. run() to generate all simulations
    4. filter_by_query() to filter results
    5. display reports
    6. initialize() before running different filter_by_query()
    """

    start = models.DateTimeField()
    end_limit = models.DateTimeField()
    der_strategy = models.ForeignKey(
        to=DERStrategy,
        related_name="single_scenario_studies",
        on_delete=models.CASCADE,
    )
    der_configuration = models.ForeignKey(
        to=DERConfiguration,
        related_name="single_scenario_studies",
        on_delete=models.CASCADE,
    )
    meter_group = models.ForeignKey(
        to=MeterGroup,
        related_name="single_scenario_studies",
        on_delete=models.CASCADE,
    )
    # Constrains Meters and RatePlan to belong to LSE. If null is True, any
    # Meter and RatePlan can be used in optimization.
    load_serving_entity = models.ForeignKey(
        to=LoadServingEntity,
        related_name="single_scenario_studies",
        on_delete=models.CASCADE,
        blank=True,
        null=True,
    )
    rate_plan = models.ForeignKey(
        to=RatePlan,
        related_name="single_scenario_studies",
        on_delete=models.CASCADE,
    )
    ghg_rates = models.ManyToManyField(
        to=GHGRate, related_name="single_scenario_studies", blank=True
    )
    system_profiles = models.ManyToManyField(
        to=SystemProfile, related_name="single_scenario_studies", blank=True
    )
    caiso_rates = models.ManyToManyField(
        to=CAISORate, related_name="single_scenario_studies", blank=True
    )
    meters = models.ManyToManyField(
        to=Meter, related_name="single_scenario_studies", blank=True
    )
    _report = JSONField(blank=True, null=True, default={})
    _report_summary = JSONField(blank=True, null=True, default={})

    # Required by IntervalFrameFileMixin.
    frame_file_class = StudyIntervalFrame

    # Exclude fields from __repr__
    repr_exclude_fields = ["_report", "_report_summary"]

    class Meta:
        ordering = ["id"]

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
        Return DERConfiguration and DERStrategy objects related to self.
        """
        return [
            {
                "der_configuration": self.der_configuration,
                "der_strategy": self.der_strategy,
            }
        ]

    @cached_property
    def der_simulations(self):
        """
        Return DERSimulations related to self with regards to any applied
        filter_by_query() or filter_by_transform() operations.
        """
        return DERSimulation.objects.filter(
            meter__in=self.meters.all(),
            start=self.start,
            end_limit=self.end_limit,
            der_configuration=self.der_configuration,
            der_strategy=self.der_strategy,
        )

    @property
    def expected_der_simulation_count(self):
        """
        Number of expected DERSimulation objects with regards to any applied
        filter_by_query() or filter_by_transform() operations.
        """
        return self.meters.count()

    @property
    def simulations_complete(self):
        """
        Return True if all DERSimulations have run and meter_intervalframe has
        been aggregated.
        """
        return (
            self.der_simulations.count() == self.expected_der_simulation_count
            and not self.meter_intervalframe.dataframe.empty
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
        if self.simulations_complete:
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
        report["SingleScenarioStudy"] = str(self.id)
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
        # TODO: Account for CCA's with multiple system profiles
        system_profile = self.system_profiles.last()
        if not system_profile:
            return None

        # compute RA on all meters combined
        return AggregateResourceAdequacyCalculation(
            agg_simulation=self.agg_simulation,
            system_profile_intervalframe=system_profile.intervalframe,
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
            der_simulation__in=self.der_simulations, rate_plan=self.rate_plan
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
            ghg_rate__in=self.ghg_rates.all(),
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
            system_profile__in=self.system_profiles.all(),
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
            caiso_rate__in=self.caiso_rates.all(),
        )

    @property
    def usage_report(self):
        """
        Return pandas DataFrame with meter SA IDs and usage deltas.
        """
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
        for (meter_id,) in self.meters.values_list("id"):
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
            Meter.objects.filter(id__in=self.meters.values_list("id")),
            column_map={"sa_id": "SA ID", "rate_plan_name": "MeterRatePlan"},
        )

    @property
    def der_simulation_class(self):
        """
        Returns the DERSimulation class to use with the study's DER
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
            der=self.der_configuration.der,
            der_strategy=self.der_strategy.der_strategy,
            start=self.start,
            end_limit=self.end_limit,
            meter_set={meter},
            multiprocess=False,
        )

        StoredBillCalculation.generate(
            der_simulation_set=der_simulation_set,
            rate_plan=self.rate_plan,  # TODO: link to meter
            multiprocess=False,
        )

        for ghg_rate in self.ghg_rates.all():
            StoredGHGCalculation.generate(
                der_simulation_set=der_simulation_set, ghg_rate=ghg_rate
            )

        for system_profile in self.system_profiles.all():
            StoredResourceAdequacyCalculation.generate(
                der_simulation_set=der_simulation_set,
                system_profile=system_profile,
            )

        for caiso_rate in self.caiso_rates.all():
            StoredProcurementCostCalculation.generate(
                der_simulation_set=der_simulation_set, caiso_rate=caiso_rate
            )

    def run(self, multiprocess=False):
        """
        Run related DERSimulations, StoredBillCalculations and
        StoredGHGCalculations.

        Note: Meters and GHGRates need to be added to object prior
        to optimization.

        :param multiprocess: True to multiprocess
        """
        # add Meters from MeterGroups
        self.initialize()

        for meter in self.meters.all():
            self.run_single_meter_simulation_and_cost(meter=meter)

    def get_aggregate_der_intervalframe(self):
        """
        Return dynamically calculated PowerIntervalFrame representing aggregate
        readings of DERSimulations within a SingleScenarioStudys.
        """
        return reduce(
            lambda x, y: x + y,
            [x.der_intervalframe for x in self.der_simulations.all()],
            PowerIntervalFrame(),
        )

    def aggregate_meter_intervalframe(self, force=False):
        """
        Only aggretate meter_intervalframe if:
            - all self.der_simulations have been run.
            - self.meter_intervalframe has not yet been aggregated.

        :param force: True to force aggregation
        """
        if (
            (self.der_simulations.count() == self.meters.count())
            and self.meter_intervalframe.dataframe.empty
        ) or force:
            der_intervalframe = self.get_aggregate_der_intervalframe()
            self.intervalframe = self.pre_der_intervalframe + der_intervalframe
            self.save()

    def filter_by_query(self, query):
        """
        Based on DataFrame query, only matching meters are kept as part of
        the SingleScenarioStudy. All filtering can be reset using
        initialize().

        Example:
        query = "Bill_Delta > 0" only keeps meters where Bill_Delta is greater
        than 0.
        """
        # TODO: delete if no longer used
        if self.meters.count() > 0:
            df = ~self.report.eval(query)
            ids_to_remove = df.index[df == 1].tolist()
            self.meters.remove(*Meter.objects.filter(id__in=ids_to_remove))

    def initialize(self):
        """
        Attaches any Meters within attached MeterGroups.
        Optimizations are performed by removing meters until only the desired
        Meters are attached to a SingleScenarioStudy. This
        method allows many optimizations to be tried.
        """
        # TODO: delete if no longer used
        self.meters.clear()
        self.meters.add(*self.meter_group.meters.all())

    def make_origin_file(self):
        """
        Makes an origin file from the post-DER simulation data, to be used for
        making compound simulations with multiple DERs.

        NOTE: This is a temporary method for use until a scenario can be the
        starting point for the creation of a new scenario.
        """
        meter_group = self.meter_group
        origin_file, _ = OriginFile.get_or_create(
            name=self.name,
            load_serving_entity=meter_group.load_serving_entity,
            file=meter_group.file,
        )

        origin_file.expected_meter_count = meter_group.meters.count()
        origin_file.meters.add(*self.der_simulations)
        origin_file.owners.add(*meter_group.owners.all())
        origin_file.save()

        aggregate_meter_group_intervalframes.delay(origin_file.id)
        return origin_file


@receiver(m2m_changed, sender=SingleScenarioStudy.meters.through)
def reset_cached_properties_update_meters(sender, **kwargs):
    """
    Reset cached properties whenever meters is updated. This resets any cached
    reports.
    """
    simulation_optimization = kwargs.get(
        "instance", SingleScenarioStudy.objects.none()
    )
    simulation_optimization._reset_cached_properties()


@receiver(m2m_changed, sender=SingleScenarioStudy.meters.through)
def validate_meters_belong_to_lse(sender, **kwargs):
    """
    If load_serving_entity is set, this validation ensure meters not belonging
    to LSE are not added to SingleScenarioStudy.
    """
    # get SingleScenarioStudy and Meters proposed to be added
    simulation_optimization = kwargs.get(
        "instance", SingleScenarioStudy.objects.none()
    )
    pk_set = kwargs.get("pk_set", {})
    if pk_set is None:
        pk_set = {}

    if (
        kwargs.get("action", None) == "pre_add"
        and simulation_optimization.load_serving_entity
        and CustomerMeter.objects.filter(id__in=pk_set).exclude(
            load_serving_entity=simulation_optimization.load_serving_entity
        )
    ):
        raise AttributeError(
            "Meter assignment is limited by those belonging to the "
            "LoadServingEntity."
        )


@receiver(m2m_changed, sender=SingleScenarioStudy.ghg_rates.through)
def reset_cached_properties_update_ghg_rates(sender, **kwargs):
    """
    Reset cached properties whenever ghg_rates is updated. This resets any
    cached reports.
    """
    simulation_optimization = kwargs.get(
        "instance", SingleScenarioStudy.objects.none()
    )
    simulation_optimization._reset_cached_properties()


class MultipleScenarioStudy(IntervalFrameFileMixin, Study):
    """
    Container for a multiple-scenario study.

    Steps to create a multiple-scenario study:
    1. Create MultipleScenarioStudy object and related to
       SingleScenarioStudy objects
    2. run() to generate all simulations
    3. filter_by_query() or filter_by_transform() to filter results
    4. display reports
    5. initialize() before running different filter_by_query()
    """

    single_scenario_studies = models.ManyToManyField(
        to=SingleScenarioStudy, related_name="multiple_scenario_studies"
    )

    # Required by IntervalFrameFileMixin.
    frame_file_class = StudyIntervalFrame

    class Meta:
        ordering = ["id"]

    @property
    def meter_intervalframe(self):
        """
        Blank PowerIntervalFrame. This property is error-prone if the same
        Meter exists in many SingleScenarioStudy objects.
        # TODO: Return self.post_der_intervalframe after filtering is
            performed.
        """
        return self.intervalframe

    @cached_property
    def meters(self):
        """
        Return QuerySet of Meters in all self.single_scenario_studies.
        """
        return reduce(
            lambda x, y: x | y,
            [x.meters.all() for x in self.single_scenario_studies.all()],
            Meter.objects.none(),
        ).distinct()

    @cached_property
    def meter_groups(self):
        """
        Return QuerySet of MeterGroups in all self.single_scenario_studies.
        """
        return reduce(
            lambda x, y: x | y,
            [x.meter_groups.all() for x in self.single_scenario_studies.all()],
            MeterGroup.objects.none(),
        ).distinct()

    @cached_property
    def ders(self):
        """
        Return DERConfiguration and DERStrategy objects related to self.
        """
        return reduce(
            lambda x, y: x + y,
            [x.ders for x in self.single_scenario_studies.all()],
            [],
        )

    @cached_property
    def der_simulations(self):
        """
        Return DERSimulations related to self with regards to any applied
        filter_by_query() or filter_by_transform() operations.
        """
        return reduce(
            lambda x, y: x | y,
            [
                x.der_simulations.all()
                for x in self.single_scenario_studies.all()
            ],
            DERSimulation.objects.none(),
        ).distinct()

    @cached_property
    def expected_der_simulation_count(self):
        """
        Number of expected DERSimulation objects with regards to any applied
        filter_by_query() or filter_by_transform() operations.
        """
        return sum(
            [
                x.expected_der_simulation_count
                for x in self.single_scenario_studies.all()
            ]
        )

    @property
    def pre_der_intervalframe(self):
        """
        PowerIntervalFrame representing aggregate readings of all meters
        before running DER simulations.
        """
        self.validate_unique_meters()
        return reduce(
            lambda x, y: x + y,
            [
                x.pre_der_intervalframe
                for x in self.single_scenario_studies.all()
            ],
            PowerIntervalFrame(),
        )

    @cached_property
    def der_intervalframe(self):
        """
        PowerIntervalFrame representing aggregate readings of all DER
        operations.
        """
        self.validate_unique_meters()
        # only include studies with meters
        studies = [
            x
            for x in self.single_scenario_studies.all()
            if x.meters.count() > 0
        ]
        if len(studies) > 0:
            return reduce(
                lambda x, y: x + y, [x.der_intervalframe for x in studies]
            )
        else:
            return (PowerIntervalFrame(),)

    @property
    def post_der_intervalframe(self):
        """
        PowerIntervalFrame representing aggregate readings of all meters
        after running DER simulations.
        """
        self.validate_unique_meters()
        return reduce(
            lambda x, y: x + y,
            [
                x.post_der_intervalframe
                for x in self.single_scenario_studies.all()
            ],
            PowerIntervalFrame(),
        )

    @property
    def simulations_complete(self):
        """
        Return True if all DERSimulations have run and meter_intervalframe has
        been aggregated in all related SingleScenarioStudy objects.
        """
        return all(
            [
                x.simulations_complete
                for x in self.single_scenario_studies.all()
            ]
        )

    @property
    def report(self):
        """
        Return pandas DataFrame of all single_scenario_studies'
        report(s) appended together.
        """
        return reduce(
            lambda x, y: x.append(y, sort=False),
            [x.report for x in self.single_scenario_studies.all()],
            pd.DataFrame(),
        ).sort_index()

    @property
    def report_summary(self):
        """
        Return pandas DataFrame of all single_scenario_studies'
        report_summary(s) added together.
        """
        return reduce(
            lambda x, y: x + y,
            [x.report_summary for x in self.single_scenario_studies.all()],
        )

    @property
    def report_html_table(self):
        """
        Return Django-formatted HTML detailed report.
        """
        return dataframe_to_html(self.report)

    @cached_property
    def bill_calculations(self):
        """
        Return StoredBillCalculations related to self.
        """
        return reduce(
            lambda x, y: x | y,
            [
                x.bill_calculations.all()
                for x in self.single_scenario_studies.all()
            ],
            StoredBillCalculation.objects.none(),
        ).distinct()

    @cached_property
    def ghg_calculations(self):
        """
        Return Stored StoredGHGCalculations related to self.
        """
        return reduce(
            lambda x, y: x | y,
            [
                x.ghg_calculations.all()
                for x in self.single_scenario_studies.all()
            ],
            StoredGHGCalculation.objects.none(),
        ).distinct()

    @cached_property
    def resource_adequacy_calculations(self):
        """
        Return StoredResourceAdequacyCalculations related to self.
        """
        return reduce(
            lambda x, y: x | y,
            [
                x.resource_adequacy_calculations.all()
                for x in self.single_scenario_studies.all()
            ],
            StoredResourceAdequacyCalculation.objects.none(),
        ).distinct()

    def validate_unique_meters(self):
        """
        Validate that each meter appears only once in
        self.single_scenario_studies.
        """
        total_meter_count = reduce(
            lambda x, y: x + y,
            [x.meters.count() for x in self.single_scenario_studies.all()],
        )
        unique_meter_count = (
            reduce(
                lambda x, y: x | y,
                [x.meters.all() for x in self.single_scenario_studies.all()],
                SingleScenarioStudy.objects.none(),
            )
            .distinct()
            .count()
        )

        if total_meter_count != unique_meter_count:
            raise RuntimeError(
                "Meters in MultipleScenarioStudy are not "
                "unique. See: filter_by_transform() to filter meters."
            )

    def run(self, multiprocess=False):
        """
        Run related SingleScenarioStudys.

        Note: Meters and GHGRates need to be added to object prior
        to optimization.

        :param multiprocess: True to multiprocess
        """
        for simulation_optimization in self.single_scenario_studies.all():
            simulation_optimization.run(multiprocess=multiprocess)
        self._reset_cached_properties()

    def filter_by_query(self, query):
        """
        Based on DataFrame query, only matching meters are kept as part of
        the single_scenario_studies. All filtering can be reset using
        initialize().

        Example:
        query = "Bill_Delta > 0" only keeps meters where Bill_Delta is greater
        than 0.
        """
        for simulation_optimization in self.single_scenario_studies.all():
            simulation_optimization.filter_by_query(query)
        self._reset_cached_properties()

    def filter_by_transform(self, column_name, transform):
        """
        Filter report filtered by transform per SA ID using values from
        column_name. transform is a function (ex. min, max) that would return
        an optimized value per SA ID.

        :param column_name: string
        :param transfrom: transform function
        """
        if self.meters.count() == 0:
            return

        # get values per SA ID using transform
        idx = (
            self.report.groupby([self.report.index])[column_name].transform(
                transform
            )
            == self.report[column_name]
        )
        report = self.report[idx]

        # remove duplicates
        report = report.loc[~report.index.duplicated(keep="first")]

        # keep only desired meters in each SingleScenarioStudy
        for simulation_optimization in self.single_scenario_studies.filter(
            id__in=set(self.report["SingleScenarioStudy"])
        ):
            id = simulation_optimization.id
            ids = report[report["SingleScenarioStudy"] == id].index
            meter_ids = list(
                simulation_optimization.meters.filter(id__in=ids).values_list(
                    "id", flat=True
                )
            )
            simulation_optimization.meters.clear()
            simulation_optimization.meters.add(
                *Meter.objects.filter(id__in=meter_ids)
            )
        self._reset_cached_properties()

    def initialize(self):
        """
        Re-attaches any Meters previously associated with related
        SingleScenarioStudy objects.
        """
        for simulation_optimization in self.single_scenario_studies.all():
            simulation_optimization.initialize()
        self._reset_cached_properties()


@receiver(
    m2m_changed, sender=MultipleScenarioStudy.single_scenario_studies.through
)
def reset_cached_properties_update_single_scenario_studies(sender, **kwargs):
    """
    Reset cached properties whenever single_scenario_studies is updated. This
    resets any cached reports.
    """
    simulation_optimization = kwargs.get("instance", None)
    simulation_optimization._reset_cached_properties()
