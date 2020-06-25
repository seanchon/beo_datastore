from functools import reduce
import os
import pandas as pd
import uuid

from django.core.exceptions import ValidationError
from django.db import models
from django.db.models.signals import m2m_changed
from django.dispatch import receiver

from beo_datastore.libs.intervalframe import PowerIntervalFrame
from beo_datastore.libs.intervalframe_file import (
    ArbitraryDataFrameFile,
    PowerIntervalFrameFile,
)
from beo_datastore.libs.models import IntervalFrameFileMixin, ValidationModel
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
from der.simulation.models import StoredBatterySimulation
from load.customer.models import CustomerMeter
from load.openei.models import ReferenceMeter
from reference.reference_model.models import (
    DERConfiguration,
    DERStrategy,
    DERSimulation,
    Meter,
    MeterGroup,
    Study,
)
from reference.auth_user.models import LoadServingEntity


class ReportDataFrameFile(ArbitraryDataFrameFile):
    """
    Model for storing a report to file.
    """

    # directory for parquet file storage
    file_directory = os.path.join(MEDIA_ROOT, "report")


class Report(IntervalFrameFileMixin, ValidationModel):
    """
    Model for storing the report associated with a Scenario.
    """

    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    created_at = models.DateTimeField(auto_now_add=True)

    # Required by IntervalFrameFileMixin.
    frame_file_class = ReportDataFrameFile


class ReportSummaryDataFrameFile(ArbitraryDataFrameFile):
    """
    Model for storing a report summary to file.
    """

    # directory for parquet file storage
    file_directory = os.path.join(MEDIA_ROOT, "report")


class ReportSummary(IntervalFrameFileMixin, ValidationModel):
    """
    Model for storing the report associated with a Scenario.
    """

    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    created_at = models.DateTimeField(auto_now_add=True)

    # Required by IntervalFrameFileMixin.
    frame_file_class = ReportSummaryDataFrameFile


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
    _report = models.OneToOneField(
        to=Report,
        related_name="single_scenario_study",
        on_delete=models.CASCADE,
        blank=True,
        null=True,
    )
    _report_summary = models.OneToOneField(
        to=ReportSummary,
        related_name="single_scenario_study",
        on_delete=models.CASCADE,
        blank=True,
        null=True,
    )

    # Required by IntervalFrameFileMixin.
    frame_file_class = StudyIntervalFrame

    class Meta:
        ordering = ["id"]

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
        Dynamically calculated PowerIntervalFrame representing aggregate
        readings of all attached Meters before running DER simulations.

        NOTE: Use self.meter_group.meter_intervalframe to get the
        pre_der_intervalframe equivalent on the entire MeterGroup.
        """
        return reduce(
            lambda x, y: x + y,
            [x.meter_intervalframe for x in self.meters.all()],
            PowerIntervalFrame(PowerIntervalFrame.default_dataframe),
        )

    @property
    def der_intervalframe(self):
        """
        Dynamically calculated PowerIntervalFrame representing aggregate
        readings of all DER operations.

        NOTE: Use the difference of self.meter_intervalframe and
        self.meter_group.meter_intervalframe to get the der_intervalframe
        equivalent on the entire MeterGroup.
        """
        if self.der_simulations.count() > 0:
            return reduce(
                lambda x, y: x + y,
                [x.der_intervalframe for x in self.der_simulations.all()],
            )
        else:
            return PowerIntervalFrame(PowerIntervalFrame.default_dataframe)

    @property
    def post_der_intervalframe(self):
        """
        PowerIntervalFrame representing aggregate readings of all meters
        after running DER simulations.
        """
        return self.pre_der_intervalframe + self.der_intervalframe

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

    @property
    def der_simulations(self):
        """
        Return DERSimulations related to self with regards to any applied
        filter_by_query() or filter_by_transform() operations.
        """
        return DERSimulation.objects.select_related("meter").filter(
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
    def report(self):
        """
        Return pandas DataFrame with meter SA IDs, DERConfiguration details,
        RatePlan details, and all cost impacts.

        This report is used in MultipleScenarioStudy reports.
        """
        if not self._report:
            self._report = Report.objects.create()
            self.save()

        # Only generate report if:
        # 1. report has not previously been generated.
        # 2. all self.der_simulations have run.
        # 3. self.meter_intervalframe has been aggregated.
        if (
            self._report.frame.dataframe.empty
            and (self.der_simulations.count() == self.meters.count())
            and not self.meter_intervalframe.dataframe.empty
        ):
            self._report.frame = ReportDataFrameFile(
                dataframe=self.generate_report(), reference_object=self._report
            )
            self._report.save()

        return self._report.frame.dataframe

    def generate_report(self):
        report = (
            self.usage_report.join(self.bill_report, how="outer")
            .join(self.ghg_report, how="outer")
            .join(self.resource_adequacy_report, how="outer")
            .join(self.procurement_report, how="outer")
            .join(self.customer_meter_report, how="outer")
            .join(self.reference_meter_report, how="outer")
        )
        report["SingleScenarioStudy"] = str(self.id)
        report.index = report.index.astype(str)

        return report

    @property
    def report_summary(self):
        """
        pandas DataFrame with RA and non-RA totals for each column of
        report.
        """
        if not self._report_summary:
            self._report_summary = ReportSummary.objects.create()
            self.save()

        # Only generate report if:
        # 1. report has not previously been generated.
        # 2. all self.der_simulations have run.
        # 3. self.meter_intervalframe has been aggregated.
        if (
            self._report_summary.frame.dataframe.empty
            and (self.der_simulations.count() == self.meters.count())
            and not self.meter_intervalframe.dataframe.empty
        ):
            self._report_summary.frame = ReportDataFrameFile(
                dataframe=self.generate_report_summary(),
                reference_object=self._report_summary,
            )
            self._report_summary.save()

        return self._report_summary.frame.dataframe

    def generate_report_summary(self):
        return self.non_ra_report_summary.append(self.ra_report_summary)

    @property
    def non_ra_report_summary(self):
        """
        pandas DataFrame with non-RA totals for each column of report.
        """
        summary = pd.DataFrame(self.report.sum())
        indices = [
            x
            for x in summary.index
            if "PreDER" in x or "PostDER" in x or "Delta" in x
        ]
        # exclude RA columns
        for index in ["RAPreDER", "RAPostDER", "RADelta"]:
            if index in indices:
                indices.remove(index)

        return summary.loc[indices]

    @property
    def ra_report_summary(self):
        """
        pandas DataFrame with RA totals for each column of report.
        Filtering disabled.
        """
        # TODO: Account for CCA's with multiple system profiles
        system_profile = self.system_profiles.last()
        if not system_profile:
            return pd.DataFrame()

        pre_DER_RA = (
            system_profile.intervalframe.maximum_frame288.dataframe.max().sum()
        )
        inverse_pre_der_intervalframe = PowerIntervalFrame(
            dataframe=self.meter_group.meter_intervalframe.dataframe * -1
        )
        post_DER_RA = (
            (
                system_profile.intervalframe
                + self.meter_intervalframe
                + inverse_pre_der_intervalframe
            )
            .maximum_frame288.dataframe.max()
            .sum()
        )
        dataframe = pd.DataFrame(
            {
                "RAPreDER": [pre_DER_RA],
                "RAPostDER": [post_DER_RA],
                "RADelta": [(post_DER_RA - pre_DER_RA)],
            }
        ).transpose()

        return dataframe

    @property
    def report_html_table(self):
        """
        Return Django-formatted HTML detailed report.
        """
        return dataframe_to_html(self.report)

    @property
    def charge_schedule(self):
        """
        Charge BatterySchedule.
        """
        return self.der_strategy.charge_schedule

    @property
    def discharge_schedule(self):
        """
        Discharge BatterySchedule.
        """
        return self.der_strategy.discharge_schedule

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

    @property
    def bill_report(self):
        """
        Return pandas DataFrame with meter SA IDs and bill deltas.
        """
        return StoredBillCalculation.get_report(self.bill_calculations)

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
    def customer_meter_report(self):
        """
        Return pandas DataFrame with Meter SA IDs and RatePlans.
        """
        return CustomerMeter.get_report(
            CustomerMeter.objects.filter(id__in=self.meters.values_list("id"))
        )

    @property
    def reference_meter_report(self):
        """
        Return pandas DataFrame with ReferenceMeter location and building
        type.
        """
        return ReferenceMeter.get_report(
            ReferenceMeter.objects.filter(id__in=self.meters.values_list("id"))
        )

    def run_single_meter_simulation_and_cost(self, meter):
        """
        Run a single Meter's DERSimulation and cost calculations.
        """
        der_simulation_set = StoredBatterySimulation.generate(
            battery=self.der_configuration.battery,
            start=self.start,
            end_limit=self.end_limit,
            meter_set={meter},
            charge_schedule=self.charge_schedule.frame288,
            discharge_schedule=self.discharge_schedule.frame288,
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

        # TODO: multiprocess run_single_meter_simulation_and_cost()?

        for meter in self.meters.all():
            self.run_single_meter_simulation_and_cost(meter=meter)

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
            self.intervalframe.dataframe = (
                self.post_der_intervalframe.dataframe
            )
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

    @property
    def meters(self):
        """
        Return QuerySet of Meters in all self.single_scenario_studies.
        """
        return reduce(
            lambda x, y: x | y,
            [x.meters.all() for x in self.single_scenario_studies.all()],
            Meter.objects.none(),
        ).distinct()

    @property
    def meter_groups(self):
        """
        Return QuerySet of MeterGroups in all self.single_scenario_studies.
        """
        return reduce(
            lambda x, y: x | y,
            [x.meter_groups.all() for x in self.single_scenario_studies.all()],
            MeterGroup.objects.none(),
        ).distinct()

    @property
    def ders(self):
        """
        Return DERConfiguration and DERStrategy objects related to self.
        """
        return reduce(
            lambda x, y: x + y,
            [x.ders for x in self.single_scenario_studies.all()],
            [],
        )

    @property
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

    @property
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
            PowerIntervalFrame(PowerIntervalFrame.default_dataframe),
        )

    @property
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
            return (PowerIntervalFrame(PowerIntervalFrame.default_dataframe),)

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
            PowerIntervalFrame(PowerIntervalFrame.default_dataframe),
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

    @property
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

    @property
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

    @property
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
