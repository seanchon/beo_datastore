from functools import reduce
import pandas as pd
import re

from django.core.exceptions import ValidationError
from django.db import models
from django.db.models.signals import m2m_changed
from django.dispatch import receiver

from beo_datastore.libs.intervalframe import ValidationIntervalFrame
from beo_datastore.libs.views import dataframe_to_html

from cost.ghg.models import GHGRate, StoredGHGCalculation
from cost.procurement.models import (
    SystemProfile,
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


class SingleScenarioStudy(Study):
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
    meters = models.ManyToManyField(
        to=Meter, related_name="single_scenario_studies", blank=True
    )

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
    def pre_der_intervalframe(self):
        """
        ValidationIntervalFrame representing aggregate readings of all meters
        before running DER simulations.
        """
        return reduce(
            lambda x, y: x + y,
            [x.meter_intervalframe for x in self.meters.all()],
            ValidationIntervalFrame(ValidationIntervalFrame.default_dataframe),
        )

    @property
    def der_intervalframe(self):
        """
        ValidationIntervalFrame representing aggregate readings of all DER
        operations.
        """
        if self.der_simulations.count() > 0:
            return reduce(
                lambda x, y: x + y,
                [x.der_intervalframe for x in self.der_simulations.all()],
            )
        else:
            return ValidationIntervalFrame(
                ValidationIntervalFrame.default_dataframe
            )

    @property
    def post_der_intervalframe(self):
        """
        ValidationIntervalFrame representing aggregate readings of all meters
        after running DER simulations.
        """
        return self.pre_der_intervalframe + self.der_intervalframe

    @property
    def report(self):
        """
        Return pandas Dataframe with meter SA IDs and all cost impacts.
        """
        return (
            self.usage_report.join(self.bill_report, how="outer")
            .join(self.ghg_report, how="outer")
            .join(self.resource_adequacy_report, how="outer")
        )

    @property
    def detailed_report(self):
        """
        Return pandas DataFrame with meter SA IDs, DERConfiguration details,
        RatePlan details, and all cost impacts.

        This report is used in MultipleScenarioStudy reports.
        """
        report = self.report_with_id
        report["DERConfiguration"] = self.der_configuration.detailed_name
        report["DERStrategy"] = self.der_strategy.name
        report["SimulationRatePlan"] = self.rate_plan.name

        return report.join(self.customer_meter_report, how="outer").join(
            self.reference_meter_report, how="outer"
        )

    @property
    def detailed_report_summary(self):
        """
        Return pandas DataFrame with totals for each column of a
        detailed_report.
        """

        # TODO: calculate system RA
        # only return PreDER, PostDER, and Delta columns
        summary = pd.DataFrame(self.detailed_report.sum())
        return summary.ix[
            [x for x in summary.index if "DER" in x or "Delta" in x]
        ]

    @property
    def detailed_report_html_table(self):
        """
        Return Django-formatted HTML detailed report.
        """
        return dataframe_to_html(self.detailed_report)

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
    def report_with_id(self):
        """
        Return pandas Dataframe self.report and SingleScenarioStudy id.
        """
        report = self.report
        report["SingleScenarioStudy"] = self.id

        return report

    @property
    def usage_report(self):
        """
        Return pandas DataFrame with meter SA IDs and usage deltas.
        """
        dataframe = pd.DataFrame(
            sorted(
                [
                    (
                        x.meter.id,
                        x.pre_DER_total,
                        x.post_DER_total,
                        x.net_impact,
                    )
                    for x in self.der_simulations
                ],
                key=lambda x: x[1],
            )
        )

        if not dataframe.empty:
            return dataframe.rename(
                columns={
                    0: "ID",
                    1: "UsagePreDER",
                    2: "UsagePostDER",
                    3: "UsageDelta",
                }
            ).set_index("ID")
        else:
            return pd.DataFrame()

    @property
    def bill_report(self):
        """
        Return pandas DataFrame with meter SA IDs and bill deltas.
        """
        dataframe = pd.DataFrame(
            sorted(
                [
                    (
                        x.der_simulation.meter.id,
                        x.pre_DER_total,
                        x.post_DER_total,
                        x.net_impact,
                    )
                    for x in self.bill_calculations
                ],
                key=lambda x: x[1],
            )
        )

        if not dataframe.empty:
            return dataframe.rename(
                columns={
                    0: "ID",
                    1: "BillPreDER",
                    2: "BillPostDER",
                    3: "BillDelta",
                }
            ).set_index("ID")
        else:
            return pd.DataFrame()

    @property
    def ghg_report(self):
        """
        Return pandas DataFrame with meter SA IDs and GHG deltas from all
        associated GHGRates.
        """
        return reduce(
            lambda x, y: x.join(y, how="outer"),
            [
                self.get_ghg_report(ghg_rate)
                for ghg_rate in self.ghg_rates.all()
            ],
            pd.DataFrame(),
        )

    @property
    def resource_adequacy_report(self):
        """
        Return pandas DataFrame with meter SA IDs and RA deltas from all
        associated GHGRates.
        """
        return reduce(
            lambda x, y: x.join(y, how="outer"),
            [
                self.get_resource_adequacy_report(system_profile)
                for system_profile in self.system_profiles.all()
            ],
            pd.DataFrame(),
        )

    @property
    def customer_meter_report(self):
        """
        Return pandas DataFrame with Meter SA IDs and RatePlans.
        """
        dataframe = pd.DataFrame(
            CustomerMeter.objects.filter(
                id__in=self.meters.values_list("id")
            ).values_list("id", "sa_id", "rate_plan_name")
        )

        if not dataframe.empty:
            return dataframe.rename(
                columns={0: "ID", 1: "SA ID", 2: "MeterRatePlan"}
            ).set_index("ID")
        else:
            return pd.DataFrame()

    @property
    def reference_meter_report(self):
        """
        Return pandas DataFrame with ReferenceMeter location and building
        type.
        """
        dataframe = pd.DataFrame(
            ReferenceMeter.objects.filter(
                id__in=self.meters.values_list("id")
            ).values_list("id", "location", "building_type__name")
        )

        if not dataframe.empty:
            return dataframe.rename(
                columns={0: "ID", 1: "Location", 2: "Building Type"}
            ).set_index("ID")
        else:
            return pd.DataFrame()

    def get_ghg_report(self, ghg_rate):
        """
        Return pandas DataFrame with meter SA IDs and GHG impacts.

        :param ghg_rate: GHGRate
        :return: pandas DataFrame
        """
        dataframe = pd.DataFrame(
            sorted(
                [
                    (
                        x.der_simulation.meter.id,
                        x.pre_DER_total,
                        x.post_DER_total,
                        x.net_impact,
                    )
                    for x in self.ghg_calculations.filter(ghg_rate=ghg_rate)
                ],
                key=lambda x: x[1],
            )
        )

        if not dataframe.empty:
            ghg_rate_name = re.sub(r"\W+", "", ghg_rate.name)
            name = "{}{}".format(ghg_rate_name, ghg_rate.effective.year)
            return dataframe.rename(
                columns={
                    0: "ID",
                    1: "{}PreDER".format(name),
                    2: "{}PostDER".format(name),
                    3: "{}Delta".format(name),
                }
            ).set_index("ID")
        else:
            return pd.DataFrame()

    def get_resource_adequacy_report(self, system_profile):
        """
        Return pandas DataFrame with meter SA IDs and RA impacts.

        :param system_profile: SystemProfile
        :return: pandas DataFrame
        """
        dataframe = pd.DataFrame(
            sorted(
                [
                    (
                        x.der_simulation.meter.id,
                        x.pre_DER_total,
                        x.post_DER_total,
                        x.net_impact,
                    )
                    for x in self.resource_adequacy_calculations.filter(
                        system_profile=system_profile
                    )
                ],
                key=lambda x: x[1],
            )
        )

        if not dataframe.empty:
            name = "{}{}".format(
                system_profile.name.replace(" ", ""),
                system_profile.load_serving_entity.name.replace(" ", ""),
            )
            return dataframe.rename(
                columns={
                    0: "ID",
                    1: "{}PeakPreDER".format(name),
                    2: "{}PeakPostDER".format(name),
                    3: "{}PeakDelta".format(name),
                }
            ).set_index("ID")
        else:
            return pd.DataFrame()

    def run_single_meter_simulation_and_cost(self, meter):
        """
        Run a single Meter's DERSimultion and cost calculations.
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

    def filter_by_query(self, query):
        """
        Based on DataFrame query, only matching meters are kept as part of
        the SingleScenarioStudy. All filtering can be reset using
        initialize().

        Example:
        query = "Bill_Delta > 0" only keeps meters where Bill_Delta is greater
        than 0.
        """
        if self.meters.count() > 0:
            df = ~self.detailed_report.eval(query)
            ids_to_remove = df.index[df == 1].tolist()
            self.meters.remove(*Meter.objects.filter(id__in=ids_to_remove))

    def initialize(self):
        """
        Attaches any Meters within attached MeterGroups.
        Optimizations are performed by removing meters until only the desired
        Meters are attached to a SingleScenarioStudy. This
        method allows many optimizations to be tried.
        """
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


class MultipleScenarioStudy(Study):
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

    class Meta:
        ordering = ["id"]

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
        ValidationIntervalFrame representing aggregate readings of all meters
        before running DER simulations.
        """
        self.validate_unique_meters()
        return reduce(
            lambda x, y: x + y,
            [
                x.pre_der_intervalframe
                for x in self.single_scenario_studies.all()
            ],
            ValidationIntervalFrame(ValidationIntervalFrame.default_dataframe),
        )

    @property
    def der_intervalframe(self):
        """
        ValidationIntervalFrame representing aggregate readings of all DER
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
            return (
                ValidationIntervalFrame(
                    ValidationIntervalFrame.default_dataframe
                ),
            )

    @property
    def post_der_intervalframe(self):
        """
        ValidationIntervalFrame representing aggregate readings of all meters
        after running DER simulations.
        """
        self.validate_unique_meters()
        return reduce(
            lambda x, y: x + y,
            [
                x.post_der_intervalframe
                for x in self.single_scenario_studies.all()
            ],
            ValidationIntervalFrame(ValidationIntervalFrame.default_dataframe),
        )

    @property
    def report(self):
        """
        Return pandas DataFrame of all single_scenario_studies' report_with_id
        appended together.
        """
        return reduce(
            lambda x, y: x.append(y, sort=False),
            [x.report_with_id for x in self.single_scenario_studies.all()],
            pd.DataFrame(),
        ).sort_index()

    @property
    def detailed_report(self):
        """
        Return pandas DataFrame of all single_scenario_studies'
        detailed_report appended together.
        """
        return reduce(
            lambda x, y: x.append(y, sort=False),
            [x.detailed_report for x in self.single_scenario_studies.all()],
            pd.DataFrame(),
        ).sort_index()

    @property
    def detailed_report_summary(self):
        """
        Return pandas DataFrame of all single_scenario_studies'
        detailed_report_summary added together.
        """
        return reduce(
            lambda x, y: x + y,
            [
                x.detailed_report_summary
                for x in self.single_scenario_studies.all()
            ],
        )

    @property
    def detailed_report_html_table(self):
        """
        Return Django-formatted HTML detailed report.
        """
        return dataframe_to_html(self.detailed_report)

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
            self.detailed_report.groupby([self.detailed_report.index])[
                column_name
            ].transform(transform)
            == self.detailed_report[column_name]
        )
        report = self.detailed_report[idx]

        # remove duplicates
        report = report.loc[~report.index.duplicated(keep="first")]

        # keep only desired meters in each SingleScenarioStudy
        for simulation_optimization in self.single_scenario_studies.filter(
            id__in=set(self.detailed_report["SingleScenarioStudy"])
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
