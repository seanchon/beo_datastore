from functools import reduce
import pandas as pd

from django.db import models
from django.db.models.signals import m2m_changed
from django.dispatch import receiver
from django.utils.functional import cached_property

from beo_datastore.libs.battery import BatteryIntervalFrame
from beo_datastore.libs.controller import AggregateBatterySimulation
from beo_datastore.libs.intervalframe import ValidationIntervalFrame
from beo_datastore.libs.models import ValidationModel
from beo_datastore.libs.views import dataframe_to_html

from cost.ghg.models import GHGRate, StoredGHGCalculation
from cost.procurement.models import (
    SystemProfile,
    StoredResourceAdequacyCalculation,
)
from cost.utility_rate.models import RatePlan, StoredBillCalculation
from der.simulation.models import (
    BatteryConfiguration,
    BatteryStrategy,
    StoredBatterySimulation,
)
from load.customer.models import Meter, CustomerCluster
from reference.reference_model.models import LoadServingEntity


class SimulationOptimization(ValidationModel):
    """
    Container for a single simulation optimization.

    Steps to create a simulation optimization:
    1. Create SimulationOptimization object
    2. Add CustomerClusters, GHGRates, and SystemProfiles
    3. run() to generate all simulations
    4. filter_by_query() to filter results
    5. display reports
    6. initialize() before running different filter_by_query()
    """

    name = models.CharField(max_length=128, blank=True, null=True)
    start = models.DateTimeField()
    end_limit = models.DateTimeField()
    battery_strategy = models.ForeignKey(
        to=BatteryStrategy,
        related_name="simulation_optimizations",
        on_delete=models.CASCADE,
    )
    battery_configuration = models.ForeignKey(
        to=BatteryConfiguration,
        related_name="simulation_optimizations",
        on_delete=models.CASCADE,
    )
    # Constrains Meters and RatePlan to belong to LSE. If null is True, any
    # Meter and RatePlan can be used in optimization.
    load_serving_entity = models.ForeignKey(
        to=LoadServingEntity,
        related_name="simulation_optimizations",
        on_delete=models.CASCADE,
        blank=True,
        null=True,
    )
    rate_plan = models.ForeignKey(
        to=RatePlan,
        related_name="simulation_optimizations",
        on_delete=models.CASCADE,
    )
    ghg_rates = models.ManyToManyField(
        to=GHGRate, related_name="simulation_optimizations", blank=True
    )
    system_profiles = models.ManyToManyField(
        to=SystemProfile, related_name="simulation_optimizations", blank=True
    )
    customer_clusters = models.ManyToManyField(
        to=CustomerCluster, related_name="simulation_optimizations", blank=True
    )
    meters = models.ManyToManyField(
        to=Meter, related_name="simulation_optimizations", blank=True
    )

    class Meta:
        ordering = ["id"]
        unique_together = (
            "name",
            "start",
            "end_limit",
            "battery_strategy",
            "battery_configuration",
            "load_serving_entity",
            "rate_plan",
        )

    def save(self, *args, **kwargs):
        if (
            self.rate_plan
            and self.load_serving_entity
            and self.rate_plan not in self.load_serving_entity.rate_plans.all()
        ):
            raise AttributeError(
                "RatePlan assignment is limited by those belonging to the "
                "LoadServingEntity."
            )

        super().save(*args, **kwargs)

    @property
    def customer_cluster_meters(self):
        """
        QuerySet of Meters from all customer_clusters.
        """
        return reduce(
            lambda x, y: x | y,
            [x.meters.all() for x in self.customer_clusters.all()],
            Meter.objects.none(),
        )

    @property
    def charge_schedule(self):
        """
        Charge BatterySchedule.
        """
        return self.battery_strategy.charge_schedule

    @property
    def discharge_schedule(self):
        """
        Discharge BatterySchedule.
        """
        return self.battery_strategy.discharge_schedule

    @property
    def battery_simulations(self):
        """
        Return StoredBatterySimulations related to self.
        """
        return StoredBatterySimulation.objects.filter(
            start=self.start,
            end_limit=self.end_limit,
            meter__in=self.meters.all(),
            battery_configuration=self.battery_configuration,
            battery_strategy=self.battery_strategy,
        )

    @property
    def bill_calculations(self):
        """
        Return StoredBillCalculations related to self.
        """
        return StoredBillCalculation.objects.filter(
            battery_simulation__in=self.battery_simulations,
            rate_plan=self.rate_plan,
        )

    @property
    def ghg_calculations(self):
        """"
        Return StoredGHGCalculations related to self.
        """
        return StoredGHGCalculation.objects.filter(
            battery_simulation__in=self.battery_simulations,
            ghg_rate__in=self.ghg_rates.all(),
        )

    @property
    def resource_adequacy_calculations(self):
        """
        Return StoredResourceAdequacyCalculations related to self.
        """
        return StoredResourceAdequacyCalculation.objects.filter(
            battery_simulation__in=self.battery_simulations,
            system_profile__in=self.system_profiles.all(),
        )

    @cached_property
    def agg_simulation(self):
        """
        Return AggregateBatterySimulation associated with self.

        AggregateBatterySimulations with the same parameters can be added to
        one another and can be used for aggregate "cost calculations" found in
        beo_datastore/libs/controller.py.
        """
        return reduce(
            lambda x, y: x + y,
            [x.agg_simulation for x in self.battery_simulations],
            AggregateBatterySimulation(
                battery=self.battery_configuration.battery,
                start=self.start,
                end_limit=self.end_limit,
                charge_schedule=self.charge_schedule.frame288,
                discharge_schedule=self.discharge_schedule.frame288,
                results={},
            ),
        )

    @property
    def aggregate_battery_intervalframe(self):
        """
        Return BatteryIntervalFrame representing all battery operations in
        aggregate.
        """
        return self.agg_simulation.aggregate_battery_intervalframe

    @property
    def pre_DER_intervalframe(self):
        """
        Return a single ValidationIntervalFrame represeting the aggregate
        readings of all meter readings before a DER simulation.
        """
        return self.agg_simulation.pre_DER_intervalframe

    @property
    def post_DER_intervalframe(self):
        """
        Return a single ValidationIntervalFrame represeting the aggregate
        readings of all meter reading after a DER simulation.
        """
        return self.agg_simulation.post_DER_intervalframe

    @cached_property
    def energy_loss(self):
        """
        Return all energy lost due to battery roundtrip efficiency.
        """
        return sum([x.energy_loss for x in self.battery_simulations])

    @cached_property
    def detailed_report(self):
        """
        Return pandas Dataframe with self.report_with_id and
        BatteryConfiguration details, Simulation RatePlan, and Meter RatePlan.

        This report is used in MultiScenarioOptimization reports.
        """
        report = self.report_with_id
        report[
            "BatteryConfiguration"
        ] = self.battery_configuration.detailed_name
        report["BatteryStrategy"] = self.battery_strategy.name
        report["SimulationRatePlan"] = self.rate_plan.name

        return report.join(self.meter_report, how="outer")

    @property
    def detailed_report_html_table(self):
        """
        Return Django-formatted HTML detailed report.
        """
        return dataframe_to_html(self.detailed_report)

    @cached_property
    def report_with_id(self):
        """
        Return pandas Dataframe self.report and SimulationOptimization id.
        """
        report = self.report
        report["SimulationOptimization"] = self.id

        return report

    @cached_property
    def report(self):
        """
        Return pandas Dataframe with meter SA IDs and all bill and GHG impacts.
        """
        return (
            self.usage_report.join(self.bill_report, how="outer")
            .join(self.ghg_report, how="outer")
            .join(self.resource_adequacy_report, how="outer")
        )

    @cached_property
    def usage_report(self):
        """
        Return pandas DataFrame with meter SA IDs and usage deltas.
        """
        dataframe = pd.DataFrame(
            sorted(
                [
                    (
                        x.meter.sa_id,
                        x.pre_DER_total,
                        x.post_DER_total,
                        x.net_impact,
                    )
                    for x in self.battery_simulations
                ],
                key=lambda x: x[1],
            )
        )

        if not dataframe.empty:
            return dataframe.rename(
                columns={
                    0: "SA_ID",
                    1: "UsagePreDER",
                    2: "UsagePostDER",
                    3: "UsageDelta",
                }
            ).set_index("SA_ID")
        else:
            return pd.DataFrame()

    @cached_property
    def bill_report(self):
        """
        Return pandas DataFrame with meter SA IDs and bill deltas.
        """
        dataframe = pd.DataFrame(
            sorted(
                [
                    (
                        x.battery_simulation.meter.sa_id,
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
                    0: "SA_ID",
                    1: "BillPreDER",
                    2: "BillPostDER",
                    3: "BillDelta",
                }
            ).set_index("SA_ID")
        else:
            return pd.DataFrame()

    @cached_property
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

    @cached_property
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

    @cached_property
    def meter_report(self):
        """
        return pandas DataFrame with meter SA IDs and meter RatePlan.
        """
        dataframe = pd.DataFrame(
            self.meters.values_list("sa_id", "rate_plan_name")
        )

        if not dataframe.empty:
            return dataframe.rename(
                columns={0: "SA_ID", 1: "MeterRatePlan"}
            ).set_index("SA_ID")
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
                        x.battery_simulation.meter.sa_id,
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
            name = "{}{}".format(
                ghg_rate.name.replace(" ", ""), ghg_rate.effective.year
            )
            return dataframe.rename(
                columns={
                    0: "SA_ID",
                    1: "{}PreDER".format(name),
                    2: "{}PostDER".format(name),
                    3: "{}Delta".format(name),
                }
            ).set_index("SA_ID")
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
                        x.battery_simulation.meter.sa_id,
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
                    0: "SA_ID",
                    1: "{}PeakPreDER".format(name),
                    2: "{}PeakPostDER".format(name),
                    3: "{}PeakDelta".format(name),
                }
            ).set_index("SA_ID")
        else:
            return pd.DataFrame()

    def run(self, multiprocess=False):
        """
        Run related StoredBatterySimulations, StoredBillCalculations and
        StoredGHGCalculations.

        Note: Meters and GHGRates need to be added to object prior to
        optimization.

        :param multiprocess: True to multiprocess
        """
        # add Meters from CustomerClusters
        self.initialize()

        battery_simulation_set = StoredBatterySimulation.generate(
            battery=self.battery_configuration.battery,
            start=self.start,
            end_limit=self.end_limit,
            meter_set=self.meters.all(),
            charge_schedule=self.charge_schedule.frame288,
            discharge_schedule=self.discharge_schedule.frame288,
            multiprocess=multiprocess,
        )

        StoredBillCalculation.generate(
            battery_simulation_set=battery_simulation_set,
            rate_plan=self.rate_plan,
            multiprocess=multiprocess,
        )

        for ghg_rate in self.ghg_rates.all():
            StoredGHGCalculation.generate(
                battery_simulation_set=battery_simulation_set,
                ghg_rate=ghg_rate,
            )

        for system_profile in self.system_profiles.all():
            StoredResourceAdequacyCalculation.generate(
                battery_simulation_set=battery_simulation_set,
                system_profile=system_profile,
            )

    def filter_by_query(self, query):
        """
        Based on DataFrame query, only matching meters are kept as part of
        the SimulationOptimization. All filtering can be reset using
        initialize().

        Example:
        query = "Bill_Delta > 0" only keeps meters where Bill_Delta is greater
        than 0.
        """
        if self.meters.count() > 0:
            df = ~self.detailed_report.eval(query)
            sa_ids_to_remove = df.index[df == 1].tolist()
            self.meters.remove(
                *Meter.objects.filter(sa_id__in=sa_ids_to_remove)
            )

    def initialize(self):
        """
        Attaches any Meters within attached CustomerClusters. Optimizations
        are performed by removing meters until only the desired Meters are
        attached to a SimulationOptimization. This method allows many
        optimizations to be tried.
        """
        self.meters.clear()
        self.meters.add(*self.customer_cluster_meters)


@receiver(m2m_changed, sender=SimulationOptimization.meters.through)
def reset_cached_properties_update_meters(sender, **kwargs):
    """
    Reset cached properties whenever meters is updated. This resets any cached
    reports.
    """
    simulation_optimization = kwargs.get(
        "instance", SimulationOptimization.objects.none()
    )
    simulation_optimization._reset_cached_properties()


@receiver(m2m_changed, sender=SimulationOptimization.meters.through)
def validate_meters_belong_to_lse(sender, **kwargs):
    """
    If load_serving_entity is set, this validation ensure meters not belonging
    to LSE are not added to SimulationOptimization.
    """
    # get SimulationOptimization and Meters proposed to be added
    simulation_optimization = kwargs.get(
        "instance", SimulationOptimization.objects.none()
    )
    pk_set = kwargs.get("pk_set", {})
    if pk_set is None:
        pk_set = {}

    if (
        kwargs.get("action", None) == "pre_add"
        and simulation_optimization.load_serving_entity
        and Meter.objects.filter(id__in=pk_set).exclude(
            load_serving_entity=simulation_optimization.load_serving_entity
        )
    ):
        raise AttributeError(
            "Meter assignment is limited by those belonging to the "
            "LoadServingEntity."
        )


@receiver(m2m_changed, sender=SimulationOptimization.ghg_rates.through)
def reset_cached_properties_update_ghg_rates(sender, **kwargs):
    """
    Reset cached properties whenever ghg_rates is updated. This resets any
    cached reports.
    """
    simulation_optimization = kwargs.get(
        "instance", SimulationOptimization.objects.none()
    )
    simulation_optimization._reset_cached_properties()


class MultiScenarioOptimization(ValidationModel):
    """
    Container for a multi-scenario simulation optimization.

    Steps to create a simulation optimization:
    1. Create MultiScenarioOptimization objects and related to
       SimulationOptimizations
    2. run() to generate all simulations
    3. filter_by_query() or filter_by_transform() to filter results
    4. display reports
    5. initialize() before running different filter_by_query()
    """

    name = models.CharField(max_length=128, blank=True, null=True)
    simulation_optimizations = models.ManyToManyField(
        to=SimulationOptimization, related_name="multi_scenario_optimizations"
    )

    class Meta:
        ordering = ["id"]

    @property
    def meters(self):
        """
        Return QuerySet of Meters in all self.simulation_optimizations.
        """
        return reduce(
            lambda x, y: x | y,
            [x.meters.all() for x in self.simulation_optimizations.all()],
            Meter.objects.none(),
        ).distinct()

    @property
    def battery_simulations(self):
        """
        Return StoredBatterySimulations related to self.
        """
        return reduce(
            lambda x, y: x | y,
            [
                x.battery_simulations.all()
                for x in self.simulation_optimizations.all()
            ],
            StoredBatterySimulation.objects.none(),
        ).distinct()

    @property
    def bill_calculations(self):
        """
        Return StoredBillCalculations related to self.
        """
        return reduce(
            lambda x, y: x | y,
            [
                x.bill_calculations.all()
                for x in self.simulation_optimizations.all()
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
                for x in self.simulation_optimizations.all()
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
                for x in self.simulation_optimizations.all()
            ],
            StoredResourceAdequacyCalculation.objects.none(),
        ).distinct()

    @cached_property
    def aggregate_battery_intervalframe(self):
        """
        Return ValidationIntervalFrame representing aggregate battery
        operations.
        """
        self.validate_unique_meters()
        return reduce(
            lambda x, y: x + y,
            [
                x.aggregate_battery_intervalframe
                for x in self.simulation_optimizations.all()
            ],
            BatteryIntervalFrame(BatteryIntervalFrame.default_dataframe),
        )

    @cached_property
    def pre_DER_intervalframe(self):
        """
        Return ValidationIntervalFrame representing aggregate pre-DER load.
        """
        self.validate_unique_meters()
        return reduce(
            lambda x, y: x + y,
            [
                x.pre_DER_intervalframe
                for x in self.simulation_optimizations.all()
            ],
            ValidationIntervalFrame(ValidationIntervalFrame.default_dataframe),
        )

    @cached_property
    def post_DER_intervalframe(self):
        """
        Return ValidationIntervalFrame representing aggregate post-DER load.
        """
        self.validate_unique_meters()
        return reduce(
            lambda x, y: x + y,
            [
                x.post_DER_intervalframe
                for x in self.simulation_optimizations.all()
            ],
            ValidationIntervalFrame(ValidationIntervalFrame.default_dataframe),
        )

    @cached_property
    def detailed_report(self):
        """
        Return pandas DataFrame of all simulation_optimizations'
        detailed_report appended together.
        """
        return reduce(
            lambda x, y: x.append(y, sort=False),
            [x.detailed_report for x in self.simulation_optimizations.all()],
            pd.DataFrame(),
        ).sort_index()

    @property
    def detailed_report_html_table(self):
        """
        Return Django-formatted HTML detailed report.
        """
        return dataframe_to_html(self.detailed_report)

    @cached_property
    def report(self):
        """
        Return pandas DataFrame of all simulation_optimizations' report_with_id
        appended together.
        """
        return reduce(
            lambda x, y: x.append(y, sort=False),
            [x.report_with_id for x in self.simulation_optimizations.all()],
            pd.DataFrame(),
        ).sort_index()

    @cached_property
    def energy_loss(self):
        """
        Return all energy lost due to battery roundtrip efficiency.
        """
        return sum(
            [x.energy_loss for x in self.simulation_optimizations.all()]
        )

    def validate_unique_meters(self):
        """
        Validate that each meter appears only once in
        self.simulation_optimizations.
        """
        total_meter_count = reduce(
            lambda x, y: x + y,
            [x.meters.count() for x in self.simulation_optimizations.all()],
        )
        unique_meter_count = (
            reduce(
                lambda x, y: x | y,
                [x.meters.all() for x in self.simulation_optimizations.all()],
                SimulationOptimization.objects.none(),
            )
            .distinct()
            .count()
        )

        if total_meter_count != unique_meter_count:
            raise RuntimeError(
                "Meters in MultiScenarioOptimization are not unique."
                "See: filter_by_transform() to filter meters."
            )

    def run(self, multiprocess=False):
        """
        Run related SimulationOptimizations.

        Note: Meters and GHGRates need to be added to object prior to
        optimization.

        :param multiprocess: True to multiprocess
        """
        for simulation_optimization in self.simulation_optimizations.all():
            simulation_optimization.run(multiprocess=multiprocess)
        self._reset_cached_properties()

    def filter_by_query(self, query):
        """
        Based on DataFrame query, only matching meters are kept as part of
        the simulation_optimizations. All filtering can be reset using
        initialize().

        Example:
        query = "Bill_Delta > 0" only keeps meters where Bill_Delta is greater
        than 0.
        """
        for simulation_optimization in self.simulation_optimizations.all():
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

        # keep only desired meters in each SimulationOptimization
        for simulation_optimization in self.simulation_optimizations.filter(
            id__in=set(self.detailed_report["SimulationOptimization"])
        ):
            id = simulation_optimization.id
            sa_ids = report[report["SimulationOptimization"] == id].index
            meter_ids = list(
                simulation_optimization.meters.filter(
                    sa_id__in=sa_ids
                ).values_list("id", flat=True)
            )
            simulation_optimization.meters.clear()
            simulation_optimization.meters.add(
                *Meter.objects.filter(id__in=meter_ids)
            )
        self._reset_cached_properties()

    def initialize(self):
        """
        Re-attaches any Meters previously associated with related
        SimulationOptimizations.
        """
        for simulation_optimization in self.simulation_optimizations.all():
            simulation_optimization.initialize()
        self._reset_cached_properties()


@receiver(
    m2m_changed,
    sender=MultiScenarioOptimization.simulation_optimizations.through,
)
def reset_cached_properties_update_simulation_optimizations(sender, **kwargs):
    """
    Reset cached properties whenever simulation_optimizations is updated. This
    resets any cached reports.
    """
    simulation_optimization = kwargs.get("instance", None)
    simulation_optimization._reset_cached_properties()
