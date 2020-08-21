from enum import Enum
from functools import reduce
import pandas as pd
import uuid

from django.contrib.auth.models import User
from django.core.exceptions import ValidationError
from django.db import models, transaction
from django.utils.functional import cached_property

from beo_datastore.libs.der.builder import (
    AggregateDERProduct,
    DER,
    DERProduct,
    DERSimulationBuilder,
    DERSimulationDirector,
    DERStrategy as pyDERStrategy,
)
from beo_datastore.libs.models import (
    IntervalFrameFileMixin,
    PolymorphicValidationModel,
    ValidationModel,
)
from beo_datastore.libs.plot_intervalframe import (
    plot_intervalframe,
    plot_frame288_monthly_comparison,
)

from reference.auth_user.models import LoadServingEntity


class BuildingType(ValidationModel):
    """
    U.S. Department of Energy Commercial Reference Buildings.

    https://www.energy.gov/eere/buildings/commercial-reference-buildings
    """

    name = models.CharField(max_length=32)
    floor_area = models.IntegerField(blank=False)
    number_of_floors = models.IntegerField(blank=False)

    class Meta:
        ordering = ["id"]
        unique_together = ("name", "floor_area", "number_of_floors")

    def __str__(self):
        return self.name


class DataUnit(ValidationModel):
    """
    Units of measure.

    Ex. kw, kwh, therms
    """

    name = models.CharField(max_length=8, unique=True)

    class Meta:
        ordering = ["id"]

    def __str__(self):
        return self.name


class RateUnit(ValidationModel):
    """
    Units of rate.

    Ex. $/kw, $/day, tCO2/kwh
    """

    numerator = models.ForeignKey(
        to=DataUnit,
        related_name="rate_unit_numerators",
        on_delete=models.PROTECT,
    )
    denominator = models.ForeignKey(
        to=DataUnit,
        related_name="rate_unit_denominators",
        on_delete=models.PROTECT,
    )

    class Meta:
        ordering = ["id"]
        unique_together = ("numerator", "denominator")

    def __str__(self):
        return "{}/{}".format(self.numerator, self.denominator)


class VoltageCategory(ValidationModel):
    """
    Utility's Customer Voltage Category.
    """

    name = models.CharField(max_length=32)
    load_serving_entity = models.ForeignKey(
        to=LoadServingEntity,
        related_name="voltage_categories",
        on_delete=models.PROTECT,
    )

    class Meta:
        ordering = ["id"]
        unique_together = ("name", "load_serving_entity")

    def __str__(self):
        return self.name


class Sector(ValidationModel):
    """
    Classification of LSE customer.

    Ex. Residential, Commercial
    """

    name = models.CharField(max_length=32)
    load_serving_entity = models.ForeignKey(
        to=LoadServingEntity, related_name="sectors", on_delete=models.PROTECT
    )

    class Meta:
        ordering = ["id"]
        unique_together = ("name", "load_serving_entity")

    def __str__(self):
        return self.name


# LOAD BASE MODELS


class MeterDataMixin(object):
    """
    Properties for accessing and displaying meter data.
    """

    @property
    def dataframe(self):
        return self.meter_intervalframe.dataframe

    @property
    def intervalframe_html_plot(self):
        """
        Return Django-formatted HTML intervalframe plt.
        """
        return plot_intervalframe(
            intervalframe=self.meter_intervalframe, y_label="kw", to_html=True
        )

    @property
    def average_vs_maximum_html_plot(self):
        """
        Return Django-formatted HTML average vs maximum 288 plt.
        """
        return plot_frame288_monthly_comparison(
            original_frame288=self.meter_intervalframe.average_frame288,
            modified_frame288=self.meter_intervalframe.maximum_frame288,
            to_html=True,
        )

    @property
    def total_288(self):
        """
        Return a 12 x 24 dataframe of totals (sums).
        """
        return self.meter_intervalframe.total_frame288.dataframe

    @property
    def average_288(self):
        """
        Return a 12 x 24 dataframe of averages.
        """
        return self.meter_intervalframe.average_frame288.dataframe

    @property
    def peak_288(self):
        """
        Return a 12 x 24 dataframe of peaks.
        """
        return self.meter_intervalframe.minimum_frame288.dataframe

    @property
    def count_288(self):
        """
        Return a 12 x 24 dataframe of counts.
        """
        return self.meter_intervalframe.count_frame288.dataframe


class MeterGroup(PolymorphicValidationModel, MeterDataMixin):
    """
    Base model containing many Meters.
    """

    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    name = models.CharField(max_length=128, blank=True, null=True)
    created_at = models.DateTimeField(auto_now_add=True)
    owners = models.ManyToManyField(
        to=User, related_name="meter_groups", blank=True
    )

    class Meta:
        ordering = ["-created_at"]

    @property
    def meter_count(self):
        return self.meters.count()

    @property
    def meter_intervalframe(self):
        """
        Return PowerIntervalFrame related to a group of buildings' load.
        """
        raise NotImplementedError(
            "meter_intervalframe must be set in {}".format(self.__class__)
        )

    @property
    def primary_linked_rate_plan(self):
        """
        Primary RatePlan associated with MeterGroup.
        """
        raise NotImplementedError(
            "primary_linked_rate_plan must be set in {}".format(self.__class__)
        )

    @cached_property
    def start(self):
        """
        First timestamp in all contained Meters' readings.
        """
        return min(
            meter.intervalframe.dataframe.index[0]
            for meter in self.meters.all()
        )

    @cached_property
    def end(self):
        """
        Last timestamp in all contained Meters' reading.
        """
        return max(
            meter.intervalframe.dataframe.index[-1]
            for meter in self.meters.all()
        )

    @cached_property
    def years(self):
        """
        Set of all years found in contained Meters' readings.
        """
        return reduce(
            lambda x, y: x.union(y),
            [
                set(meter.intervalframe.dataframe.index.year)
                for meter in self.meters.all()
            ],
            set(),
        )


class Meter(PolymorphicValidationModel, MeterDataMixin):
    """
    Base model containing a linked intervalframe with time-stamped power
    readings.
    """

    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    created_at = models.DateTimeField(auto_now_add=True)
    meter_groups = models.ManyToManyField(
        to=MeterGroup, related_name="meters", blank=True
    )

    class Meta:
        ordering = ["-created_at"]

    @property
    def meter_intervalframe(self):
        """
        Return PowerIntervalFrame related to a building's load.
        """
        raise NotImplementedError(
            "meter_intervalframe must be set in {}".format(self.__class__)
        )


# DER BASE MODELS


class DERConfiguration(PolymorphicValidationModel):
    """
    Base model containing particular DER configurations.
    """

    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    name = models.CharField(max_length=128, blank=True, null=True)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ["-created_at"]

    @property
    def der_type(self) -> str:
        """
        A DERConfiguration.der_type must match a DERStrategy.der_type when
        creating a DERSimulation.
        """
        raise NotImplementedError(
            "der_type must be set in {}".format(self.__class__)
        )

    @property
    def der(self) -> DER:
        """
        Return Python DER model equivalent of self.
        """
        raise NotImplementedError(
            "der must be set in {}".format(self.__class__)
        )


class DERStrategy(PolymorphicValidationModel):
    """
    Base model containing particular DER strategies.
    """

    class Objective(Enum):
        load_flattening = "Load Flattening"
        reduce_bill = "Reduce Bill"
        reduce_ghg = "Reduce GHG"
        reduce_cca_finance = "Minimize CCA Financial Impacts"

    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    name = models.CharField(max_length=128, blank=True, null=True)
    description = models.TextField(blank=True, null=True)
    created_at = models.DateTimeField(auto_now_add=True)
    objective = models.CharField(
        max_length=18,
        choices=[(item.name, item.value) for item in Objective],
        blank=True,
        null=True,
    )

    class Meta:
        ordering = ["-created_at"]

    @property
    def der_type(self) -> str:
        """
        A DERConfiguration.der_type must match a DERStrategy.der_type when
        creating a DERSimulation.
        """
        raise NotImplementedError(
            "der_type must be set in {}".format(self.__class__)
        )

    @property
    def der_strategy(self) -> pyDERStrategy:
        """
        Returns a `pyDERStrategy` that the `DERStrategy` model wraps
        """
        raise NotImplementedError(
            "der_strategy must be set in {}".format(self.__class__)
        )


class DERSimulation(IntervalFrameFileMixin, Meter):
    """
    Base model containing simulated load resulting from DER simulations.
    """

    start = models.DateTimeField()
    end_limit = models.DateTimeField()
    meter = models.ForeignKey(
        to=Meter,
        on_delete=models.CASCADE,
        related_name="der_simulations",
        blank=False,
        null=False,
    )
    der_configuration = models.ForeignKey(
        to=DERConfiguration,
        on_delete=models.CASCADE,
        related_name="der_simulations",
        blank=False,
        null=False,
    )
    der_strategy = models.ForeignKey(
        to=DERStrategy,
        on_delete=models.CASCADE,
        related_name="der_simulations",
        blank=True,
        null=True,
    )
    pre_DER_total = models.FloatField()
    post_DER_total = models.FloatField()

    class Meta:
        ordering = ["-created_at"]
        unique_together = (
            "start",
            "end_limit",
            "meter",
            "der_configuration",
            "der_strategy",
        )

    def clean(self, *args, **kwargs):
        """
        Constrain related DERConfiguration and DERStrategy to be of the same
        type.
        """
        if self.der_strategy and (
            self.der_configuration.der_type != self.der_strategy.der_type
        ):
            raise ValidationError(
                "der_configuration.der_type must match der_strategy.der_type"
            )
        super().clean(*args, **kwargs)

    @property
    def frame_file_class(self):
        raise NotImplementedError(
            "frame_file_class must be set in {}".format(self.__class__)
        )

    @cached_property
    def net_impact(self):
        return self.post_DER_total - self.pre_DER_total

    @cached_property
    def pre_der_intervalframe(self):
        """
        PowerIntervalFrame before running a DERSimulation.
        """
        return self.meter.meter_intervalframe

    @property
    def der_intervalframe(self):
        """
        ValidationIntervalFrame representing all DER operations.
        """
        return self.intervalframe

    @property
    def der_columns(self):
        """
        Columns from der_intervalframe for use in frame288 computation.
        """
        return self.der_intervalframe.dataframe.columns

    @cached_property
    def post_der_intervalframe(self):
        """
        PowerIntervalFrame after running a DERSimulation.
        """
        return (
            self.meter.intervalframe.filter_by_datetime(
                start=self.start, end_limit=self.end_limit
            )
            + self.intervalframe
        )

    @cached_property
    def meter_intervalframe(self):
        """
        PowerIntervalFrame representing building load after DER has been
        introduced.
        """
        return self.post_der_intervalframe

    @staticmethod
    def get_report(der_simulations):
        """
        Return pandas DataFrame in the format:

        |   ID  |   UsagePreDER |   UsagePostDER    |   UsageDelta  |

        :param der_simulations: QuerySet or set of DERSimulations
        :return: pandas DataFrame
        """
        # TODO: handle case with same Meter, multiple DERSimulations
        dataframe = pd.DataFrame(
            sorted(
                [
                    (
                        x.meter.id,
                        x.pre_DER_total,
                        x.post_DER_total,
                        x.net_impact,
                    )
                    for x in der_simulations
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

    @cached_property
    def simulation(self):
        """
        Return DERProduct equivalent of self.
        """
        pre_der_intervalframe = self.meter.intervalframe.filter_by_datetime(
            start=self.start, end_limit=self.end_limit
        )

        return DERProduct(
            der=self.der_configuration.der,
            der_strategy=self.der_strategy.der_strategy,
            pre_der_intervalframe=pre_der_intervalframe,
            der_intervalframe=self.intervalframe,
            post_der_intervalframe=(
                pre_der_intervalframe + self.intervalframe
            ),
        )

    @cached_property
    def agg_simulation(self):
        """
        Return AggregateDERProduct equivalent of self.

        AggregateDERProduct with the same parameters can be added to
        one another and can be used for aggregate "cost calculations" found in
        beo_datastore/libs/controller.py.
        """
        return AggregateDERProduct(der_products={self.id: self.simulation})

    @classmethod
    def get_or_create_from_objects(
        cls, meter, der_product, start=None, end_limit=None
    ):
        """
        Get existing or create new DERSimulation from a Meter and DERProduct.
        Creates necessary DERConfiguration and DERSchedule objects

        :param meter: Meter
        :param der_product: DERProduct
        :param start: datetime
        :param end_limit: datetime
        :return: (
            StoredBatterySimulation,
            StoredBatterySimulation created (True/False)
        )
        """
        if start is None:
            start = der_product.der_intervalframe.start_datetime
        if end_limit is None:
            end_limit = der_product.der_intervalframe.end_limit_datetime

        with transaction.atomic():
            configuration = cls.get_configuration(der_product.der)
            der_strategy = cls.get_strategy(der_product.der_strategy)

            pre_total_frame288 = (
                der_product.pre_der_intervalframe.total_frame288
            )

            pre_der_total = pre_total_frame288.dataframe.sum().sum()
            post_total_frame288 = (
                der_product.post_der_intervalframe.total_frame288
            )
            post_der_total = post_total_frame288.dataframe.sum().sum()

            return cls.get_or_create(
                start=start,
                end_limit=end_limit,
                meter=meter,
                der_configuration=configuration,
                der_strategy=der_strategy,
                pre_DER_total=pre_der_total,
                post_DER_total=post_der_total,
                dataframe=der_product.der_intervalframe.dataframe,
            )

    @classmethod
    def get_configuration(cls, der: DER) -> DERConfiguration:
        """
        Gets a `DERConfiguration` for use in a simulation, given the DER python
        model that the configuration wraps
        """
        raise NotImplementedError(
            "get_configuration must be set in {}".format(cls.__class__)
        )

    @classmethod
    def get_strategy(cls, der_strategy: pyDERStrategy) -> DERStrategy:
        """
        Gets a `DERStrategy` for use in a simulation, given the DERStrategy
        python model that the strategy wraps
        """
        raise NotImplementedError(
            "get_strategy must be set in {}".format(cls.__class__)
        )

    @classmethod
    def get_simulation_builder(
        cls, der: DER, der_strategy: DERStrategy
    ) -> DERSimulationBuilder:
        """
        Gets the `DERSimulationBuilder` for use in a simulation.
        """
        raise NotImplementedError(
            "get_simulation_builder must be set in {}".format(cls.__class__)
        )

    @classmethod
    def generate(
        cls,
        der: DER,
        der_strategy: pyDERStrategy,
        start,
        end_limit,
        meter_set,
        multiprocess=False,
    ):
        """
        Get or create many DERSimulations at once. Pre-existing simulations are
        retrieved and non-existing simulations are created.

        :param der: DER
        :param der_strategy: pyDERStrategy
        :param start: datetime
        :param end_limit: datetime
        :param meter_set: QuerySet or set of Meters
        :param multiprocess: True or False
        :return: simulation QuerySet
        """
        with transaction.atomic():
            configuration = cls.get_configuration(der)
            strategy = cls.get_strategy(der_strategy)

            # get existing simulations
            stored_simulations = cls.objects.filter(
                meter__id__in=[x.id for x in meter_set],
                der_configuration=configuration,
                der_strategy=strategy,
                start=start,
                end_limit=end_limit,
            )

            # generate new simulations for remaining meters
            new_meters = set(meter_set) - {x.meter for x in stored_simulations}
            builder = cls.get_simulation_builder(
                der=der, der_strategy=strategy
            )
            director = DERSimulationDirector(builder=builder)
            new_simulation = director.run_many_simulations(
                intervalframe_dict={
                    meter: meter.intervalframe for meter in new_meters
                },
                start=start,
                end_limit=end_limit,
                multiprocess=multiprocess,
            )

            # store new simulations
            for (meter, der_simulation) in new_simulation.der_products.items():
                cls.get_or_create_from_objects(
                    meter=meter,
                    der_product=der_simulation,
                    start=start,
                    end_limit=end_limit,
                )

            return cls.objects.filter(
                meter__in=meter_set,
                der_configuration=configuration,
                der_strategy=strategy,
                start=start,
                end_limit=end_limit,
            )


# STUDY BASE MODELS


class Study(PolymorphicValidationModel, MeterDataMixin):
    """
    Base model containing many meters run under DER simulations and cost
    calculations.
    """

    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    name = models.CharField(max_length=128, blank=True, null=True)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ["-created_at"]

    @property
    def meter_intervalframe(self):
        """
        PowerIntervalFrame representing buildings' load after DER has been
        introduced.
        """
        raise NotImplementedError(
            "meter_intervalframe must be set in {}".format(self.__class__)
        )

    @property
    def pre_der_intervalframe(self):
        """
        PowerIntervalFrame representing aggregate readings of all meters
        before running DER simulations.
        """
        raise NotImplementedError(
            "pre_der_intervalframe must be set in {}".format(self.__class__)
        )

    @property
    def der_intervalframe(self):
        """
        PowerIntervalFrame representing aggregate readings of all DER
        operations.
        """
        raise NotImplementedError(
            "der_intervalframe must be set in {}".format(self.__class__)
        )

    @property
    def post_der_intervalframe(self):
        """
        PowerIntervalFrame representing aggregate readings of all meters
        after running DER simulations.
        """
        raise NotImplementedError(
            "post_der_intervalframe must be set in {}".format(self.__class__)
        )

    @property
    def meters(self):
        """
        Associated Meter queryset.
        """
        raise NotImplementedError(
            "meters must be set in {}".format(self.__class__)
        )

    @property
    def meter_groups(self):
        """
        Associated MeterGroup queryset.
        """
        raise NotImplementedError(
            "meter_groups must be set in {}".format(self.__class__)
        )

    @property
    def meter_count(self):
        return self.meters.count()

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
        raise NotImplementedError(
            "ders must be set in {}".format(self.__class__)
        )

    @property
    def der_simulations(self):
        """
        Associated DERSimulation queryset.
        """
        raise NotImplementedError(
            "der_simulations must be set in {}".format(self.__class__)
        )

    @property
    def der_simulation_count(self):
        """
        Number of created DERSimulation objects.
        """
        return self.der_simulations.count()

    @property
    def expected_der_simulation_count(self):
        """
        Number of expected DERSimulation objects.
        """
        raise NotImplementedError(
            "expected_der_simulation_count must be set in {}".format(
                self.__class__
            )
        )

    @property
    def report(self):
        """
        Return pandas Dataframe with meter SA IDs and all cost impacts.
        """
        raise NotImplementedError(
            "report must be set in {}".format(self.__class__)
        )

    @property
    def detailed_report(self):
        """
        Return pandas Dataframe with meter SA IDs, DERConfiguration details,
        RatePlan details, and all cost impacts.
        """
        raise NotImplementedError(
            "report must be set in {}".format(self.__class__)
        )
