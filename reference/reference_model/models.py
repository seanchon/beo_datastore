import attr
from datetime import datetime
from enum import Enum
from functools import reduce
import pandas as pd
from typing import List, Set
import uuid

from django.contrib.auth.models import User
from django.core.exceptions import ValidationError
from django.db import models, transaction
from django.utils.functional import cached_property

from navigader_core.der.builder import (
    AggregateDERProduct,
    DER as pyDER,
    DERProduct,
    DERSimulationBuilder,
    DERSimulationDirector,
    DERStrategy as pyDERStrategy,
)
from navigader_core.load.intervalframe import (
    EnergyContainer,
    GasIntervalFrame,
    PowerIntervalFrame,
    ValidationIntervalFrame,
)

from beo_datastore.libs.models import (
    IntervalFrameFileMixin,
    PolymorphicValidationModel,
    TaskStatusModelMixin,
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

    def build_aggregate_metrics(self):
        """
        Computes aggregate metrics on the model only if they have not already
        been computed. The model is expected to have `total_kwh` and
        `max_monthly_demand` fields.
        """
        intervalframe = self.meter_intervalframe
        if self.total_kwh is None:
            self.total_kwh = intervalframe.total

        if self.max_monthly_demand is None:
            self.max_monthly_demand = intervalframe.maximum

        self.save(update_fields=["total_kwh", "max_monthly_demand"])


class MeterGroup(
    PolymorphicValidationModel, MeterDataMixin, TaskStatusModelMixin
):
    """
    Base model containing many Meters.
    """

    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    name = models.CharField(max_length=128, blank=True, null=True)
    created_at = models.DateTimeField(auto_now_add=True)
    max_monthly_demand = models.FloatField(blank=True, null=True)
    owners = models.ManyToManyField(
        to=User, related_name="meter_groups", blank=True
    )
    total_kwh = models.FloatField(blank=True, null=True)

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
    def has_gas(self):
        """
        Returns boolean indicating if the MeterGroup has gas data
        """
        raise NotImplementedError(
            "has_gas must be set in {}".format(self.__class__)
        )

    @property
    def primary_linked_rate_plan_name(self):
        """
        Primary RatePlan associated with MeterGroup.
        """
        raise NotImplementedError(
            "primary_linked_rate_plan_name must be set in {}".format(
                self.__class__
            )
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

    @property
    def years(self):
        """
        Set of all years found in contained Meters' readings.
        """
        return self.intervalframe.years


class Meter(PolymorphicValidationModel, MeterDataMixin):
    """
    Base model containing a linked intervalframe with time-stamped power
    readings.
    """

    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    created_at = models.DateTimeField(auto_now_add=True)
    max_monthly_demand = models.FloatField(blank=True, null=True)
    meter_groups = models.ManyToManyField(
        to=MeterGroup, related_name="meters", blank=True
    )
    total_kwh = models.FloatField(blank=True, null=True)

    class Meta:
        ordering = ["-created_at"]

    @property
    def meter_intervalframe(self) -> PowerIntervalFrame:
        """
        Return PowerIntervalFrame related to a building's load.
        """
        raise NotImplementedError(
            "meter_intervalframe must be set in {}".format(self.__class__)
        )

    @property
    def gas_intervalframe(self) -> GasIntervalFrame:
        """
        Return GasIntervalFrame containing gas usage data
        """
        raise NotImplementedError(
            "gas_intervalframe must be set in {}".format(self.__class__)
        )

    @property
    def energy_container(self) -> EnergyContainer:
        """
        Return EnergyContainer related to meter
        """
        return EnergyContainer(
            kw=self.meter_intervalframe, gas=self.gas_intervalframe
        )

    @property
    def has_gas(self):
        """
        Returns boolean indicating if the Meter has gas data
        """
        raise NotImplementedError(
            "has_gas must be set in {}".format(self.__class__)
        )

    @staticmethod
    def get_report(meters, column_map):
        """
        Return pandas DataFrame in the format:

        |   ID  |   NAME_1   |   NAME_2  |

        Where column_map is a dictionary mapping Django field/property to a
        column name. (The ID column is automatically set.)

        example:
            {
                "field_1": "NAME_1",
                "property_2": "NAME_2"
            }

        :param meters: QuerySet or set of Meters
        :param column_map: dictionary of Django field/property and column names
        :return: pandas DataFrame
        """
        # add id column
        column_map["id"] = "id"

        # extract fields and column_names
        fields = list(column_map.keys())
        column_names = {
            pos: name for pos, name in enumerate(column_map.values())
        }

        dataframe = pd.DataFrame(
            [
                [getattr(meter, field, None) for field in fields]
                for meter in meters
            ]
        )

        if not dataframe.empty:
            return dataframe.rename(columns=column_names).set_index("id")
        else:
            return pd.DataFrame()


# DER BASE MODELS


class DERConfiguration(PolymorphicValidationModel):
    """
    Base model containing particular DER configurations.
    """

    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    name = models.CharField(max_length=128, blank=True, null=True)
    created_at = models.DateTimeField(auto_now_add=True)
    load_serving_entity = models.ForeignKey(
        to=LoadServingEntity,
        related_name="der_configurations",
        on_delete=models.CASCADE,
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
    def der(self) -> pyDER:
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
    load_serving_entity = models.ForeignKey(
        to=LoadServingEntity,
        related_name="der_strategies",
        on_delete=models.CASCADE,
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
    def is_stacked(self) -> bool:
        """
        DERSimulation is not a StackedDERSimulation.
        """
        return False

    @property
    def frame_file_class(self):
        raise NotImplementedError(
            "frame_file_class must be set in {}".format(self.__class__)
        )

    @property
    def rate_plan_name(self) -> str:
        """
        rate_plan_name associated with upstream CustomerMeter.
        """
        return self.meter.rate_plan_name

    @property
    def linked_rate_plans(self):
        """
        linked_rate_plans associated with upstream CustomerMeter.
        """
        return self.meter.linked_rate_plans

    @property
    def sa_id(self):
        """
        sa_id associated with upstream CustomerMeter.
        """
        return self.meter.sa_id

    @cached_property
    def net_impact(self) -> float:
        return self.post_DER_total - self.pre_DER_total

    @cached_property
    def pre_der_intervalframe(self) -> PowerIntervalFrame:
        """
        PowerIntervalFrame before running a DERSimulation.
        """
        return self.meter.meter_intervalframe.filter_by_datetime(
            start=self.start, end_limit=self.end_limit
        )

    @property
    def der_intervalframe(self) -> ValidationIntervalFrame:
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
    def post_der_intervalframe(self) -> PowerIntervalFrame:
        """
        PowerIntervalFrame after running a DERSimulation.
        """
        return self.pre_der_intervalframe + self.der_intervalframe

    @cached_property
    def meter_intervalframe(self) -> PowerIntervalFrame:
        """
        PowerIntervalFrame representing building load after DER has been
        introduced.
        """
        return self.post_der_intervalframe

    @property
    def gas_intervalframe(self) -> GasIntervalFrame:
        return self.meter.gas_intervalframe

    @property
    def has_gas(self):
        return False

    @property
    def pre_vs_post_average_288_html_plot(self):
        """
        Return Django-formatted HTML pre vs. post average 288 plt.
        """
        return plot_frame288_monthly_comparison(
            original_frame288=self.pre_der_intervalframe.average_frame288,
            modified_frame288=self.post_der_intervalframe.average_frame288,
            to_html=True,
        )

    @property
    def pre_vs_post_maximum_288_html_plot(self):
        """
        Return Django-formatted HTML pre vs. post maximum 288 plt.
        """
        return plot_frame288_monthly_comparison(
            original_frame288=self.pre_der_intervalframe.maximum_frame288,
            modified_frame288=self.post_der_intervalframe.maximum_frame288,
            to_html=True,
        )

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
    def simulation(self) -> DERProduct:
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

    @cached_property
    def agg_simulation(self) -> AggregateDERProduct:
        """
        Return AggregateDERProduct equivalent of self.

        AggregateDERProduct with the same parameters can be added to
        one another and can be used for aggregate "cost calculations" found in
        beo_datastore/libs/controller.py.
        """
        return AggregateDERProduct(der_products={self.id: self.simulation})

    @cached_property
    def instream_der_simulations(self) -> List:
        """
        A list of DERSimulation objects representing all historical DER
        transformations.
        """
        if isinstance(self.meter, DERSimulation):
            return self.meter.instream_der_simulations + [self]
        else:
            return [self]

    @cached_property
    def stacked_der_simulation(self):
        """
        Return StackedDERSimulation equivalent of self including all upstream
        DERSimulation objects.
        """
        return StackedDERSimulation(
            der_simulations=self.instream_der_simulations
        )

    @classmethod
    def get_or_create_from_objects(
        cls,
        meter: Meter,
        der_configuration: DERConfiguration,
        der_product: DERProduct,
        der_strategy: DERStrategy,
        start=None,
        end_limit=None,
    ):
        """
        Get existing or create new DERSimulation from a Meter and DERProduct.
        Creates necessary DERConfiguration and DERSchedule objects

        :param meter: Meter
        :param der_configuration: DERConfiguration
        :param der_product: DERProduct
        :param der_strategy: DERStrategy
        :param start: datetime
        :param end_limit: datetime
        :return: (
            DERSimulation,
            DERSimulation created (True/False)
        )
        """
        if start is None:
            start = der_product.der_intervalframe.start_datetime
        if end_limit is None:
            end_limit = der_product.der_intervalframe.end_limit_datetime

        with transaction.atomic():
            return cls.get_or_create(
                start=start,
                end_limit=end_limit,
                meter=meter,
                der_configuration=der_configuration,
                der_strategy=der_strategy,
                pre_DER_total=der_product.pre_der_intervalframe.total,
                post_DER_total=der_product.post_der_intervalframe.total,
                dataframe=der_product.der_intervalframe.dataframe,
            )

    @classmethod
    def get_simulation_builder(
        cls, der: pyDER, der_strategy: pyDERStrategy
    ) -> DERSimulationBuilder:
        """
        Gets the `DERSimulationBuilder` for use in a simulation.
        """
        raise NotImplementedError(
            "get_simulation_builder must be set in {}".format(cls.__class__)
        )

    @classmethod
    def get_intervalframes(cls, meters: Set[Meter]):
        return {meter: meter.meter_intervalframe for meter in meters}

    @classmethod
    def generate(
        cls,
        der_configuration: DERConfiguration,
        der_strategy: DERStrategy,
        start,
        end_limit,
        meter_set,
        multiprocess=False,
    ):
        """
        Get or create many DERSimulations at once. Pre-existing simulations are
        retrieved and non-existing simulations are created.

        :param der_configuration: DERConfiguration
        :param der_strategy: DERStrategy
        :param start: datetime
        :param end_limit: datetime
        :param meter_set: QuerySet or set of Meters
        :param multiprocess: True or False
        :return: simulation QuerySet
        """
        with transaction.atomic():
            # get existing simulations
            stored_simulations = cls.objects.filter(
                meter__id__in=[x.id for x in meter_set],
                der_configuration=der_configuration,
                der_strategy=der_strategy,
                start=start,
                end_limit=end_limit,
            )

            # generate new simulations for remaining meters
            new_meters = set(meter_set) - {x.meter for x in stored_simulations}
            builder = cls.get_simulation_builder(
                der=der_configuration.der,
                der_strategy=der_strategy.der_strategy,
            )
            director = DERSimulationDirector(builder=builder)
            new_simulation = director.run_many_simulations(
                intervalframe_dict=cls.get_intervalframes(meters=new_meters),
                start=start,
                end_limit=end_limit,
                multiprocess=multiprocess,
            )

            # store new simulations
            for (meter, der_simulation) in new_simulation.der_products.items():
                cls.get_or_create_from_objects(
                    meter=meter,
                    der_configuration=der_configuration,
                    der_product=der_simulation,
                    der_strategy=der_strategy,
                    start=start,
                    end_limit=end_limit,
                )

            return cls.objects.filter(
                meter__in=meter_set,
                der_configuration=der_configuration,
                der_strategy=der_strategy,
                start=start,
                end_limit=end_limit,
            )


@attr.s(frozen=True)
class StackedDERSimulation(object):
    """
    A container for sequential DERSimulations.
    """

    der_simulations = attr.ib(type=List[DERSimulation])
    is_stacked = True

    @der_simulations.validator
    def _validate_der_simulations(self, attribute, value):
        """
        Validate that der_simulations contains sequential DERSimulations.
        """
        if len(value) < 1:
            raise AttributeError("der_simulations cannot be empty.")

        for i, _ in enumerate(value[:-1]):
            current = value[i]
            next_ = value[i + 1]

            if current != next_.meter:
                raise AttributeError("DERSimulations are not sequential.")

    @property
    def id(self):
        return self.last_simulation.id

    @property
    def first_simulation(self):
        return self.der_simulations[0]

    @property
    def last_simulation(self):
        return self.der_simulations[-1]

    @property
    def meter(self):
        return self.last_simulation.meter

    @property
    def start(self) -> datetime:
        return self.last_simulation.start

    @property
    def end_limit(self) -> datetime:
        return self.last_simulation.end_limit

    @property
    def der_configuration(self) -> DERConfiguration:
        return self.last_simulation.der_configuration

    @property
    def der_strategy(self) -> DERStrategy:
        return self.last_simulation.der_strategy

    @property
    def pre_DER_total(self) -> float:
        return self.first_simulation.pre_DER_total

    @property
    def post_DER_total(self) -> float:
        return self.last_simulation.post_DER_total

    @property
    def net_impact(self) -> float:
        return self.post_DER_total - self.pre_DER_total

    @property
    def pre_der_intervalframe(self) -> PowerIntervalFrame:
        return self.first_simulation.pre_der_intervalframe.filter_by_datetime(
            start=self.start, end_limit=self.end_limit
        )

    @property
    def der_intervalframe(self) -> PowerIntervalFrame:
        return reduce(
            lambda x, y: x + y,
            [sim.der_intervalframe for sim in self.der_simulations],
            PowerIntervalFrame(),
        )

    @property
    def post_der_intervalframe(self) -> PowerIntervalFrame:
        return self.last_simulation.post_der_intervalframe

    @property
    def simulation(self):
        """
        Return DERProduct equivalent of a StackedDERSimulation using the
        most-recent DER and DERStrategy as placeholder values.
        """
        return DERProduct(
            der=self.der_configuration.der,
            der_strategy=self.der_strategy.der_strategy,
            pre_der_intervalframe=self.pre_der_intervalframe,
            der_intervalframe=self.der_intervalframe,
            post_der_intervalframe=self.post_der_intervalframe,
        )

    @cached_property
    def agg_simulation(self):
        """
        Return AggregateDERProduct equivalent of a StackedDERSimulation.
        """
        return AggregateDERProduct(der_products={self.id: self.simulation})
