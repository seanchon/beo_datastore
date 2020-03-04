from localflavor.us.models import USStateField
from localflavor.us.us_states import STATE_CHOICES
import uuid

from django.core.exceptions import ValidationError
from django.db import models

from beo_datastore.libs.models import (
    PolymorphicValidationModel,
    ValidationModel,
)
from beo_datastore.libs.plot_intervalframe import (
    plot_intervalframe,
    plot_frame288_monthly_comparison,
)


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


class LoadServingEntity(ValidationModel):
    """
    Load serving entity (ex. Utility, CCA).
    """

    name = models.CharField(max_length=32, unique=True)
    short_name = models.CharField(max_length=8, unique=False)
    state = USStateField(choices=STATE_CHOICES)
    _parent_utility = models.ForeignKey(
        to="LoadServingEntity",
        related_name="load_serving_entities",
        on_delete=models.PROTECT,
        blank=True,
        null=True,
    )

    class Meta:
        ordering = ["id"]
        verbose_name_plural = "load serving entities"

    def __str__(self):
        return self.name

    @property
    def parent_utility(self):
        if self._parent_utility:
            return self._parent_utility
        else:
            return self

    @parent_utility.setter
    def parent_utility(self, parent_utility):
        self._parent_utility = parent_utility

    @classmethod
    def menu(cls):
        """
        Return a list of IDs and LoadServingEntity names. This menu is used in
        various scripts that require a LoadServingEntity as an input.
        """
        return "\n".join(
            [
                "ID: {} NAME: {}".format(x[0], x[1])
                for x in cls.objects.values_list("id", "name")
            ]
        )


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

    class Meta:
        ordering = ["-created_at"]

    @property
    def meter_count(self):
        return self.meters.count()

    @property
    def meter_intervalframe(self):
        """
        Return ValidationIntervalFrame related to a group of buildings' load.
        """
        raise NotImplementedError(
            "meter_intervalframe must be set in {}".format(self.__class__)
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
        Return ValidationIntervalFrame related to a building's load.
        """
        raise NotImplementedError(
            "meter_intervalframe must be set in {}".format(self.__class__)
        )


# DER BASE MODELS


class DERConfiguration(PolymorphicValidationModel):
    """
    Base model containing particular DER configurations.

    ex. Battery rating and duration.
    """

    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    name = models.CharField(max_length=128, blank=True, null=True)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ["created_at"]

    @property
    def der_type(self):
        """
        A DERConfiguration.der_type must match a DERStrategy.der_type when
        creating a DERSimulation.
        """
        raise NotImplementedError(
            "der_type must be set in {}".format(self.__class__)
        )

    @property
    def configuration(self):
        """
        Return dictionary containing configuration values.
        """
        raise NotImplementedError(
            "configuration must be set in {}".format(self.__class__)
        )


class DERStrategy(PolymorphicValidationModel):
    """
    Base model containing particular DER strategies.

    ex. Battery charge with solar and discharge evening load.
    """

    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    name = models.CharField(max_length=128, blank=True, null=True)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ["-created_at"]

    @property
    def der_type(self):
        """
        A DERConfiguration.der_type must match a DERStrategy.der_type when
        creating a DERSimulation.
        """
        raise NotImplementedError(
            "der_type must be set in {}".format(self.__class__)
        )

    @property
    def strategy(self):
        """
        Return dictionary containing strategy values.
        """
        raise NotImplementedError(
            "strategy must be set in {}".format(self.__class__)
        )


class DERSimulation(Meter):
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
    def meter_intervalframe(self):
        """
        Return ValidationIntervalFrame related to a simulated building's load.
        This is what a building's electricity meter would read after a
        hypothetical DER is installed.
        """
        raise NotImplementedError(
            "meter_intervalframe must be set in {}".format(self.__class__)
        )

    @property
    def der_intervalframe(self):
        """
        Return ValidationIntervalFrame related to a DER's impact to a
        building's load. The original building's load plus the
        der_intervalframe would yield the meter_intervalframe.
        """
        raise NotImplementedError(
            "der_intervalframe must be set in {}".format(self.__class__)
        )

    @property
    def der_columns(self):
        """
        Return columns from der_intervalframe for use in frame288 computation.
        """
        return self.der_intervalframe.dataframe.columns
