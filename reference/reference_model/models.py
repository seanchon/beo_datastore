from localflavor.us.models import USStateField
from localflavor.us.us_states import STATE_CHOICES
import uuid

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


class MeterDataMixin(object):
    """
    Properties for accessing and displaying meter data.
    """

    @property
    def dataframe(self):
        return self.intervalframe.dataframe

    @property
    def intervalframe_html_plot(self):
        """
        Return Django-formatted HTML intervalframe plt.
        """
        return plot_intervalframe(
            intervalframe=self.intervalframe, y_label="kw", to_html=True
        )

    @property
    def average_vs_maximum_html_plot(self):
        """
        Return Django-formatted HTML average vs maximum 288 plt.
        """
        return plot_frame288_monthly_comparison(
            original_frame288=self.intervalframe.average_frame288,
            modified_frame288=self.intervalframe.maximum_frame288,
            to_html=True,
        )

    @property
    def total_288(self):
        """
        Return a 12 x 24 dataframe of totals (sums).
        """
        return self.intervalframe.total_frame288.dataframe

    @property
    def average_288(self):
        """
        Return a 12 x 24 dataframe of averages.
        """
        return self.intervalframe.average_frame288.dataframe

    @property
    def peak_288(self):
        """
        Return a 12 x 24 dataframe of peaks.
        """
        return self.intervalframe.minimum_frame288.dataframe

    @property
    def count_288(self):
        """
        Return a 12 x 24 dataframe of counts.
        """
        return self.intervalframe.count_frame288.dataframe


class MeterGroup(PolymorphicValidationModel, MeterDataMixin):
    """
    Model many Meters.
    """

    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    name = models.CharField(max_length=128, blank=True, null=True)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ["id"]

    @property
    def meter_count(self):
        return self.meters.count()

    @property
    def meter_group_type(self):
        """
        String representation of ctype minus spaces.
        """
        return self.polymorphic_ctype.name.replace(" ", "")

    @property
    def intervalframe(self):
        raise NotImplementedError()


class Meter(PolymorphicValidationModel, MeterDataMixin):
    """
    Model containing a linked intervalframe with time-stamped power readings.
    """

    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    created_at = models.DateTimeField(auto_now_add=True)
    meter_groups = models.ManyToManyField(
        to=MeterGroup, related_name="meters", blank=True
    )

    class Meta:
        ordering = ["id"]

    @property
    def meter_type(self):
        """
        String representation of ctype minus spaces.
        """
        return self.polymorphic_ctype.name.replace(" ", "")

    @property
    def intervalframe(self):
        raise NotImplementedError()


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
