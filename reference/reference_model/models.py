from localflavor.us.models import USStateField
from localflavor.us.us_states import STATE_CHOICES
import os
import pandas as pd

from django.contrib.auth.models import User
from django.db import models, transaction
from django.utils.functional import cached_property

from beo_datastore.libs.ingest import get_item_17_dict
from beo_datastore.libs.models import (
    PolymorphicValidationModel,
    ValidationModel,
)
from beo_datastore.libs.plot_intervalframe import (
    plot_intervalframe,
    plot_frame288_monthly_comparison,
)
from beo_datastore.libs.utils import file_md5sum
from beo_datastore.settings import MEDIA_ROOT


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


class OriginFile(ValidationModel):
    """
    File containing customer Meter and Channel data.
    """

    uploaded_at = models.DateTimeField(auto_now_add=True)
    file = models.FileField(upload_to="origin_files")
    md5sum = models.CharField(max_length=32)
    load_serving_entity = models.ForeignKey(
        to=LoadServingEntity,
        related_name="origin_files",
        on_delete=models.PROTECT,
        blank=False,
        null=False,
    )
    owners = models.ManyToManyField(
        to=User, related_name="origin_files", blank=True
    )

    class Meta:
        ordering = ["id"]

    @property
    def file_path(self):
        try:
            return self.file.path
        except NotImplementedError:  # S3Boto3StorageFile
            return os.path.join(MEDIA_ROOT, str(self.file))

    @cached_property
    def dataframe(self):
        return pd.read_csv(self.file_path)

    @cached_property
    def meter_data_dict(self):
        """
        Return CSV file as dict in the following format.

        {
            SA_ID_1: {
                "rate_plan_name": string,
                "import": dataframe,
                "export": dataframe,
            },
            ...
        }
        """
        if self.load_serving_entity:
            if (
                self.load_serving_entity.name
                == "Southern California Edison Co"
            ):
                # TODO: parse SCE data
                pass
            elif (
                self.load_serving_entity.name == "San Diego Gas & Electric Co"
            ):
                # TODO: parse SDG&E data
                pass
            else:  # "Pacific Gas & Electric Co"
                return get_item_17_dict(self.dataframe)

    @classmethod
    def get_or_create(cls, file, load_serving_entity, owner=None):
        """
        Create OriginFile and assign ownership. If OriginFile already exists,
        only assign ownership.

        :param file: file path
        :param load_serving_entity: LoadServingEntity
        :param user: Django User object
        :return: (OriginFile, created)
        """
        with transaction.atomic():
            origin_file = OriginFile(
                load_serving_entity=load_serving_entity, md5sum="0"
            )
            origin_file.file.save(os.path.basename(file.name), file, save=True)
            origin_file.md5sum = file_md5sum(origin_file.file.file)
            origin_file.save()
            if owner:
                origin_file.owners.add(owner)

            # TODO: delete duplicate files
            existing_files = OriginFile.objects.filter(
                load_serving_entity=load_serving_entity,
                md5sum=origin_file.md5sum,
            ).exclude(id=origin_file.id)

            if not existing_files:
                return (origin_file, True)
            else:
                origin_file.delete()
                return (existing_files.first(), False)


class MeterIntervalFrame(PolymorphicValidationModel):
    """
    Model containing a linked intervalframe with time-stamped power readings.
    """

    origin_file = models.ForeignKey(
        to=OriginFile,
        related_name="meter_intervalframes",
        on_delete=models.CASCADE,
        blank=True,
        null=True,
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
