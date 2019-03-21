from localflavor.us.models import USStateField
from localflavor.us.us_states import STATE_CHOICES
import os
import us

from django.core.validators import MinValueValidator, MaxValueValidator
from django.db import models
from django.utils.functional import cached_property

from beo_datastore.libs.intervalframe import IntervalFrame
from beo_datastore.libs.models import ValidationModel
from beo_datastore.settings import MEDIA_ROOT

from reference.reference_unit.models import BuildingType, DataUnit


class ReferenceBuildingQuerySet(models.QuerySet):
    def delete(self, *args, **kwargs):
        """
        Overloads delete() method so that intervalframes are deleted from disk
        along with ReferenceBuilding instances.
        """
        # TODO: Create a quicker cleanup method.
        for obj in self:
            obj.intervalframe.delete()
        super(ReferenceBuildingQuerySet, self).delete(*args, **kwargs)


class ReferenceBuilding(ValidationModel):
    location = models.CharField(max_length=64, blank=False)
    state = USStateField(choices=STATE_CHOICES, blank=True)
    TMY3_id = models.IntegerField(
        db_index=True,
        validators=[MinValueValidator(100000), MaxValueValidator(999999)],
    )
    source_file_url = models.URLField(max_length=254)
    building_type = models.ForeignKey(
        BuildingType,
        related_name="reference_buildings",
        on_delete=models.PROTECT,
    )
    data_unit = models.ForeignKey(
        DataUnit,
        related_name="reference_buildings",
        on_delete=models.PROTECT
    )

    # custom QuerySet manager for intervalframe file-handling
    objects = ReferenceBuildingQuerySet.as_manager()

    def __str__(self):
        return self.building_type.name + ": " + self.location

    def save(self, *args, **kwargs):
        if self.intervalframe:
            self.intervalframe.save()
        super().save(*args, **kwargs)

    def delete(self, *args, **kwargs):
        if self.intervalframe:
            self.intervalframe.delete()
        super().delete(*args, **kwargs)

    @property
    def timezone(self):
        return us.states.lookup(self.state).capital_tz

    @cached_property
    def source_file_intervalframe(self):
        """
        Creates IntervalFrame from csv_url.
        """
        return ReferenceBuildingIntervalFrame.csv_url_to_intervalframe(
            ref_object=self,
            csv_url=self.source_file_url,
            index_column="Date/Time",
        )

    @cached_property
    def parquet_file_intervalframe(self):
        """
        Creates IntervalFrame from local parquet copy.
        """
        return ReferenceBuildingIntervalFrame.get_parquet_intervalframe(
            ref_object=self
        )

    @property
    def intervalframe(self):
        """
        Returns IntervalFrame sourced from the source_file_url or cached
        locally.
        """
        if self.parquet_file_intervalframe:
            self._intervalframe = self.parquet_file_intervalframe
        else:
            self._intervalframe = self.source_file_intervalframe

        return self._intervalframe

    @intervalframe.setter
    def intervalframe(self, intervalframe):
        """
        Assigns intervalframe to self._intervalframe. Writes to disk on save().
        """
        self._intervalframe = intervalframe

    @cached_property
    def electricity_facility_288_average(self):
        # TODO: save to disk if used often
        return self.intervalframe.get_288_matrix(
            "Electricity:Facility [kW](Hourly)", "average"
        )

    @cached_property
    def electricity_facility_288_maximum(self):
        # TODO: save to disk if used often
        return self.intervalframe.get_288_matrix(
            "Electricity:Facility [kW](Hourly)", "maximum"
        )

    @cached_property
    def electricity_facility_288_count(self):
        # TODO: save to disk if used often
        return self.intervalframe.get_288_matrix(
            "Electricity:Facility [kW](Hourly)", "count"
        )


class ReferenceBuildingIntervalFrame(IntervalFrame):
    file_directory = os.path.join(MEDIA_ROOT, "reference_buildings")
    file_prefix = "rb_"

    def filter_dataframe(
        self, column, month=None, day=None, hour=None, *args, **kwargs
    ):
        """
        Returns self.dataframe filtered by column, month, day, and hour.
        """
        if column:
            dataframe = self.dataframe[[column]]
        else:
            dataframe = self.dataframe

        if month is not None:
            month = str(month).zfill(2)
        else:
            month = "\d\d"
        if day is not None:
            day = str(day).zfill(2)
        else:
            day = "\d\d"
        if hour is not None:
            hour += 1  # TODO: investigate OpenEI time formatting
            hour = str(hour).zfill(2)
        else:
            hour = "\d\d"

        search_string = "{}\/{}  {}".format(month, day, hour)
        dataframe = dataframe[dataframe.index.str.contains(search_string)]

        return dataframe
