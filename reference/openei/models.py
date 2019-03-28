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
    """
    Overloads QuerySet operations for bulk file-handling.
    """

    def delete(self, *args, **kwargs):
        """
        Bulk delete IntervalFrame files from disk.
        """
        # TODO: Create a quicker cleanup method.
        for obj in self:
            obj.intervalframe.delete()
        super(ReferenceBuildingQuerySet, self).delete(*args, **kwargs)


class ReferenceBuilding(ValidationModel):
    """
    OpenEI: Commercial and Residential Hourly Load Profiles for all TMY3
    Locations in the United States.

    Source: https://openei.org/doe-opendata/dataset/commercial-and-residential
        -hourly-load-profiles-for-all-tmy3-locations-in-the-united-states

    Based on Typical Meteorological Year 3 (TMY3) data.

    Source: https://rredc.nrel.gov/solar/old_data/nsrdb/1991-2005/tmy3/
    """

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
        DataUnit, related_name="reference_buildings", on_delete=models.PROTECT
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
            reference_object=self,
            csv_url=self.source_file_url,
            index_column="Date/Time",
        )

    @cached_property
    def intervalframe_from_file(self):
        """
        Creates IntervalFrame from local parquet copy.
        """
        return ReferenceBuildingIntervalFrame.get_frame_from_file(
            reference_object=self
        )

    @property
    def intervalframe(self):
        """
        Returns IntervalFrame sourced from the source_file_url or cached
        locally.
        """
        if self.intervalframe_from_file:
            self._intervalframe = self.intervalframe_from_file
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
    def average_288_dataframe(self):
        # TODO: save to disk if used often
        return self.intervalframe.get_288_matrix(
            "Electricity:Facility [kW](Hourly)", "average"
        )

    @cached_property
    def maximum_288_dataframe(self):
        # TODO: save to disk if used often
        return self.intervalframe.get_288_matrix(
            "Electricity:Facility [kW](Hourly)", "maximum"
        )

    @cached_property
    def count_288_dataframe(self):
        # TODO: save to disk if used often
        return self.intervalframe.get_288_matrix(
            "Electricity:Facility [kW](Hourly)", "count"
        )


class ReferenceBuildingIntervalFrame(IntervalFrame):
    """
    Model for handling ReferenceBuilding IntervalFrames, which have timestamps
    with ambiguous year as well as multiple columns representing energy usage
    in various categories (ex. facility, lights, HVAC, etc.).
    """

    reference_model = ReferenceBuilding
    file_directory = os.path.join(MEDIA_ROOT, "reference_buildings")

    @staticmethod
    def validate_dataframe(dataframe):
        """
        Disable dataframe validation due to index missing year and extra
        columns of values.
        """
        pass

    @staticmethod
    def mask_dataframe_date(
        dataframe, month=None, day=None, hour=None, *args, **kwargs
    ):
        """
        Returns dataframe masked to match month, day, and/or hour.

        ReferenceBuilding timestamps are irregular and contain no year and use
        hours 1 to 24 versus 0 to 23.
        """
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
