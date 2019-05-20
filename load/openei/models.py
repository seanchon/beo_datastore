from datetime import datetime
from localflavor.us.models import USStateField
from localflavor.us.us_states import STATE_CHOICES
import os
import pandas as pd
import us

from django.core.validators import MinValueValidator, MaxValueValidator
from django.db import models
from django.utils.functional import cached_property

from beo_datastore.libs.dataframe import csv_url_to_dataframe
from beo_datastore.libs.intervalframe import (
    IntervalFrameFile,
    ValidationIntervalFrame,
)
from beo_datastore.libs.models import ValidationModel
from beo_datastore.settings import MEDIA_ROOT

from reference.reference_model.models import BuildingType, DataUnit


class ReferenceBuildingQuerySet(models.QuerySet):
    """
    Overloads QuerySet operations for bulk file-handling.
    """

    def delete(self, *args, **kwargs):
        """
        Bulk delete IntervalFrameFile files from disk.
        """
        # TODO: Create a quicker cleanup method.
        for obj in self:
            obj.intervalframe.delete()
        super().delete(*args, **kwargs)


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

    class Meta:
        ordering = ["id"]

    def save(self, *args, **kwargs):
        if hasattr(self, "_intervalframe"):
            self._intervalframe.save()
        super().save(*args, **kwargs)

    def delete(self, *args, **kwargs):
        if hasattr(self, "_lookup_table"):
            self._intervalframe.delete()
        super().delete(*args, **kwargs)

    @property
    def timezone(self):
        return us.states.lookup(self.state).capital_tz

    @cached_property
    def source_file_intervalframe(self):
        """
        Creates IntervalFrameFile from csv_url.
        """
        dataframe = csv_url_to_dataframe(self.source_file_url)

        # add 2018 datetime column
        dataframe["start"] = pd.date_range(
            start=datetime(2018, 1, 1),
            end=datetime(2018, 12, 31, 23),
            freq="3600S",
        )
        dataframe.set_index("start", inplace=True)

        return ReferenceBuildingIntervalFrame(
            reference_object=self, dataframe=dataframe
        )

    @cached_property
    def intervalframe_from_file(self):
        """
        Creates IntervalFrameFile from local parquet copy.
        """
        return ReferenceBuildingIntervalFrame.get_frame_from_file(
            reference_object=self
        )

    @property
    def full_intervalframe(self):
        """
        Returns IntervalFrameFile sourced from the source_file_url or cached
        locally. If sourced from source_file_url, performs save() to disk.
        """
        if not hasattr(self, "_intervalframe"):
            if self.intervalframe_from_file:
                # attempt to retrieve from file
                self._intervalframe = self.intervalframe_from_file
            if self._intervalframe.dataframe.equals(
                ReferenceBuildingIntervalFrame.default_dataframe
            ):
                # file does not exist
                self._intervalframe = self.source_file_intervalframe
                self._intervalframe.save()  # save to disk

        return self._intervalframe

    @property
    def intervalframe(self):
        """
        Returns ValidationIntervalFrame using self.full_intervalframe using the
        column representing building total usage.
        """
        return ValidationIntervalFrame(
            self.full_intervalframe.dataframe[
                [self.full_intervalframe.aggregation_column]
            ].rename(
                columns={self.full_intervalframe.aggregation_column: "kw"}
            )
        )

    @intervalframe.setter
    def intervalframe(self, intervalframe):
        """
        Assigns intervalframe to self._intervalframe. Writes to disk on save().
        """
        self._intervalframe = intervalframe

    @property
    def total_288(self):
        """
        Returns a 12 x 24 dataframe of totals (sums).
        """
        return self.intervalframe.total_frame288.dataframe

    @property
    def average_288(self):
        """
        Returns a 12 x 24 dataframe of averages.
        """
        return self.intervalframe.average_frame288.dataframe

    @property
    def peak_288(self):
        """
        Returns a 12 x 24 dataframe of peaks.
        """
        return self.intervalframe.maximum_frame288.dataframe

    @property
    def count_288(self):
        """
        Returns a 12 x 24 dataframe of counts.
        """
        return self.intervalframe.count_frame288.dataframe


class ReferenceBuildingIntervalFrame(IntervalFrameFile):
    """
    Model for handling ReferenceBuilding IntervalFrameFiles, which have
    timestamps with ambiguous year as well as multiple columns representing
    energy usage in various categories (ex. facility, lights, HVAC, etc.).
    """

    reference_model = ReferenceBuilding
    file_directory = os.path.join(MEDIA_ROOT, "reference_buildings")
    default_aggregation_column = "Electricity:Facility [kW](Hourly)"
    default_dataframe = pd.DataFrame(
        columns=[
            "Date/Time",
            "Electricity:Facility [kW](Hourly)",
            "Fans:Electricity [kW](Hourly)",
            "Cooling:Electricity [kW](Hourly)",
            "Heating:Electricity [kW](Hourly)",
            "InteriorLights:Electricity [kW](Hourly)",
            "InteriorEquipment:Electricity [kW](Hourly)",
            "Gas:Facility [kW](Hourly)",
            "Heating:Gas [kW](Hourly)",
            "InteriorEquipment:Gas [kW](Hourly)",
            "Water Heater:WaterSystems:Gas [kW](Hourly)",
        ],
        index=pd.to_datetime([]),
    )
