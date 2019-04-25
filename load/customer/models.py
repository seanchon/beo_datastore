from localflavor.us.models import USStateField
from localflavor.us.us_states import STATE_CHOICES
import os
import us

from django.db import models
from django.utils.functional import cached_property

from beo_datastore.libs.intervalframe import IntervalFrameFile
from beo_datastore.libs.models import ValidationModel
from beo_datastore.settings import MEDIA_ROOT

from reference.reference_model.models import DataUnit


class Meter(ValidationModel):
    """
    A Meter is a connection point to the Utility's distribution grid
    identified by a Service Address Identifier (sa_id).
    """

    sa_id = models.IntegerField(db_index=True)
    rate_plan = models.CharField(
        max_length=16, db_index=True, blank=True, null=True
    )
    state = USStateField(choices=STATE_CHOICES)

    class Meta:
        ordering = ["id"]

    def __str__(self):
        return str(self.sa_id)

    @property
    def timezone(self):
        return us.states.lookup(self.state).capital_tz

    @property
    def import_channel(self):
        try:
            return self.channels.get(export=False)
        except Channel.DoesNotExist:
            return None

    @property
    def export_channel(self):
        try:
            return self.channels.get(export=True)
        except Channel.DoesNotExist:
            return None


class ChannelQuerySet(models.QuerySet):
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
        super(ChannelQuerySet, self).delete(*args, **kwargs)


class Channel(ValidationModel):
    """
    A Channel is a component of a Meter that tracks energy imported from
    (export=False) or energy exported to (export=True) the grid.
    """

    export = models.BooleanField(default=False)
    data_unit = models.ForeignKey(
        DataUnit, related_name="channels", on_delete=models.PROTECT
    )
    meter = models.ForeignKey(
        Meter, related_name="channels", on_delete=models.CASCADE
    )

    # custom QuerySet manager for intervalframe file-handling
    objects = ChannelQuerySet.as_manager()

    class Meta:
        ordering = ["id"]
        unique_together = ("export", "meter")

    def __str__(self):
        return "{} (export: {})".format(self.meter, self.export)

    def save(self, *args, **kwargs):
        if hasattr(self, "_intervalframe"):
            self._intervalframe.save()
        super().save(*args, **kwargs)

    def delete(self, *args, **kwargs):
        if hasattr(self, "_intervalframe"):
            self._intervalframe.delete()
        super().delete(*args, **kwargs)

    @cached_property
    def intervalframe_from_file(self):
        """
        Creates IntervalFrameFile from local parquet copy.
        """
        return ChannelIntervalFrame.get_frame_from_file(reference_object=self)

    @property
    def intervalframe(self):
        """
        Retrieves IntervalFrameFile from parquet file.
        """
        if not hasattr(self, "_intervalframe"):
            self._intervalframe = self.intervalframe_from_file
        return self._intervalframe

    @intervalframe.setter
    def intervalframe(self, intervalframe):
        """
        Sets intervalframe property. Writes to disk on save().
        """
        self._intervalframe = intervalframe

    @property
    def total_288(self):
        """
        Returns a 12 x 24 dataframe of totals (sums).
        """
        return self.intervalframe.total_288_dataframe

    @property
    def average_288(self):
        """
        Returns a 12 x 24 dataframe of averages.
        """
        return self.intervalframe.average_288_dataframe

    @property
    def peak_288(self):
        """
        Returns a 12 x 24 dataframe of peaks. Export meters return minimum
        values.
        """
        if self.export:
            return self.intervalframe.minimum_288_dataframe
        else:
            return self.intervalframe.maximum_288_dataframe

    @property
    def count_288(self):
        """
        Returns a 12 x 24 dataframe of counts.
        """
        return self.intervalframe.count_288_dataframe


class ChannelIntervalFrame(IntervalFrameFile):
    """
    Model for handling Channel IntervalFrameFiles, which have timestamps and
    values.
    """

    reference_model = Channel
    file_directory = os.path.join(MEDIA_ROOT, "meters")
