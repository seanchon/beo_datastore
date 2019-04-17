from localflavor.us.models import USStateField
from localflavor.us.us_states import STATE_CHOICES
import os
import us

from django.db import models
from django.utils.functional import cached_property

from beo_datastore.libs.intervalframe import IntervalFrameFile
from beo_datastore.libs.models import ValidationModel
from beo_datastore.settings import MEDIA_ROOT

from reference.reference_unit.models import DataUnit


class ServiceDrop(ValidationModel):
    """
    A ServiceDrop is a connection point to the Utility's distribution grid
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


class MeterQuerySet(models.QuerySet):
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
        super(MeterQuerySet, self).delete(*args, **kwargs)


class Meter(ValidationModel):
    """
    A Meter is a device that tracks energy consumption (export=False) or energy
    generation (export=True).
    """

    export = models.BooleanField(default=False)
    data_unit = models.ForeignKey(
        DataUnit, related_name="meters", on_delete=models.PROTECT
    )
    service_drop = models.ForeignKey(
        ServiceDrop, related_name="meters", on_delete=models.CASCADE
    )

    # custom QuerySet manager for intervalframe file-handling
    objects = MeterQuerySet.as_manager()

    class Meta:
        ordering = ["id"]

    def __str__(self):
        return "{} (export: {})".format(self.service_drop, self.export)

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
        return MeterIntervalFrame.get_frame_from_file(reference_object=self)

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
    def average_288_dataframe(self):
        return self.intervalframe.average_288_dataframe

    @property
    def maximum_288_dataframe(self):
        return self.intervalframe.maximum_288_dataframe

    @property
    def count_288_dataframe(self):
        return self.intervalframe.count_288_dataframe


class MeterIntervalFrame(IntervalFrameFile):
    """
    Model for handling Meter IntervalFrameFiles, which have timestamps and
    values.
    """

    reference_model = Meter
    file_directory = os.path.join(MEDIA_ROOT, "meters")
