from localflavor.us.models import USStateField
from localflavor.us.us_states import STATE_CHOICES
import os
import us

from django.db import models
from django.utils.functional import cached_property

from beo_datastore.libs.intervalframe import IntervalFrame
from beo_datastore.libs.models import ValidationModel
from beo_datastore.settings import MEDIA_ROOT

from reference.reference_unit.models import DataUnit


class ServiceDrop(ValidationModel):
    sa_id = models.IntegerField(db_index=True)
    rate_plan = models.CharField(max_length=16, db_index=True)
    state = USStateField(choices=STATE_CHOICES)

    def __str__(self):
        return str(self.sa_id)

    @property
    def timezone(self):
        return us.states.lookup(self.state).capital_tz


class MeterQuerySet(models.QuerySet):
    def delete(self, *args, **kwargs):
        """
        Overloads delete() method so that intervalframes are deleted from disk
        along with Meter instances.
        """
        # TODO: Create a quicker cleanup method.
        for obj in self:
            obj.intervalframe.delete()
        super(MeterQuerySet, self).delete(*args, **kwargs)


class Meter(ValidationModel):
    export = models.BooleanField(default=False)
    data_unit = models.ForeignKey(
        DataUnit, related_name="meters", on_delete=models.PROTECT
    )
    service_drop = models.ForeignKey(
        ServiceDrop, related_name="meters", on_delete=models.CASCADE
    )

    # custom QuerySet manager for intervalframe file-handling
    objects = MeterQuerySet.as_manager()

    def __str__(self):
        return "{} (export: {})".format(self.service_drop, self.export)

    def save(self, *args, **kwargs):
        if self.intervalframe:
            self.intervalframe.save()
        super().save(*args, **kwargs)

    def delete(self, *args, **kwargs):
        if self.intervalframe:
            self.intervalframe.delete()
        super().delete(*args, **kwargs)

    @cached_property
    def parquet_file_intervalframe(self):
        """
        Creates IntervalFrame from local parquet copy.
        """
        return MeterIntervalFrame.get_parquet_intervalframe(ref_object=self)

    @property
    def intervalframe(self):
        """
        Retrieves IntervalFrame from parquet file.
        """
        if not getattr(self, "_intervalframe", None):
            self._intervalframe = self.parquet_file_intervalframe
        return self._intervalframe

    @intervalframe.setter
    def intervalframe(self, intervalframe):
        """
        Sets intervalframe property. Writes to disk on save().
        """
        self._intervalframe = intervalframe

    def get_288_average(self):
        return self.intervalframe.get_288_matrix("value", "average")

    def get_288_maximum(self):
        return self.intervalframe.get_288_matrix("value", "maximum")

    def get_288_count(self):
        return self.intervalframe.get_288_matrix("value", "count")


class MeterIntervalFrame(IntervalFrame):
    file_directory = os.path.join(MEDIA_ROOT, "meters")
    file_prefix = "meter_"
