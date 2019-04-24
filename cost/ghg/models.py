import os

from django.db import models

from beo_datastore.libs.intervalframe import Frame288File
from beo_datastore.libs.models import ValidationModel

from reference.reference_model.models import RateUnit


class GHGRate(ValidationModel):
    """
    Provides lookup-values for GHG emissions calculations.
    """

    name = models.CharField(max_length=32)
    effective = models.DateField(blank=True, null=True)
    source = models.URLField(max_length=128, blank=True, null=True)
    rate_unit = models.ForeignKey(
        RateUnit, related_name="ghg_cost", on_delete=models.PROTECT
    )

    class Meta:
        ordering = ["id"]
        unique_together = ("name", "effective")

    def __str__(self):
        if self.effective:
            return "{} effective: {} ({})".format(
                self.name, self.effective, self.rate_unit
            )
        else:
            return "{} ({})".format(self.name, self.rate_unit)

    def save(self, *args, **kwargs):
        if hasattr(self, "_lookup_table"):
            self._lookup_table.save()
        super().save(*args, **kwargs)

    def delete(self, *args, **kwargs):
        if hasattr(self, "_lookup_table"):
            self._lookup_table.delete()
        super().delete(*args, **kwargs)

    @property
    def lookup_table(self):
        """
        Retrieves GHGRateLookupTable from parquet file.
        """
        if not hasattr(self, "_lookup_table"):
            self._lookup_table = GHGRateLookupTable.get_frame_from_file(
                reference_object=self
            )
        return self._lookup_table

    @lookup_table.setter
    def lookup_table(self, lookup_table):
        """
        Sets lookup_table property. Writes to disk on save().
        """
        self._lookup_table = lookup_table

    @property
    def lookup_table_dataframe(self):
        return self.lookup_table.dataframe


class GHGRateLookupTable(Frame288File):
    """
    Model for handling GHGRateLookupTable Frame288s.
    """

    reference_model = GHGRate
    file_directory = os.path.join("MEDIA_ROOT", "lookup_tables")
