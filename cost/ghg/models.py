import os

from django.db import models

from beo_datastore.libs.intervalframe import Frame288
from beo_datastore.libs.models import ValidationModel


class CleanNetShort(ValidationModel):
    """
    Provides lookup-values for Clean Net Short GHG emissions calculations
    taken from the "Clean Net Short Calculator Tool" ("Emissions Factors" tab)
    on the CPUC's website.

    Source: http://www.cpuc.ca.gov/General.aspx?id=6442451195
    """

    effective = models.DateField(blank=False, null=False)

    class Meta:
        ordering = ["id"]

    def __str__(self):
        return "Clean Net Short (effective: {})".format(self.effective)

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
        Retrieves CleanNetShortLookupTable from parquet file.
        """
        if not hasattr(self, "_lookup_table"):
            self._lookup_table = CleanNetShortLookupTable.get_frame_from_file(
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


class CleanNetShortLookupTable(Frame288):
    """
    Model for handling CleanNetShortLookupTable Frame288s.
    """

    reference_model = CleanNetShort
    file_directory = os.path.join("MEDIA_ROOT", "lookup_tables")
