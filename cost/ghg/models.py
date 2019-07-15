import os

from django.db import models

from beo_datastore.libs.intervalframe_file import Frame288File
from beo_datastore.libs.models import ValidationModel, Frame288FileMixin
from beo_datastore.settings import MEDIA_ROOT

from reference.reference_model.models import RateUnit


class GHGRateFrame288(Frame288File):
    """
    Model for handling GHGRate Frame288Files.
    """

    # directory for parquet file storage
    file_directory = os.path.join(MEDIA_ROOT, "ghg_rates")


class GHGRate(Frame288FileMixin, ValidationModel):
    """
    Provides lookup-values for GHG emissions calculations.
    """

    name = models.CharField(max_length=32)
    effective = models.DateField(blank=True, null=True)
    source = models.URLField(max_length=128, blank=True, null=True)
    rate_unit = models.ForeignKey(
        RateUnit, related_name="ghg_rates", on_delete=models.PROTECT
    )

    # Required by Frame288FileMixin.
    frame_file_class = GHGRateFrame288

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

    @property
    def dataframe(self):
        return self.frame288.dataframe
