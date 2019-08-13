import os

from django.db import models

from beo_datastore.libs.intervalframe_file import IntervalFrameFile
from beo_datastore.libs.models import ValidationModel, IntervalFrameFileMixin
from beo_datastore.libs.plot_intervalframe import (
    plot_intervalframe,
    plot_frame288_monthly_comparison,
)
from beo_datastore.settings import MEDIA_ROOT

from reference.reference_model.models import LoadServingEntity


class SystemProfileIntervalFrame(IntervalFrameFile):
    """
    Model for handling SystemProfile IntervalFrameFiles.
    """

    # directory for parquet file storage
    file_directory = os.path.join(MEDIA_ROOT, "system_profiles")


class SystemProfile(IntervalFrameFileMixin, ValidationModel):
    name = models.CharField(max_length=32)
    load_serving_entity = models.ForeignKey(
        to=LoadServingEntity,
        related_name="system_profiles",
        on_delete=models.PROTECT,
    )

    # Required by IntervalFrameFileMixin.
    frame_file_class = SystemProfileIntervalFrame

    class Meta:
        ordering = ["id"]
        unique_together = ["name", "load_serving_entity"]

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
