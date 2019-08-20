import os

from django.db import models, transaction
from django.utils.functional import cached_property

from beo_datastore.libs.controller import AggregateResourceAdequacyCalculation
from beo_datastore.libs.intervalframe_file import IntervalFrameFile
from beo_datastore.libs.models import ValidationModel, IntervalFrameFileMixin
from beo_datastore.libs.plot_intervalframe import (
    plot_intervalframe,
    plot_frame288_monthly_comparison,
)
from beo_datastore.settings import MEDIA_ROOT
from beo_datastore.libs.views import dataframe_to_html

from der.simulation.models import StoredBatterySimulation
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

    def __str__(self):
        return self.load_serving_entity.name + ": " + self.name

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


class StoredResourceAdequacyCalculation(ValidationModel):
    """
    Container for storing AggregateResourceAdequacyCalculation.
    """

    pre_DER_total = models.FloatField()
    post_DER_total = models.FloatField()
    battery_simulation = models.ForeignKey(
        to=StoredBatterySimulation,
        related_name="stored_resource_adequacy_calculations",
        on_delete=models.CASCADE,
    )
    system_profile = models.ForeignKey(
        to=SystemProfile,
        related_name="stored_resource_adequacy_calculations",
        on_delete=models.CASCADE,
    )

    class Meta:
        ordering = ["id"]
        unique_together = ("battery_simulation", "system_profile")

    @property
    def net_impact(self):
        """
        Return post-DER total minus pre-DER total.
        """
        return self.post_DER_total - self.pre_DER_total

    @property
    def comparision_html_table(self):
        """
        Return Django-formatted HTML pre vs. post comparision table.
        """
        return dataframe_to_html(
            self.aggregate_resource_adequacy_calculation.comparison_table
        )

    @cached_property
    def aggregate_resource_adequacy_calculation(self):
        """
        Return AggregateResourceAdequacyCalculation equivalent of self.
        """
        return AggregateResourceAdequacyCalculation(
            agg_simulation=self.battery_simulation.agg_simulation,
            system_profile_intervalframe=self.system_profile.intervalframe,
        )

    @classmethod
    def generate(cls, battery_simulation_set, system_profile):
        """
        Get or create many StoredResourceAdequacyCalculations at once.
        Pre-existing StoredResourceAdequacyCalculations are retrieved and
        non-existing StoredResourceAdequacyCalculations are created.

        :param battery_simulation_set: QuerySet or set of
            StoredBatterySimulations
        :param system_profile: SystemProfile
        :return: StoredResourceAdequacyCalculation QuerySet
        """
        with transaction.atomic():
            # get existing RA calculations
            stored_ra_calculations = cls.objects.filter(
                battery_simulation__in=battery_simulation_set,
                system_profile=system_profile,
            )

            # create new RA calculations
            stored_simulations = [
                x.battery_simulation for x in stored_ra_calculations
            ]
            objects = []
            for battery_simulation in battery_simulation_set:
                if battery_simulation in stored_simulations:
                    continue
                ra_calculation = AggregateResourceAdequacyCalculation(
                    agg_simulation=battery_simulation.agg_simulation,
                    system_profile_intervalframe=system_profile.intervalframe,
                )
                objects.append(
                    cls(
                        pre_DER_total=ra_calculation.pre_DER_total,
                        post_DER_total=ra_calculation.post_DER_total,
                        battery_simulation=battery_simulation,
                        system_profile=system_profile,
                    )
                )
            cls.objects.bulk_create(objects)

            return cls.objects.filter(
                battery_simulation__in=battery_simulation_set,
                system_profile=system_profile,
            )
