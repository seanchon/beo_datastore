import os

from django.db import models, transaction
from django.utils.functional import cached_property

from beo_datastore.libs.controller import AggregateGHGCalculation
from beo_datastore.libs.intervalframe_file import Frame288File
from beo_datastore.libs.models import ValidationModel, Frame288FileMixin
from beo_datastore.settings import MEDIA_ROOT

from der.simulation.models import StoredBatterySimulation
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
        to=RateUnit, related_name="ghg_rates", on_delete=models.PROTECT
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

    def calculate_ghg_total(self, intervalframe):
        """
        Return total tCO2 created.

        :param intervalframe: ValidationIntervalFrame
        :return: tCO2 (float)
        """
        return (
            (self.frame288.dataframe * intervalframe.total_frame288.dataframe)
            .sum()
            .sum()
        )


class StoredGHGCalculation(ValidationModel):
    """
    Container for storing AggregateGHGCalculation.
    """

    pre_DER_total = models.FloatField()
    post_DER_total = models.FloatField()
    battery_simulation = models.ForeignKey(
        to=StoredBatterySimulation,
        related_name="stored_ghg_calculations",
        on_delete=models.CASCADE,
    )
    ghg_rate = models.ForeignKey(
        to=GHGRate,
        related_name="stored_ghg_calculations",
        on_delete=models.CASCADE,
    )

    class Meta:
        ordering = ["id"]
        unique_together = ("battery_simulation", "ghg_rate")

    @property
    def net_impact(self):
        """
        Return post-DER total minus pre-DER total.
        """
        return self.post_DER_total - self.pre_DER_total

    @cached_property
    def aggregate_ghg_calculation(self):
        """
        Return AggregateGHGCalculation equivalent of self.
        """
        return AggregateGHGCalculation(
            agg_simulation=self.battery_simulation.agg_simulation,
            ghg_frame288=self.ghg_rate.frame288,
        )

    @classmethod
    def generate(cls, battery_simulation_set, ghg_rate):
        """
        Get or create many StoredGHGCalculations at once. Pre-existing
        StoredGHGCalculations are retrieved and non-existing
        StoredGHGCalculations are created.

        :param battery_simulation_set: QuerySet or set of
            StoredBatterySimulations
        :param ghg_rate: GHGRate
        :return: StoredGHGCalculation QuerySet
        """
        with transaction.atomic():
            # get existing GHG calculations
            stored_ghg_calculations = cls.objects.filter(
                battery_simulation__in=battery_simulation_set,
                ghg_rate=ghg_rate,
            )

            # create new GHG calculations
            for battery_simulation in battery_simulation_set:
                objects = []
                if battery_simulation not in [
                    x.battery_simulation for x in stored_ghg_calculations
                ]:
                    objects.append(
                        cls(
                            pre_DER_total=ghg_rate.calculate_ghg_total(
                                battery_simulation.pre_intervalframe
                            ),
                            post_DER_total=ghg_rate.calculate_ghg_total(
                                battery_simulation.post_intervalframe
                            ),
                            battery_simulation=battery_simulation,
                            ghg_rate=ghg_rate,
                        )
                    )
                cls.objects.bulk_create(objects)

            return cls.objects.filter(
                battery_simulation__in=battery_simulation_set,
                ghg_rate=ghg_rate,
            )
