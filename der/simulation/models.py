from datetime import timedelta
import os

from django.core.validators import MinValueValidator, MaxValueValidator
from django.db import models, transaction

from beo_datastore.libs.battery import Battery, FixedScheduleBatterySimulation
from beo_datastore.libs.intervalframe_file import (
    BatteryIntervalFrameFile,
    Frame288File,
)
from beo_datastore.libs.models import (
    ValidationModel,
    Frame288FileMixin,
    IntervalFrameFileMixin,
)
from beo_datastore.settings import MEDIA_ROOT

from load.customer.models import Meter


class BatteryScheduleFrame288(Frame288File):
    """
    Model for handling BatterySchedule Frame288Files.
    """

    file_directory = os.path.join(MEDIA_ROOT, "der_battery_simulations")


class BatterySchedule(Frame288FileMixin, ValidationModel):
    hash = models.CharField(max_length=64, unique=True)

    # Required by Frame288FileMixin.
    frame_file_class = BatteryScheduleFrame288

    class Meta:
        ordering = ["id"]

    def save(self, *args, **kwargs):
        """
        Save ValidationFrame288 hash value.
        """
        self.hash = self.frame288.__hash__()
        super().save(*args, **kwargs)


class BatteryConfiguration(ValidationModel):
    rating = models.IntegerField(blank=False, null=False)
    discharge_duration_hours = models.IntegerField(blank=False, null=False)
    efficiency = models.FloatField(
        blank=False,
        null=False,
        validators=[
            MinValueValidator(limit_value=0),
            MaxValueValidator(limit_value=1),
        ],
    )

    class Meta:
        ordering = ["id"]
        unique_together = ("rating", "discharge_duration_hours", "efficiency")

    @property
    def battery(self):
        """
        Return Battery equivalent of self.
        """
        return Battery(
            rating=self.rating,
            discharge_duration=timedelta(hours=self.discharge_duration_hours),
            efficiency=self.efficiency,
        )


class DERBatterySimulationFrame(BatteryIntervalFrameFile):
    """
    Model for handling DERBatterySimulation BatteryIntervalFrameFiles.
    """

    # directory for parquet file storage
    file_directory = os.path.join(MEDIA_ROOT, "der_battery_simulations")


class DERBatterySimulation(IntervalFrameFileMixin, ValidationModel):
    meter = models.ForeignKey(
        to=Meter,
        on_delete=models.CASCADE,
        related_name="der_battery_simulations",
    )
    battery_configuration = models.ForeignKey(
        to=BatteryConfiguration,
        on_delete=models.CASCADE,
        related_name="der_battery_simulations",
    )
    charge_schedule = models.ForeignKey(
        to=BatterySchedule,
        on_delete=models.CASCADE,
        related_name="charge_schedule_der_battery_simulations",
    )
    discharge_schedule = models.ForeignKey(
        to=BatterySchedule,
        on_delete=models.CASCADE,
        related_name=("discharge_schedule_der_battery_simulations"),
    )

    # Required by IntervalFrameF.
    frame_file_class = DERBatterySimulationFrame

    class Meta:
        ordering = ["id"]
        unique_together = (
            "meter",
            "battery_configuration",
            "charge_schedule",
            "discharge_schedule",
        )

    @property
    def simulation(self):
        """
        Return FixedScheduleBatterySimulation equivalent of self.
        """
        return FixedScheduleBatterySimulation(
            battery=self.battery_configuration.battery,
            load_intervalframe=self.meter.intervalframe,
            charge_schedule=self.charge_schedule.frame288,
            discharge_schedule=self.discharge_schedule.frame288,
            battery_intervalframe=self.intervalframe,
        )

    @classmethod
    def create_from_meter_simulation(cls, meter, simulation):
        """
        Create new DERBatterySimulation record from a Meter and Simulation.
        Creates necessary BatteryConfiguration and charge and discharge
        BatterySchedule objects.

        :param meter: Meter
        :param simulation: FixedScheduleBatterySimulation
        """
        with transaction.atomic():
            configuration, _ = BatteryConfiguration.objects.get_or_create(
                rating=simulation.battery.rating,
                discharge_duration_hours=(
                    simulation.battery.discharge_duration_hours
                ),
                efficiency=simulation.battery.efficiency,
            )
            charge_schedule, _ = BatterySchedule.get_or_create(
                hash=simulation.charge_schedule.__hash__(),
                dataframe=simulation.charge_schedule.dataframe,
            )
            discharge_schedule, _ = BatterySchedule.get_or_create(
                hash=simulation.discharge_schedule.__hash__(),
                dataframe=simulation.discharge_schedule.dataframe,
            )
            return cls.get_or_create(
                meter=meter,
                battery_configuration=configuration,
                charge_schedule=charge_schedule,
                discharge_schedule=discharge_schedule,
                dataframe=simulation.battery_intervalframe.dataframe,
            )[0]
