from datetime import timedelta
import os

from django.core.validators import MinValueValidator, MaxValueValidator
from django.db import models, transaction
from django.utils.functional import cached_property

from beo_datastore.libs.battery import Battery, FixedScheduleBatterySimulation
from beo_datastore.libs.controller import AggregateBatterySimulation
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

    file_directory = os.path.join(MEDIA_ROOT, "battery_simulations")


class BatterySchedule(Frame288FileMixin, ValidationModel):
    """
    Container for storing charge and discharge schedule ValidationFrame288s.
    """

    hash = models.CharField(max_length=64, unique=True)

    # Required by Frame288FileMixin.
    frame_file_class = BatteryScheduleFrame288

    class Meta:
        ordering = ["id"]

    @classmethod
    def create_from_frame288(cls, frame288):
        """
        Create BatterySchedule from ValidationFrame288.

        :param frame288: ValidationFrame288
        :return: BatterySchedule
        """
        return cls.create(hash=frame288, dataframe=frame288.dataframe)

    @classmethod
    def get_or_create_from_frame288(cls, frame288):
        """
        Get or create BatterySchedule from ValidationFrame288.

        :param frame288: ValidationFrame288
        :return: BatterySchedule
        """
        objects = cls.objects.filter(hash=frame288.__hash__())
        if objects:
            return (objects.first(), False)
        else:
            return (cls.create_from_frame288(frame288), True)

    def save(self, *args, **kwargs):
        """
        Save ValidationFrame288 hash value.
        """
        self.hash = self.frame288.__hash__()
        super().save(*args, **kwargs)


class BatteryConfiguration(ValidationModel):
    """
    Container for storing Battery configurations.
    """

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

    def __repr__(self):
        return self.detailed_name

    @property
    def detailed_name(self):
        """
        Return battery's detailed name as a string.

        Example:
        100kW @ 2 hours (90% efficiency)
        """
        return "{}kW @ {} hours ({}% efficiency)".format(
            self.rating, self.discharge_duration_hours, self.efficiency * 100
        )

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

    @classmethod
    def create_from_battery(cls, battery):
        """
        Create BatteryConfiguration from Battery.

        :param battery: Battery
        :return: BatteryConfiguration
        """
        return cls.objects.create(
            rating=battery.rating,
            discharge_duration_hours=battery.discharge_duration_hours,
            efficiency=battery.efficiency,
        )

    @classmethod
    def get_or_create_from_battery(cls, battery):
        """
        Get or create BatteryConfiguration from Battery.

        :param battery: Battery
        :return: BatteryConfiguration
        """
        objects = cls.objects.filter(
            rating=battery.rating,
            discharge_duration_hours=battery.discharge_duration_hours,
            efficiency=battery.efficiency,
        )
        if objects:
            return (objects.first(), False)
        else:
            return (cls.create_from_battery(battery), True)


class StoredBatterySimulationFrame(BatteryIntervalFrameFile):
    """
    Model for handling StoredBatterySimulation BatteryIntervalFrameFiles.
    """

    # directory for parquet file storage
    file_directory = os.path.join(MEDIA_ROOT, "battery_simulations")


class StoredBatterySimulation(IntervalFrameFileMixin, ValidationModel):
    """
    Container for storing BatterySimulations.
    """

    start = models.DateTimeField()
    end_limit = models.DateTimeField()
    meter = models.ForeignKey(
        to=Meter, on_delete=models.CASCADE, related_name="battery_simulations"
    )
    battery_configuration = models.ForeignKey(
        to=BatteryConfiguration,
        on_delete=models.CASCADE,
        related_name="battery_simulations",
    )
    charge_schedule = models.ForeignKey(
        to=BatterySchedule,
        on_delete=models.CASCADE,
        related_name="charge_schedule_battery_simulations",
    )
    discharge_schedule = models.ForeignKey(
        to=BatterySchedule,
        on_delete=models.CASCADE,
        related_name=("discharge_schedule_battery_simulations"),
    )

    # Required by IntervalFrameFileMixin.
    frame_file_class = StoredBatterySimulationFrame

    class Meta:
        ordering = ["id"]
        unique_together = (
            "meter",
            "battery_configuration",
            "charge_schedule",
            "discharge_schedule",
            "start",
            "end_limit",
        )

    @cached_property
    def pre_intervalframe(self):
        return self.simulation.pre_intervalframe

    @cached_property
    def post_intervalframe(self):
        return self.simulation.post_intervalframe

    @cached_property
    def simulation(self):
        """
        Return FixedScheduleBatterySimulation equivalent of self.
        """
        return FixedScheduleBatterySimulation(
            battery=self.battery_configuration.battery,
            load_intervalframe=self.meter.intervalframe.filter_by_datetime(
                start=self.start, end_limit=self.end_limit
            ),
            charge_schedule=self.charge_schedule.frame288,
            discharge_schedule=self.discharge_schedule.frame288,
            battery_intervalframe=self.intervalframe,
        )

    @cached_property
    def agg_simulation(self):
        """
        Return AggregateBatterySimulation equivalent of self.
        AggregateBatterySimulations with the same parameters can be added to
        one another.
        """
        return AggregateBatterySimulation(
            battery=self.battery_configuration.battery,
            start=self.start,
            end_limit=self.end_limit,
            charge_schedule=self.charge_schedule.frame288,
            discharge_schedule=self.discharge_schedule.frame288,
            results={self.meter: self.simulation},
        )

    @classmethod
    def get_or_create_from_objects(
        cls, meter, simulation, start=None, end_limit=None
    ):
        """
        Get existing or create new StoredBatterySimulation from a Meter and
        Simulation. Creates necessary BatteryConfiguration and charge and
        discharge BatterySchedule objects.

        :param meter: Meter
        :param simulation: FixedScheduleBatterySimulation
        :param start: datetime
        :param end_limit: datetime
        :return: (
            StoredBatterySimulation,
            StoredBatterySimulation created (True/False)
        )
        """
        if start is None:
            start = simulation.battery_intervalframe.start_datetime
        if end_limit is None:
            end_limit = simulation.battery_intervalframe.end_limit_datetime

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
                start=start,
                end_limit=end_limit,
                dataframe=simulation.battery_intervalframe.dataframe,
            )

    @classmethod
    def generate(
        cls,
        battery,
        start,
        end_limit,
        meter_set,
        charge_schedule,
        discharge_schedule,
        multiprocess=False,
    ):
        """
        Get or create many StoredBatterySimulations at once. Pre-existing
        StoredBatterySimulations are retrieved and non-existing
        StoredBatterySimulations are created.

        :param battery: Battery
        :param start: datetime
        :param end_limit: datetime
        :param meter_set: QuerySet or set of Meters
        :param charge_schedule: ValidationFrame288
        :param discharge_schedule: ValidationFrame288
        :param multiprocess: True or False
        :return: StoredBatterySimulation QuerySet
        """
        with transaction.atomic():
            configuration, _ = BatteryConfiguration.objects.get_or_create(
                rating=battery.rating,
                discharge_duration_hours=(battery.discharge_duration_hours),
                efficiency=battery.efficiency,
            )
            charge_schedule, _ = BatterySchedule.get_or_create(
                hash=charge_schedule.__hash__(),
                dataframe=charge_schedule.dataframe,
            )
            discharge_schedule, _ = BatterySchedule.get_or_create(
                hash=discharge_schedule.__hash__(),
                dataframe=discharge_schedule.dataframe,
            )

            # get existing aggregate simulation
            stored_simulations = cls.objects.filter(
                meter__id__in=[x.id for x in meter_set],
                battery_configuration=configuration,
                charge_schedule=charge_schedule,
                discharge_schedule=discharge_schedule,
                start=start,
                end_limit=end_limit,
            )

            # generate new aggregate simulation for remaining meters
            new_meters = set(meter_set) - {x.meter for x in stored_simulations}
            new_simulation = AggregateBatterySimulation.create(
                battery=battery,
                start=start,
                end_limit=end_limit,
                meter_set=new_meters,
                charge_schedule=charge_schedule.frame288,
                discharge_schedule=discharge_schedule.frame288,
                multiprocess=multiprocess,
            )

            # store new simulations
            for meter, battery_simulation in new_simulation.results.items():
                cls.get_or_create_from_objects(
                    meter=meter,
                    simulation=battery_simulation,
                    start=start,
                    end_limit=end_limit,
                )

            return cls.objects.filter(
                meter__in=meter_set,
                battery_configuration=configuration,
                charge_schedule=charge_schedule,
                discharge_schedule=discharge_schedule,
                start=start,
                end_limit=end_limit,
            )
