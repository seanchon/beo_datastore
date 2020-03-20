from datetime import timedelta
import os

from django.core.validators import MinValueValidator, MaxValueValidator
from django.db import models, transaction
from django.db.models.signals import post_save
from django.dispatch import receiver
from django.utils.functional import cached_property

from beo_datastore.libs.battery import Battery, FixedScheduleBatterySimulation
from beo_datastore.libs.battery_schedule import optimize_battery_schedule
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
from beo_datastore.libs.plot_intervalframe import (
    plot_frame288,
    plot_frame288_monthly_comparison,
)
from reference.reference_model.models import (
    DERConfiguration,
    DERSimulation,
    DERStrategy,
)


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
        return cls.create(
            hash=frame288.__hash__(), dataframe=frame288.dataframe
        )

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

    def clean(self, *args, **kwargs):
        """
        Save ValidationFrame288 hash value.
        """
        self.hash = self.frame288.__hash__()
        super().clean(*args, **kwargs)


class BatteryStrategy(DERStrategy):
    """
    Container for storing a combination of charge and discharge schedules.
    """

    charge_schedule = models.ForeignKey(
        to=BatterySchedule,
        related_name="charge_schedule_battery_strategies",
        on_delete=models.PROTECT,
    )
    discharge_schedule = models.ForeignKey(
        to=BatterySchedule,
        related_name="discharge_schedule_battery_strategies",
        on_delete=models.PROTECT,
    )

    der_type = "Battery"

    class Meta:
        ordering = ["id"]
        unique_together = ("charge_schedule", "discharge_schedule")
        verbose_name_plural = "battery strategies"

    @property
    def charge_schedule_html_table(self):
        return self.charge_schedule.html_table

    @property
    def discharge_schedule_html_table(self):
        return self.discharge_schedule.html_table

    @property
    def charge_discharge_html_plot(self):
        """
        Return Django-formatted HTML charge vs. discharge 288 plt.
        """
        return plot_frame288_monthly_comparison(
            original_frame288=self.charge_schedule.frame288,
            original_line_color="green",
            modified_frame288=self.discharge_schedule.frame288,
            modified_line_color="red",
            to_html=True,
        )

    @classmethod
    def generate(
        cls,
        frame288_name,
        frame288,
        level,
        minimize=True,
        charge_threshold=None,
        discharge_threshold=None,
    ):
        """
        Based on an input ValidationFrame288 representing part of a cost
        function (i.e. GHG rates, utility rates, RA system maximums, etc.),
        this method will create a BatteryStrategy composed of a charge_schedule
        and discharge_schedule.

        :param frame288_name: name of ValidationFrame288 (ex. "E-19 Energy
            Demand Rates", "A-10 Energy Weekend Rates", etc.)
        :param frame288: ValidationFrame288
        :param level: aggresiveness of charge/discharge schedule, the higher
            the value, the more the schedule tries to charge and discharge
            (int)
        :param minimize: when True attempts to minimize the cost function, when
            False attempts to maximize the cost function
        :param charge_threshold: a threshold at which when a meter reading is
            below, a battery attepts to charge
        :param discharge_threshold: a threshold at which when a meter reading
            is above, attempts to discharge
        :return: BatteryStrategy
        """
        charge_schedule_frame_288 = optimize_battery_schedule(
            frame288=frame288,
            level=level,
            charge=True,
            minimize=minimize,
            threshold=charge_threshold,
        )
        charge_schedule, _ = BatterySchedule.get_or_create(
            hash=charge_schedule_frame_288.__hash__(),
            dataframe=charge_schedule_frame_288.dataframe,
        )
        discharge_schedule_frame_288 = optimize_battery_schedule(
            frame288=frame288,
            level=level,
            charge=False,
            minimize=minimize,
            threshold=discharge_threshold,
        )
        discharge_schedule, _ = BatterySchedule.get_or_create(
            hash=discharge_schedule_frame_288.__hash__(),
            dataframe=discharge_schedule_frame_288.dataframe,
        )

        objective = "Minimize" if minimize else "Maximize"
        name = (
            "{} {} - charge threshold: {} kw, discharge threshold: {}"
            " kw (level: {})"
        ).format(
            objective,
            frame288_name,
            charge_threshold,
            discharge_threshold,
            level,
        )
        object, _ = cls.objects.get_or_create(
            charge_schedule=charge_schedule,
            discharge_schedule=discharge_schedule,
        )
        object.name = name
        object.save()

        return object


class BatteryConfiguration(DERConfiguration):
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

    der_type = "Battery"

    class Meta:
        ordering = ["id"]
        unique_together = ("rating", "discharge_duration_hours", "efficiency")

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


@receiver(post_save, sender=BatteryConfiguration)
def assign_battery_configuration_name(sender, instance, **kwargs):
    """
    If BatteryConfiguration has no name, automatically assign one.

    # TODO: Remove if DERConfiguration.name is set to null=False.
    """
    if not instance.name:
        instance.name = instance.detailed_name
        instance.save()


class StoredBatterySimulationFrame(BatteryIntervalFrameFile):
    """
    Model for handling StoredBatterySimulation BatteryIntervalFrameFiles.
    """

    # directory for parquet file storage
    file_directory = os.path.join(MEDIA_ROOT, "battery_simulations")


class StoredBatterySimulation(IntervalFrameFileMixin, DERSimulation):
    """
    Container for storing BatterySimulations.
    """

    pre_DER_total = models.FloatField()
    post_DER_total = models.FloatField()

    # Required by IntervalFrameFileMixin.
    frame_file_class = StoredBatterySimulationFrame

    der_type = "Battery"

    @property
    def der_intervalframe(self):
        """
        ValidationIntervalFrame represention all DER operations.
        """
        return self.intervalframe

    @property
    def charge_schedule(self):
        return self.der_strategy.charge_schedule

    @property
    def discharge_schedule(self):
        return self.der_strategy.discharge_schedule

    @property
    def energy_loss(self):
        """
        Return all energy lost due to battery roundtrip efficiency.
        """
        return self.intervalframe.energy_loss

    @cached_property
    def net_impact(self):
        return self.post_DER_total - self.pre_DER_total

    @cached_property
    def average_state_of_charge_frame288(self):
        aggregation_column = self.intervalframe.aggregation_column
        self.intervalframe.aggregation_column = "capacity"
        capacity_frame288 = self.intervalframe.average_frame288
        self.intervalframe.aggregation_column = "charge"
        charge_frame288 = self.intervalframe.average_frame288
        self.intervalframe.aggregation_column = aggregation_column

        return charge_frame288 / capacity_frame288

    @property
    def pre_vs_post_average_288_html_plot(self):
        """
        Return Django-formatted HTML pre vs. post average 288 plt.
        """
        return plot_frame288_monthly_comparison(
            original_frame288=self.pre_der_intervalframe.average_frame288,
            modified_frame288=self.post_der_intervalframe.average_frame288,
            to_html=True,
        )

    @property
    def pre_vs_post_maximum_288_html_plot(self):
        """
        Return Django-formatted HTML pre vs. post maximum 288 plt.
        """
        return plot_frame288_monthly_comparison(
            original_frame288=self.pre_der_intervalframe.maximum_frame288,
            modified_frame288=self.post_der_intervalframe.maximum_frame288,
            to_html=True,
        )

    @property
    def average_battery_operations_html_plot(self):
        return plot_frame288(
            frame288=self.intervalframe.average_frame288,
            y_label="kW",
            to_html=True,
        )

    @property
    def average_state_of_charge_html_plot(self):
        return plot_frame288(
            frame288=self.average_state_of_charge_frame288, to_html=True
        )

    @cached_property
    def simulation(self):
        """
        Return FixedScheduleBatterySimulation equivalent of self.
        """
        return FixedScheduleBatterySimulation(
            battery=self.der_configuration.battery,
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
        one another and can be used for aggregate "cost calculations" found in
        beo_datastore/libs/controller.py.
        """
        return AggregateBatterySimulation(
            battery=self.der_configuration.battery,
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
            der_strategy, _ = BatteryStrategy.objects.get_or_create(
                charge_schedule=charge_schedule,
                discharge_schedule=discharge_schedule,
            )
            pre_total_frame288 = simulation.pre_intervalframe.total_frame288
            pre_DER_total = pre_total_frame288.dataframe.sum().sum()
            post_total_frame288 = simulation.post_intervalframe.total_frame288
            post_DER_total = post_total_frame288.dataframe.sum().sum()
            return cls.get_or_create(
                start=start,
                end_limit=end_limit,
                meter=meter,
                der_configuration=configuration,
                der_strategy=der_strategy,
                pre_DER_total=pre_DER_total,
                post_DER_total=post_DER_total,
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
            der_strategy, _ = BatteryStrategy.objects.get_or_create(
                charge_schedule=charge_schedule,
                discharge_schedule=discharge_schedule,
            )

            # get existing aggregate simulation
            stored_simulations = cls.objects.filter(
                meter__id__in=[x.id for x in meter_set],
                der_configuration=configuration,
                der_strategy=der_strategy,
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
            for (meter, battery_simulation) in new_simulation.results.items():
                cls.get_or_create_from_objects(
                    meter=meter,
                    simulation=battery_simulation,
                    start=start,
                    end_limit=end_limit,
                )

            return cls.objects.filter(
                meter__in=meter_set,
                der_configuration=configuration,
                der_strategy=der_strategy,
                start=start,
                end_limit=end_limit,
            )
