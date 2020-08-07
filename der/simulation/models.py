from datetime import timedelta
import os

from django.core.validators import MinValueValidator, MaxValueValidator
from django.db import models, transaction
from django.db.models.signals import post_save
from django.dispatch import receiver
from django.utils.functional import cached_property

from beo_datastore.libs.battery_schedule import optimize_battery_schedule
from beo_datastore.libs.der.battery import (
    Battery,
    BatteryIntervalFrame,
    BatterySimulationBuilder,
    BatteryStrategy as pyBatteryStrategy,
)
from beo_datastore.libs.der.evse import (
    EVSE,
    EVSEIntervalFrame,
    EVSESimulationBuilder,
    EVSEStrategy as pyEVSEStrategy,
)
from beo_datastore.libs.intervalframe import ValidationFrame288
from beo_datastore.libs.intervalframe_file import DataFrameFile, Frame288File
from beo_datastore.libs.models import (
    ValidationModel,
    Frame288FileMixin,
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


class DERScheduleFrame288(Frame288File):
    """
    Model for handling DERSchedule Frame288Files.
    """

    file_directory = os.path.join(MEDIA_ROOT, "der_schedules")


class DERSchedule(Frame288FileMixin, ValidationModel):
    """
    Container for storing schedules based upon 288 models.
    """

    hash = models.BigIntegerField(unique=True)

    # Required by Frame288FileMixin.
    frame_file_class = DERScheduleFrame288

    class Meta:
        ordering = ["id"]

    @classmethod
    def create_from_frame288(cls, frame288):
        """
        Create DERSchedule from ValidationFrame288.

        :param frame288: ValidationFrame288
        :return: DERSchedule
        """
        return cls.create(
            hash=frame288.__hash__(), dataframe=frame288.dataframe
        )

    @classmethod
    def get_or_create_from_frame288(cls, frame288):
        """
        Get or create DERSchedule from ValidationFrame288.

        :param frame288: ValidationFrame288
        :return: DERSchedule
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
        to=DERSchedule,
        related_name="charge_schedule_battery_strategies",
        on_delete=models.PROTECT,
    )
    discharge_schedule = models.ForeignKey(
        to=DERSchedule,
        related_name="discharge_schedule_battery_strategies",
        on_delete=models.PROTECT,
    )

    der_type = "Battery"

    class Meta:
        ordering = ["id"]
        unique_together = ("charge_schedule", "discharge_schedule")
        verbose_name_plural = "battery strategies"

    def __str__(self):
        return self.name

    @property
    def der_strategy(self):
        return pyBatteryStrategy(
            charge_schedule=self.charge_schedule.frame288,
            discharge_schedule=self.discharge_schedule.frame288,
        )

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
        name,
        description,
        frame288,
        charge_aggresiveness,
        discharge_aggresiveness,
        objective,
        minimize=True,
        charge_threshold=None,
        discharge_threshold=None,
    ):
        """
        Based on an input ValidationFrame288 representing part of a cost
        function (i.e. GHG rates, utility rates, RA system maximums, etc.),
        this method will create a BatteryStrategy composed of a charge_schedule
        and discharge_schedule.

        :param name: name of ValidationFrame288 (ex. "E-19 Energy
            Demand Rates", "A-10 Energy Weekend Rates", etc.)
        :param frame288: ValidationFrame288
        :param charge_aggresiveness: aggresiveness of charge schedule, the
            higher the value, the more the strategy tries to charge (int)
        :param discharge_aggresiveness: aggresiveness of discharge schedule,
            the higher the value, the more the strategy tries to discharge (int)
        :param: objective: the DERStrategy objective
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
            level=charge_aggresiveness,
            charge=True,
            minimize=minimize,
            threshold=charge_threshold,
        )
        charge_schedule, _ = DERSchedule.get_or_create(
            hash=charge_schedule_frame_288.__hash__(),
            dataframe=charge_schedule_frame_288.dataframe,
        )
        discharge_schedule_frame_288 = optimize_battery_schedule(
            frame288=frame288,
            level=discharge_aggresiveness,
            charge=False,
            minimize=minimize,
            threshold=discharge_threshold,
        )
        discharge_schedule, _ = DERSchedule.get_or_create(
            hash=discharge_schedule_frame_288.__hash__(),
            dataframe=discharge_schedule_frame_288.dataframe,
        )

        object, _ = cls.objects.get_or_create(
            charge_schedule=charge_schedule,
            discharge_schedule=discharge_schedule,
            objective=objective,
        )
        object.name = name
        object.description = description
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
    def der(self):
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


class EVSEStrategy(DERStrategy):
    """
    Container for storing a combination of charge and drive schedules.
    """

    charge_schedule = models.ForeignKey(
        to=DERSchedule,
        related_name="charge_schedule_evse_strategies",
        on_delete=models.PROTECT,
    )

    drive_schedule = models.ForeignKey(
        to=DERSchedule,
        related_name="drive_schedule_evse_strategies",
        on_delete=models.PROTECT,
    )

    der_type = "EVSE"

    class Meta:
        ordering = ["id"]
        unique_together = ("charge_schedule", "drive_schedule")
        verbose_name_plural = "EVSE strategies"

    @property
    def der_strategy(self):
        return pyEVSEStrategy(
            charge_schedule=self.charge_schedule.frame288,
            drive_schedule=self.drive_schedule.frame288,
        )

    @property
    def charge_schedule_html_table(self):
        return self.charge_schedule.html_table

    @property
    def drive_schedule_html_table(self):
        return self.drive_schedule.html_table

    @property
    def charge_drive_html_plot(self):
        """
        Return Django-formatted HTML charge vs. drive 288 plt.
        """
        return plot_frame288_monthly_comparison(
            original_frame288=self.charge_schedule.frame288,
            original_line_color="green",
            modified_frame288=self.drive_schedule.frame288,
            modified_line_color="red",
            to_html=True,
        )


class EVSEConfiguration(DERConfiguration):
    """
    Container for storing EVSE configurations.
    """

    ev_mpkwh = models.FloatField(
        blank=False, null=False, validators=[MinValueValidator(limit_value=0)],
    )
    ev_mpg_eq = models.FloatField(
        blank=False, null=False, validators=[MinValueValidator(limit_value=0)],
    )
    ev_capacity = models.FloatField(
        blank=False, null=False, validators=[MinValueValidator(limit_value=0)],
    )
    ev_efficiency = models.FloatField(
        blank=False,
        null=False,
        validators=[
            MinValueValidator(limit_value=0),
            MaxValueValidator(limit_value=1),
        ],
    )
    evse_rating = models.FloatField(
        blank=False, null=False, validators=[MinValueValidator(limit_value=0)],
    )
    ev_count = models.IntegerField(
        blank=False, null=False, validators=[MinValueValidator(limit_value=0)]
    )
    evse_count = models.IntegerField(
        blank=False, null=False, validators=[MinValueValidator(limit_value=0)]
    )

    der_type = "EVSE"

    class Meta:
        ordering = ["id"]
        unique_together = (
            "ev_mpkwh",
            "ev_mpg_eq",
            "ev_capacity",
            "ev_efficiency",
            "evse_rating",
            "ev_count",
            "evse_count",
        )
        verbose_name_plural = "EVSE configurations"

    @property
    def der(self):
        """
        Return EVSE equivalent of self.
        """
        return EVSE(
            ev_mpkwh=self.ev_mpkwh,
            ev_mpg_eq=self.ev_mpg_eq,
            ev_capacity=self.ev_capacity,
            ev_efficiency=self.ev_efficiency,
            evse_rating=self.evse_rating,
            ev_count=self.ev_count,
            evse_count=self.evse_count,
        )


@receiver(post_save, sender=BatteryConfiguration)
def assign_battery_configuration_name(sender, instance, **kwargs):
    """
    If BatteryConfiguration has no name, automatically assign one.

    # TODO: Remove if DERConfiguration.name is set to null=False.
    """
    if not instance.name:
        instance.name = instance.detailed_name
        instance.save()


class StoredBatterySimulationFrame(BatteryIntervalFrame, DataFrameFile):
    """
    Model for handling StoredBatterySimulation BatteryIntervalFrameFiles.
    """

    # directory for parquet file storage
    file_directory = os.path.join(MEDIA_ROOT, "battery_simulations")


class StoredBatterySimulation(DERSimulation):
    """
    Container for storing BatterySimulations.
    """

    # Required by IntervalFrameFileMixin.
    frame_file_class = StoredBatterySimulationFrame

    der_type = "Battery"

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

    @classmethod
    def get_or_create_from_objects(
        cls, meter, simulation, start=None, end_limit=None
    ):
        """
        Get existing or create new StoredBatterySimulation from a Meter and
        Simulation. Creates necessary BatteryConfiguration and charge and
        discharge DERSchedule objects.

        :param meter: Meter
        :param simulation: DERProduct
        :param start: datetime
        :param end_limit: datetime
        :return: (
            StoredBatterySimulation,
            StoredBatterySimulation created (True/False)
        )
        """
        if start is None:
            start = simulation.der_intervalframe.start_datetime
        if end_limit is None:
            end_limit = simulation.der_intervalframe.end_limit_datetime

        with transaction.atomic():
            configuration, _ = BatteryConfiguration.objects.get_or_create(
                rating=simulation.der.rating,
                discharge_duration_hours=(
                    simulation.der.discharge_duration_hours
                ),
                efficiency=simulation.der.efficiency,
            )
            charge_schedule, _ = DERSchedule.get_or_create(
                hash=simulation.der_strategy.charge_schedule.__hash__(),
                dataframe=simulation.der_strategy.charge_schedule.dataframe,
            )
            discharge_schedule, _ = DERSchedule.get_or_create(
                hash=simulation.der_strategy.discharge_schedule.__hash__(),
                dataframe=simulation.der_strategy.discharge_schedule.dataframe,
            )
            der_strategy, _ = BatteryStrategy.objects.get_or_create(
                charge_schedule=charge_schedule,
                discharge_schedule=discharge_schedule,
            )
            pre_total_frame288 = (
                simulation.pre_der_intervalframe.total_frame288
            )
            pre_DER_total = pre_total_frame288.dataframe.sum().sum()
            post_total_frame288 = (
                simulation.post_der_intervalframe.total_frame288
            )
            post_DER_total = post_total_frame288.dataframe.sum().sum()
            return cls.get_or_create(
                start=start,
                end_limit=end_limit,
                meter=meter,
                der_configuration=configuration,
                der_strategy=der_strategy,
                pre_DER_total=pre_DER_total,
                post_DER_total=post_DER_total,
                dataframe=simulation.der_intervalframe.dataframe,
            )

    @classmethod
    def get_configuration(cls, der: Battery) -> BatteryConfiguration:
        configuration, _ = BatteryConfiguration.objects.get_or_create(
            rating=der.rating,
            discharge_duration_hours=der.discharge_duration_hours,
            efficiency=der.efficiency,
        )
        return configuration

    @classmethod
    def get_strategy(
        cls,
        charge_schedule: ValidationFrame288,
        discharge_schedule: ValidationFrame288,
    ) -> BatteryStrategy:
        charge_schedule, _ = DERSchedule.get_or_create(
            hash=charge_schedule.__hash__(),
            dataframe=charge_schedule.dataframe,
        )
        discharge_schedule, _ = DERSchedule.get_or_create(
            hash=discharge_schedule.__hash__(),
            dataframe=discharge_schedule.dataframe,
        )
        der_strategy, _ = BatteryStrategy.objects.get_or_create(
            charge_schedule=charge_schedule,
            discharge_schedule=discharge_schedule,
        )
        return der_strategy

    @classmethod
    def get_simulation_builder(
        cls, der: Battery, der_strategy: BatteryStrategy
    ) -> BatterySimulationBuilder:
        return BatterySimulationBuilder(
            der=der, der_strategy=der_strategy.der_strategy
        )


class EVSESimulationFrame(EVSEIntervalFrame, DataFrameFile):
    """
    Model for handling EVSESimulation EVSEIntervalFrame.
    """

    # directory for parquet file storage
    file_directory = os.path.join(MEDIA_ROOT, "battery_simulations")


class EVSESimulation(DERSimulation):
    """
    Container for storing EVSE simulations
    """

    # Required by IntervalFrameFileMixin.
    frame_file_class = EVSESimulationFrame

    der_type = "EVSE"

    @classmethod
    def get_configuration(cls, der: EVSE) -> EVSEConfiguration:
        configuration, _ = EVSEConfiguration.objects.get_or_create(
            ev_mpkwh=der.ev_mpkwh,
            ev_mpg_eq=der.ev_mpg_eq,
            ev_capacity=der.ev_capacity,
            ev_efficiency=der.ev_efficiency,
            evse_rating=der.evse_rating,
            ev_count=der.ev_count,
            evse_count=der.evse_count,
        )
        return configuration

    @classmethod
    def get_strategy(
        cls,
        charge_schedule: ValidationFrame288,
        drive_schedule: ValidationFrame288,
    ) -> EVSEStrategy:
        charge_schedule, _ = DERSchedule.get_or_create(
            hash=charge_schedule.__hash__(),
            dataframe=charge_schedule.dataframe,
        )
        drive_schedule, _ = DERSchedule.get_or_create(
            hash=drive_schedule.__hash__(), dataframe=drive_schedule.dataframe,
        )
        der_strategy, _ = EVSEStrategy.objects.get_or_create(
            charge_schedule=charge_schedule, discharge_schedule=drive_schedule,
        )
        return der_strategy

    @classmethod
    def get_simulation_builder(
        cls, der: EVSE, der_strategy: EVSEStrategy
    ) -> EVSESimulationBuilder:
        return EVSESimulationBuilder(
            der=der, der_strategy=der_strategy.der_strategy
        )
