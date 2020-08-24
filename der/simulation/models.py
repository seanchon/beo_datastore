import attr
from datetime import timedelta
from jsonfield import JSONField
import os
from typing import Tuple

from django.core.validators import MinValueValidator, MaxValueValidator
from django.db import models
from django.db.models.signals import post_save
from django.dispatch import receiver
from django.utils.functional import cached_property

from beo_datastore.libs.der.schedule_utils import (
    create_diurnal_schedule,
    create_fixed_schedule,
    optimize_battery_schedule,
)
from beo_datastore.libs.der.battery import (
    Battery as pyBattery,
    BatteryIntervalFrame,
    BatterySimulationBuilder,
    BatteryStrategy as pyBatteryStrategy,
)
from beo_datastore.libs.der.evse import (
    EVSE as pyEVSE,
    EVSEIntervalFrame,
    EVSESimulationBuilder,
    EVSEStrategy as pyEVSEStrategy,
)
from beo_datastore.libs.der.solar import (
    SolarPV as pySolarPV,
    SolarPVSimulationBuilder,
    SolarPVStrategy as pySolarPVStrategy,
)
from beo_datastore.libs.intervalframe_file import (
    DataFrameFile,
    Frame288File,
    PowerIntervalFrame,
    PowerIntervalFrameFile,
)
from beo_datastore.libs.models import ValidationModel, Frame288FileMixin
from beo_datastore.libs.plot_intervalframe import plot_intervalframe
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
            return objects.first(), False
        else:
            return cls.create_from_frame288(frame288), True

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
        charge_schedule, _ = DERSchedule.get_or_create_from_frame288(
            optimize_battery_schedule(
                frame288=frame288,
                level=charge_aggresiveness,
                charge=True,
                minimize=minimize,
                threshold=charge_threshold,
            )
        )

        discharge_schedule, _ = DERSchedule.get_or_create_from_frame288(
            optimize_battery_schedule(
                frame288=frame288,
                level=discharge_aggresiveness,
                charge=False,
                minimize=minimize,
                threshold=discharge_threshold,
            )
        )

        object, _ = cls.objects.get_or_create(
            charge_schedule=charge_schedule,
            description=description,
            discharge_schedule=discharge_schedule,
            name=name,
            objective=objective,
        )

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
        Return pyBattery equivalent of self.
        """
        return pyBattery(
            rating=self.rating,
            discharge_duration=timedelta(hours=self.discharge_duration_hours),
            efficiency=self.efficiency,
        )

    @classmethod
    def create_from_battery(cls, battery):
        """
        Create BatteryConfiguration from pyBattery.

        :param battery: pyBattery
        :return: BatteryConfiguration
        """
        return cls.objects.create(
            rating=battery.rating,
            discharge_duration_hours=battery.discharge_duration_hours,
            efficiency=battery.efficiency,
        )

    @classmethod
    def get_or_create_from_object(cls, battery):
        """
        Get or create BatteryConfiguration from pyBattery.

        :param battery: pyBattery
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

    @classmethod
    def generate(
        cls,
        charge_during_day: bool,
        charge_off_nem: bool,
        description: str,
        drive_in_hour: int,
        drive_home_hour: int,
        distance: float,
        name: str,
        objective=None,
    ):
        """
        Creates an `EVSEStrategy` given a name, description, the drive times and
        distance, and optionally an objective.

        :param charge_during_day: `True` if the EV should charge after the
          drive-in time and before the drive-home time
        :param charge_off_nem: `True` if EVs should only charge off NEM exports
        :param description: strategy description
        :param drive_in_hour: hour at which drivers commute to work
        :param drive_home_hour: hour at which drivers commute home
        :param distance: the number of miles the drivers commute one-way
        :param name: name of the strategy
        :param objective: the DERStrategy objective
        """
        charge_limit = 0 if charge_off_nem else float("inf")
        no_charge = float("-inf")

        charge_schedule, _ = DERSchedule.get_or_create_from_frame288(
            create_diurnal_schedule(
                start_hour=drive_in_hour + 1,
                end_limit_hour=drive_home_hour,
                power_limit_1=charge_limit if charge_during_day else no_charge,
                power_limit_2=no_charge if charge_during_day else charge_limit,
            )
        )

        drive_schedule, _ = DERSchedule.get_or_create_from_frame288(
            create_fixed_schedule(
                [0] * drive_in_hour
                + [distance]
                + [0] * (drive_home_hour - drive_in_hour - 1)
                + [distance]
                + [0] * (23 - drive_home_hour)
            )
        )

        obj, _ = cls.objects.get_or_create(
            charge_schedule=charge_schedule,
            description=description,
            drive_schedule=drive_schedule,
            name=name,
            objective=objective,
        )

        return obj


class EVSEConfiguration(DERConfiguration):
    """
    Container for storing EVSE configurations.
    """

    ev_mpkwh = models.FloatField(
        blank=False, null=False, validators=[MinValueValidator(limit_value=0)]
    )
    ev_mpg_eq = models.FloatField(
        blank=False, null=False, validators=[MinValueValidator(limit_value=0)]
    )
    ev_capacity = models.FloatField(
        blank=False, null=False, validators=[MinValueValidator(limit_value=0)]
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
        blank=False, null=False, validators=[MinValueValidator(limit_value=0)]
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
        Return pyEVSE equivalent of self.
        """
        return pyEVSE(
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
    def get_configuration(cls, der: pyBattery) -> BatteryConfiguration:
        configuration, _ = BatteryConfiguration.objects.get_or_create(
            rating=der.rating,
            discharge_duration_hours=der.discharge_duration_hours,
            efficiency=der.efficiency,
        )
        return configuration

    @classmethod
    def get_strategy(cls, der_strategy: pyBatteryStrategy) -> BatteryStrategy:
        charge_schedule, _ = DERSchedule.get_or_create_from_frame288(
            der_strategy.charge_schedule
        )
        discharge_schedule, _ = DERSchedule.get_or_create_from_frame288(
            der_strategy.discharge_schedule
        )
        der_strategy, _ = BatteryStrategy.objects.get_or_create(
            charge_schedule=charge_schedule,
            discharge_schedule=discharge_schedule,
        )
        return der_strategy

    @classmethod
    def get_simulation_builder(
        cls, der: pyBattery, der_strategy: pyBatteryStrategy
    ) -> BatterySimulationBuilder:
        return BatterySimulationBuilder(der=der, der_strategy=der_strategy)


class EVSESimulationFrame(EVSEIntervalFrame, DataFrameFile):
    """
    Model for handling EVSESimulation EVSEIntervalFrames.
    """

    # directory for parquet file storage
    file_directory = os.path.join(MEDIA_ROOT, "der_simulations")


class EVSESimulation(DERSimulation):
    """
    Container for storing EVSE simulations.
    """

    # Required by IntervalFrameFileMixin.
    frame_file_class = EVSESimulationFrame

    der_type = "EVSE"

    class Meta(DERSimulation.Meta):
        verbose_name_plural = "EVSE simulations"

    @classmethod
    def get_configuration(cls, der: pyEVSE) -> EVSEConfiguration:
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
    def get_strategy(cls, der_strategy: pyEVSEStrategy) -> EVSEStrategy:
        charge_schedule, _ = DERSchedule.get_or_create_from_frame288(
            frame288=der_strategy.charge_schedule
        )
        drive_schedule, _ = DERSchedule.get_or_create_from_frame288(
            frame288=der_strategy.drive_schedule
        )
        der_strategy, _ = EVSEStrategy.objects.get_or_create(
            charge_schedule=charge_schedule, drive_schedule=drive_schedule
        )
        return der_strategy

    @classmethod
    def get_simulation_builder(
        cls, der: pyEVSE, der_strategy: pyEVSEStrategy
    ) -> EVSESimulationBuilder:
        return EVSESimulationBuilder(der=der, der_strategy=der_strategy)


class SolarPVConfiguration(DERConfiguration):
    """
    Container for storing SolarPV configurations.
    """

    parameters = JSONField(unique=True)
    stored_response = JSONField()

    der_type = "SolarPV"
    # remove stored_response from model __repr__
    repr_exclude_fields = ["stored_response"]

    class Meta:
        ordering = ["id"]
        verbose_name_plural = "Solar PV configurations"

    def clean(self, *args, **kwargs):
        self.parameters.pop("api_key", None)  # do not store API key
        super().clean(*args, **kwargs)

    @property
    def der(self) -> pySolarPV:
        """
        Return pySolarPV equivalent of self.
        """
        return pySolarPV(
            stored_response=self.stored_response, **self.parameters
        )

    @cached_property
    def solar_intervalframe(self) -> PowerIntervalFrame:
        """
        Return PowerIntervalFrame from the year 2000.
        """
        return self.der.get_annual_solar_intervalframe(2000)

    @property
    def intervalframe_html_plot(self):
        """
        Return Django-formatted HTML intervalframe plt.
        """
        return plot_intervalframe(
            intervalframe=self.solar_intervalframe, y_label="kw", to_html=True
        )

    @classmethod
    def get_or_create_from_object(
        cls, solar_pv: pySolarPV
    ) -> Tuple[DERConfiguration, bool]:
        """
        Get or create SolarPVConfiguration object from pySolarPV object.
        """
        parameters = solar_pv.request_params
        parameters.pop("api_key", None)  # do not store API key
        response = solar_pv.pvwatts_response

        return cls.objects.get_or_create(
            parameters=parameters, stored_response=response
        )


class SolarPVStrategy(DERStrategy):
    """
    Container for storing SolarPVStrategy objects.
    """

    parameters = JSONField(unique=True)

    der_type = "SolarPV"

    class Meta:
        ordering = ["id"]
        verbose_name_plural = "Solar PV strategies"

    @property
    def der_strategy(self) -> pySolarPVStrategy:
        """
        Return pySolarPVStrategy equivalent of self.
        """
        return pySolarPVStrategy(**self.parameters)

    @classmethod
    def get_or_create_from_object(
        cls, solar_pv_strategy: pySolarPVStrategy
    ) -> Tuple[DERStrategy, bool]:
        """
        Get or create SolarPVStrategy object from pySolarPVStrategy object.
        """
        return cls.objects.get_or_create(
            parameters=attr.asdict(solar_pv_strategy)
        )


class SolarPVSimulationFrame(PowerIntervalFrameFile):
    """
    Model for handling SolarPV PowerIntervalFrameFiles.
    """

    # directory for parquet file storage
    file_directory = os.path.join(MEDIA_ROOT, "der_simulations")


class SolarPVSimulation(DERSimulation):
    """
    Container for storing SolarPV simulations.
    """

    # Required by IntervalFrameFileMixin.
    frame_file_class = SolarPVSimulationFrame

    der_type = "SolarPV"

    class Meta(DERSimulation.Meta):
        verbose_name_plural = "Solar PV simulations"

    @property
    def system_capacity(self) -> float:
        """
        System capcity relative to reference solar intervalframe.
        """
        return self.der_configuration.der.get_system_capacity(
            self.intervalframe
        )

    @classmethod
    def get_configuration(cls, der: pySolarPV) -> SolarPVConfiguration:
        configuration, _ = SolarPVConfiguration.get_or_create_from_object(
            solar_pv=der
        )

        return configuration

    @classmethod
    def get_strategy(cls, der_strategy: pySolarPVStrategy) -> SolarPVStrategy:
        der_strategy, _ = SolarPVStrategy.get_or_create_from_object(
            solar_pv_strategy=der_strategy
        )

        return der_strategy

    @classmethod
    def get_simulation_builder(
        cls, der: pySolarPV, der_strategy: pySolarPVStrategy
    ) -> SolarPVSimulationBuilder:
        return SolarPVSimulationBuilder(der=der, der_strategy=der_strategy)
