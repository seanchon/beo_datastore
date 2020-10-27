import attr
from cached_property import cached_property
from collections import OrderedDict
from datetime import datetime, timedelta
from math import floor
import pandas as pd

from beo_datastore.libs.der.builder import (
    DataFrameQueue,
    DER,
    DERSimulationSequenceBuilder,
    DERStrategy,
)
from beo_datastore.libs.load.intervalframe import ValidationFrame288
from beo_datastore.libs.utils import timedelta_to_hours


@attr.s(frozen=True)
class Battery(DER):
    """
    Initialize a battery with a rating (kw), discharge duration (timedelta),
    efficiency (percentage), and initial charge (kwh).

    A fully discharged battery can fully charge:
        - at rating (kw) for (discharge duration / efficiency)

    A fully charged battery can fully discharge:
        - at rating (kw) for discharge duration
    """

    rating = attr.ib(type=float)
    discharge_duration = attr.ib(type=timedelta)
    efficiency = attr.ib(type=float)

    @rating.validator
    def _validate_rating(self, attribute, value):
        """
        Validate rating is 0 or greater.
        """
        if value < 0:
            raise ValueError("rating must be 0 or greater.")

    @discharge_duration.validator
    def _validate_discharge_duration(self, attribute, value):
        """
        Validate discharge_duration is 0 or greater.
        """
        if value < timedelta(0):
            raise ValueError(
                "discharge_duration must be timedelta(0) or greater."
            )

    @efficiency.validator
    def _validate_efficiency(self, attribute, value):
        """
        Validate efficiency is between 0 and 1.
        """
        if not (0 < value <= 1):
            raise ValueError("efficiency must be between 0 and 1.")

    def _validate_power(self, power: float) -> None:
        """
        Ensures that power level does not exceed the battery's rating.
        """
        if abs(power) > self.rating:
            raise ValueError(
                "Power must be between {} and {} kw.".format(
                    -self.rating, self.rating
                )
            )

    def _validate_charge(self, charge: float) -> None:
        """
        Ensures that battery charge state is neither less than zero or greater
        that its max capacity.
        """
        if not (0 <= charge <= self.capacity):
            raise ValueError(
                "Charge must be between 0 and {}.".format(self.capacity)
            )

    @cached_property
    def discharge_duration_hours(self) -> float:
        """
        Discharge duration converted to hours.
        """
        return timedelta_to_hours(self.discharge_duration)

    @cached_property
    def capacity(self) -> float:
        """
        Maximum capacity a battery has available for discharge.
        """
        return self.rating * self.discharge_duration_hours

    def get_target_power(
        self, duration: timedelta, current_charge: float, target_charge: float
    ) -> float:
        """
        Return the power level to get from current charge to target charge in a
        duration of time based on a battery's rating, max capacity, and
        efficiency.

        The following battery constraints apply:
            - The battery cannot charge/discharge at power beyond it rating.
            - The battery cannot charge beyond its max capacity or discharge
            below zero.
            - The battery losses due to the efficiency factor are calculated on
            the charge cycle.

        :param duration: timedelta
        :param current_charge: current charge level (kwh)
        :param target_charge: target charge level (kwh)
        :return: target power (kw)
        """
        hours = timedelta_to_hours(duration)
        if (target_charge - current_charge) >= 0:  # charge
            max_charge = min(target_charge, self.capacity)
            power = (max_charge - current_charge) / (hours * self.efficiency)
            target_power = min(power, self.rating)
        else:  # discharge
            min_charge = max(target_charge, 0)
            power = (min_charge - current_charge) / hours
            target_power = max(power, -self.rating)

        self._validate_power(target_power)
        return target_power

    def get_next_charge(
        self, power: float, duration: timedelta, current_charge: float
    ) -> float:
        """
        Get next charge level (kwh) based on specified power, specified
        duration, and beginning charge level (kwh).

        :param power: input power (kw)
        :param duration: timedelta
        :param current_charge: current charge level (kwh)
        :return: charge (kwh)
        """
        self._validate_power(power)
        if power >= 0:  # charge
            next_charge = current_charge + (
                power * timedelta_to_hours(duration) * self.efficiency
            )
        else:  # discharge
            next_charge = current_charge + (
                power * timedelta_to_hours(duration)
            )

        self._validate_charge(next_charge)
        return next_charge


@attr.s(frozen=True)
class BatteryStrategy(DERStrategy):
    """
    A combination of a charge_schedule and discharge_schedule where the
    charge_schedule specifies month-hour thresholds at which to allow charging
    and the discharge_schedule specifies month-hour thresholds at which to
    allow discharging.
    """

    charge_schedule = attr.ib(type=ValidationFrame288)
    discharge_schedule = attr.ib(type=ValidationFrame288)

    def get_charge_threshold(self, month: int, hour: int) -> float:
        """
        Return charge_threshold at given month and hour.
        """
        return self.charge_schedule.dataframe[month][hour]

    def get_discharge_threshold(self, month: int, hour: int) -> float:
        """
        Return discharge_threshold at given month and hour.
        """
        return self.discharge_schedule.dataframe[month][hour]

    def get_target_power(
        self, timestamp: datetime, meter_reading: float
    ) -> float:
        """
        Based on a given timestamp and meter_reading, return the target power
        for a battery operation based on this strategy.

        - When power is below the charge threshold, the battery will attempt to
        charge up to the charge threshold.
        - When power is above discharge threshold, the battery will attempt to
        discharge down to the discharge threshold.

        Note: Power level is rounded to nearest kw to increase hits in
        generate_battery_operations()'s lru_cache.
        """
        charge_threshold = self.get_charge_threshold(
            month=timestamp.month, hour=timestamp.hour
        )
        discharge_threshold = self.get_discharge_threshold(
            month=timestamp.month, hour=timestamp.hour
        )

        if meter_reading < charge_threshold:  # charge battery
            try:
                power_level = floor(charge_threshold - meter_reading)
            except OverflowError:
                power_level = charge_threshold - meter_reading
        elif meter_reading > discharge_threshold:  # discharge battery
            try:
                power_level = floor(discharge_threshold - meter_reading)
            except OverflowError:
                power_level = discharge_threshold - meter_reading
        else:  # no operation
            power_level = 0

        return power_level


class BatteryIntervalFrame(DataFrameQueue):
    """
    Container for generating and storing battery operation intervals.
    """

    default_dataframe = pd.DataFrame(
        columns=["kw", "charge", "capacity"], index=pd.to_datetime([])
    )
    default_aggregation_column = "kw"

    @property
    def current_charge(self) -> float:
        """
        Current charge on battery.
        """
        return self.get_latest_value(column="charge", default=0.0)

    @property
    def current_capacity(self) -> float:
        """
        Current capacity of battery.
        """
        return self.get_latest_value(column="capacity", default=float("inf"))

    @property
    def current_state_of_charge(self) -> float:
        """
        Current state of charge on battery.
        """
        return self.current_charge / self.current_capacity

    @property
    def energy_loss(self) -> float:
        """
        Energy lost to charge/discharge cycles in kWh.
        """
        self.reset_cached_properties()
        return self.total - self.dataframe.iloc[-1].charge


@attr.s(frozen=True)
class BatterySimulationBuilder(DERSimulationSequenceBuilder):
    """
    Generates DERProducts a.k.a Battery Simulations.
    """

    der = attr.ib(type=Battery)
    der_strategy = attr.ib(type=BatteryStrategy)

    def get_der_intervalframe(self) -> DataFrameQueue:
        return BatteryIntervalFrame()

    def get_noop(
        self, interval_start: datetime, der_intervalframe: BatteryIntervalFrame
    ) -> OrderedDict:
        return OrderedDict(
            {
                "start": interval_start,
                "kw": 0,
                "charge": der_intervalframe.current_charge,
                "capacity": self.der.capacity,
            }
        )

    def operate_der(
        self,
        interval_start: datetime,
        interval_load: float,
        duration: timedelta,
        der_intervalframe: BatteryIntervalFrame,
    ) -> OrderedDict:
        power_level = self.der_strategy.get_target_power(
            timestamp=interval_start, meter_reading=interval_load
        )

        if power_level >= 0:  # charge
            charge_limit = self.der.capacity
        else:  # discharge
            charge_limit = 0

        current_charge = der_intervalframe.current_charge
        power_limit = self.der.get_target_power(
            duration=duration,
            current_charge=current_charge,
            target_charge=charge_limit,
        )

        if power_level >= 0:  # charge
            operational_power = min(power_level, power_limit)
        else:  # discharge
            operational_power = max(power_level, power_limit)

        current_charge = self.der.get_next_charge(
            power=operational_power,
            duration=duration,
            current_charge=current_charge,
        )

        return OrderedDict(
            {
                "start": interval_start,
                "kw": operational_power,
                "charge": current_charge,
                "capacity": self.der.capacity,
            }
        )
