from cached_property import cached_property
from collections import OrderedDict
from datetime import timedelta
from functools import lru_cache as cache
from math import floor
import pandas as pd

from beo_datastore.libs.intervalframe import ValidationIntervalFrame
from beo_datastore.libs.utils import timedelta_to_hours


class Battery(object):
    def __init__(
        self, rating, discharge_duration, efficiency, charge=0, *args, **kwargs
    ):
        """
        Initialize a battery with a rating (kw), discharge duration
        (timedelta), efficiency (percentage), and initial charge.

        A fully discharged battery can fully charge:
            - at rating (kw) for (discharge duration / efficiency)

        A fully charged battery can fully discharge:
            - at rating (kw) for discharge duration

        :param rating: float (kw)
        :param discharge_duration: timedelta
        :param charge: float (kwh)
        :param efficiency: float (0 to 1)
        """
        self.rating = rating
        self.discharge_duration = discharge_duration
        self.efficiency = efficiency
        self.charge = charge

    @property
    def charge(self):
        return self._charge

    @charge.setter
    def charge(self, charge):
        self.validate_charge(charge, self.max_capacity)
        self._charge = charge

    @property
    def max_capacity(self):
        """
        Maximum capacity a battery has available for discharge.
        """
        return self.rating * self.discharge_duration_hours

    @property
    def state_of_charge(self):
        """
        Charge available divided by the maximum capacity.
        """
        return self.charge / self.max_capacity

    @state_of_charge.setter
    def state_of_charge(self, state_of_charge):
        self.charge = state_of_charge * self.max_capacity

    @cached_property
    def discharge_duration_hours(self):
        """
        Discharge duration converted to hours.
        """
        return timedelta_to_hours(self.discharge_duration)

    @staticmethod
    def validate_charge(charge, max_capacity):
        """
        Ensures that battery charge state is neither less than zero or greater
        that its max capacity.
        """
        if charge < 0:
            raise AttributeError("Charge cannot drop below 0.")
        elif charge > max_capacity:
            raise AttributeError(
                "Charge cannot exceed max capacity - {} kwh.".format(
                    max_capacity
                )
            )

    @staticmethod
    def validate_power(power, rating):
        """
        Ensures that power level is neither less than zero or greater than the
        battery's rating.
        """
        if not (-rating <= power <= rating):
            raise AttributeError(
                "Power must be between {} and {} kw".format(-rating, rating)
            )

    @staticmethod
    def get_target_power(
        duration,
        current_charge,
        target_charge,
        rating,
        max_capacity,
        efficiency,
    ):
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
        :param rating: battery rating (kw)
        :param max_capacity: battery max capacity (kwh)
        :param efficiency: battery charge-cycle efficiency coefficient
        :return: target power (kw)
        """
        hours = timedelta_to_hours(duration)
        if (target_charge - current_charge) >= 0:  # charge
            max_charge = min(target_charge, max_capacity)
            power = (max_charge - current_charge) / (hours * efficiency)
            return min(power, rating)
        else:  # discharge
            min_charge = max(target_charge, 0)
            power = (min_charge - current_charge) / hours
            return max(power, -rating)

    @staticmethod
    def get_next_charge(
        power, duration, current_charge, rating, max_capacity, efficiency
    ):
        """
        Get next charge level (kwh) based on specified power, specified
        duration, and beginning charge level (kwh).

        :param power: input power (kw)
        :param duration: timedelta
        :param current_charge: current charge level (kwh)
        :param rating: battery rating (kw)
        :param max_capacity: battery max capacity (kwh)
        :param efficiency: battery charge-cycle efficiency coefficient
        :return: charge (kwh)
        """
        if power >= 0:  # charge
            return current_charge + (
                power * timedelta_to_hours(duration) * efficiency
            )
        else:  # discharge
            return current_charge + (power * timedelta_to_hours(duration))

    def operate_battery(self, power, duration):
        """
        Charge or discharge a battery at power level (kw) for duration.
        Positive values of power will charge the battery and negative values
        will discharge the battery.

        :param power: kw
        :param duration: timedelta
        :return: operational power (kwh)
        """
        if power >= 0:  # charge
            charge_limit = self.max_capacity
        else:  # discharge
            charge_limit = 0

        power_limit = self.get_target_power(
            duration,
            self.charge,
            charge_limit,
            self.rating,
            self.max_capacity,
            self.efficiency,
        )

        if power >= 0:  # charge
            operational_power = min(power, power_limit)
        else:  # discharge
            operational_power = max(power, power_limit)

        self.validate_power(operational_power, self.rating)

        self.charge = self.get_next_charge(
            operational_power,
            duration,
            self.charge,
            self.rating,
            self.max_capacity,
            self.efficiency,
        )

        return operational_power


class BatteryIntervalFrame(ValidationIntervalFrame):
    """
    Base class for generating and storing battery operation intervals.
    """

    default_dataframe = pd.DataFrame(
        columns=["kw", "charge", "state of charge"], index=pd.to_datetime([])
    )
    default_aggregation_column = "kw"

    def __init__(self, battery, load_intervalframe, *args, **kwargs):
        """
        :param battery: Battery
        :param load_intervalframe: ValidationIntervalFrame
        """
        self.battery = battery
        self.load_intervalframe = load_intervalframe
        super().__init__(self.default_dataframe, *args, **kwargs)

    @property
    def combined_intervalframe(self):
        """
        Return load plus battery operations in a ValidationIntervalFrame.
        """
        return self.load_intervalframe + self

    @property
    def load_start_timestamp(self):
        return self.load_intervalframe.start_timestamp

    @property
    def load_end_timestamp(self):
        return self.load_intervalframe.end_timestamp

    @property
    def load_period(self):
        return self.load_intervalframe.period

    @property
    def next_interval_timestamp(self):
        """
        Return the timestamp after the latest timestamp in self.dataframe.
        """
        if self.dataframe.empty:
            return self.load_start_timestamp
        else:
            return self.dataframe.iloc[-1].name + self.load_period

    def operate_battery(self):
        """
        Logic for operating batteries.
        """
        raise NotImplementedError()


class ManualScheduleBatteryIntervalFrame(BatteryIntervalFrame):
    """
    Battery schedule generator for manual charging and discharging.
    """

    def operate_battery(self, power, duration):
        """
        Operate battery beginning at latest interval in self.intervalframe at
        specified power for specified duration. Intervals are recorded
        according to the load period.

        :param power: float (kw)
        :param duration: timedelta
        """
        operation_timestamps = [
            self.next_interval_timestamp + (x * self.load_period)
            for x in range(0, int(duration / self.load_period))
        ]

        operations = []
        remaining_time = duration
        while remaining_time > timedelta():
            operational_power = self.battery.operate_battery(
                power, self.load_period
            )

            operations.append(
                OrderedDict(
                    [
                        ("kw", operational_power),
                        ("charge", self.battery.charge),
                        ("state of charge", self.battery.state_of_charge),
                    ]
                )
            )
            remaining_time -= self.load_period

        self.dataframe = pd.concat(
            [self.dataframe, pd.DataFrame(operations, operation_timestamps)]
        )


class FixedScheduleBatteryIntervalFrame(BatteryIntervalFrame):
    """
    Battery schedule generator for fixed charging and discharging based on 288
    schedules (set operations per hours of the day each month).

    - self.min_threshold_frame288 dictates kw values such that anytime a meter
    is reporting a lower power level, the battery attempts to charge.
    - self.max_threshold_frame288 dictates kw values such that anytime a meter
    is reporting a higher power level, the battery attempts to discharge.
    """

    def __init__(
        self,
        battery,
        load_intervalframe,
        min_threshold_frame288,
        max_threshold_frame288,
        *args,
        **kwargs
    ):
        """
        Tracks battery operations and writes a single update using
        self._commit_battery_operations() for speed.

        :param battery: Battery
        :param load_intervalframe: ValidationIntervalFrame
        :param min_threshold_frame288: ValidationFrame288
        :param max_threshold_frame288: ValidationFrame288
        """
        self.cached_operations = []
        self.cached_operation_timestamps = []
        self.cached_battery_charge = battery.charge
        self.min_threshold_frame288 = min_threshold_frame288
        self.max_threshold_frame288 = max_threshold_frame288

        super().__init__(
            battery,
            load_intervalframe,
            self.default_dataframe,
            *args,
            **kwargs
        )

    @property
    def next_interval_timestamp(self):
        """
        Return the timestamp after the latest timestamp in
        self.dataframe or self.operation_timestamps.
        """
        if self.cached_operation_timestamps:
            return self.cached_operation_timestamps[-1] + self.load_period
        elif not self.dataframe.empty:
            return self.dataframe.iloc[-1].name + self.load_period
        else:
            return self.load_start_timestamp

    @staticmethod
    @cache(maxsize=None)
    def _generate_battery_operations(
        power,
        duration,
        load_period,
        initial_charge,
        rating,
        max_capacity,
        efficiency,
    ):
        """
        Generate battery operations based off of input power and duration,
        a load's period, and battery's inital charge level, and battery's
        specs.

        Calculations are cached for speed-up purposes, so battery state must be
        passed as function parameters.

        :param power: input power (kw)
        :param duration: timedelta
        :param load_period: timedelta
        :param initial_charge: intial charge level (kwh)
        :param rating: battery rating (kw)
        :param max_capacity: battery max capacity (kwh)
        :param efficiency: battery charge-cycle efficiency coefficient
        """
        if power >= 0:  # charge
            charge_limit = max_capacity
        else:  # discharge
            charge_limit = 0

        operations = []
        remaining_time = duration
        current_charge = initial_charge
        while remaining_time > timedelta():
            power_limit = Battery.get_target_power(
                load_period,
                current_charge,
                charge_limit,
                rating,
                max_capacity,
                efficiency,
            )

            if power >= 0:  # charge
                operational_power = min(power, power_limit)
            else:  # discharge
                operational_power = max(power, power_limit)

            Battery.validate_power(operational_power, rating)

            current_charge = Battery.get_next_charge(
                operational_power,
                load_period,
                current_charge,
                rating,
                max_capacity,
                efficiency,
            )

            operations.append(
                OrderedDict(
                    [
                        ("kw", operational_power),
                        ("charge", current_charge),
                        ("state of charge", current_charge / max_capacity),
                    ]
                )
            )
            remaining_time -= load_period

        return operations

    def _commit_battery_operations(self):
        """
        After performing self.operate_battery() many times, this method can be
        run to perform a single state update, which saves time.
        """
        self.dataframe = pd.concat(
            [
                self.dataframe,
                pd.DataFrame(
                    self.cached_operations, self.cached_operation_timestamps
                ),
            ]
        )
        self.battery.charge = self.cached_battery_charge
        self.cached_operations = []
        self.cached_operation_timestamps = []

    def operate_battery(self, power, duration):
        """
        Operate battery beginning at latest interval in self.intervalframe at
        specified power for specified duration. Intervals are recorded
        according to the load period.

        :param power: float (kw)
        :param duration: timedelta
        """
        self.cached_operation_timestamps += [
            self.next_interval_timestamp + (x * self.load_period)
            for x in range(0, int(duration / self.load_period))
        ]
        self.cached_operations += self._generate_battery_operations(
            power,
            duration,
            self.load_period,
            self.cached_battery_charge,
            self.battery.rating,
            self.battery.max_capacity,
            self.battery.efficiency,
        )
        self.cached_battery_charge = self.cached_operations[-1]["charge"]

    def generate_full_sequence(self):
        """
        Generate full charge/discharge sequence.

            - When power is below self.min_threshold_frame288, battery will
            attempt to charge.
            - When power is above self.max_threshold_frame288, battery will
            attempt to discharge.
        """
        for index, row in self.load_intervalframe.filter_by_datetime(
            start=self.next_interval_timestamp
        ).dataframe.iterrows():
            if index - self.next_interval_timestamp > timedelta():
                # fill gaps with no operations
                time_gap = index - self.next_interval_timestamp
                self.operate_battery(0, time_gap)

            min_threshold = self.min_threshold_frame288.dataframe[index.month][
                index.hour
            ]
            max_threshold = self.max_threshold_frame288.dataframe[index.month][
                index.hour
            ]
            if row.kw < min_threshold:  # charge battery
                self.operate_battery(
                    floor(min_threshold - row.kw), self.load_period
                )
            elif row.kw > max_threshold:  # discharge battery
                self.operate_battery(
                    floor(max_threshold - row.kw), self.load_period
                )
            else:  # no operation
                self.operate_battery(0, self.load_period)

        self._commit_battery_operations()
