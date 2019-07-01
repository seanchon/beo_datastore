import attr
from attr.validators import instance_of
from cached_property import cached_property
from collections import OrderedDict
from datetime import timedelta
from functools import lru_cache
from math import floor
import pandas as pd

from beo_datastore.libs.intervalframe import ValidationIntervalFrame
from beo_datastore.libs.utils import timedelta_to_hours


@attr.s(frozen=True)
class Battery(object):
    """
    Initialize a battery with a rating (kw), discharge duration (timedelta),
    efficiency (percentage), and initial charge (kwh).

    A fully discharged battery can fully charge:
        - at rating (kw) for (discharge duration / efficiency)

    A fully charged battery can fully discharge:
        - at rating (kw) for discharge duration
    """

    rating = attr.ib(validator=instance_of(int))
    discharge_duration = attr.ib(validator=instance_of(timedelta))
    efficiency = attr.ib(validator=instance_of(float))

    @efficiency.validator
    def validate_efficiency(self, attribute, value):
        """
        Validate efficiency is between 0 and 1.
        """
        if not (0 <= value <= 1):
            raise ValueError("Efficiency must be between 0 and 1.")

    @cached_property
    def discharge_duration_hours(self):
        """
        Discharge duration converted to hours.
        """
        return timedelta_to_hours(self.discharge_duration)

    @property
    def capacity(self):
        """
        Maximum capacity a battery has available for discharge.
        """
        return self.rating * self.discharge_duration_hours

    def validate_power(self, power):
        """
        Ensures that power level is neither less than zero or greater than the
        battery's rating.
        """
        if not (-self.rating <= power <= self.rating):
            raise ValueError(
                "Power must be between {} and {} kw.".format(
                    -self.rating, self.rating
                )
            )

    def validate_charge(self, charge):
        """
        Ensures that battery charge state is neither less than zero or greater
        that its max capacity.
        """
        if not (0 <= charge <= self.capacity):
            raise ValueError(
                "Charge must be between 0 and {}.".format(self.capacity)
            )

    def get_target_power(self, duration, current_charge, target_charge):
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

        self.validate_power(target_power)
        return target_power

    def get_next_charge(self, power, duration, current_charge):
        """
        Get next charge level (kwh) based on specified power, specified
        duration, and beginning charge level (kwh).

        :param power: input power (kw)
        :param duration: timedelta
        :param current_charge: current charge level (kwh)
        :return: charge (kwh)
        """
        self.validate_power(power)
        if power >= 0:  # charge
            next_charge = current_charge + (
                power * timedelta_to_hours(duration) * self.efficiency
            )
        else:  # discharge
            next_charge = current_charge + (
                power * timedelta_to_hours(duration)
            )

        self.validate_charge(next_charge)
        return next_charge


class BatteryIntervalFrame(ValidationIntervalFrame):
    """
    Base class for generating and storing battery operation intervals.
    """

    default_dataframe = pd.DataFrame(
        columns=["kw", "charge", "capacity"], index=pd.to_datetime([])
    )
    default_aggregation_column = "kw"

    @property
    def current_charge(self):
        """
        Current charge on battery.
        """
        if self.dataframe.empty:
            return 0.0
        else:
            return self.dataframe.iloc[-1].charge

    @property
    def current_state_of_charge(self):
        """
        Current state of charge on battery.
        """
        if self.dataframe.empty:
            return 0.0
        else:
            return (
                self.dataframe.iloc[-1].charge
                / self.dataframe.iloc[-1].capacity
            )

    @property
    def energy_loss(self):
        """
        Energy lost to charge/discharge cycles in kWh.
        """
        self.reset_cached_properties()
        return (
            self.battery_intervalframe.total_frame288.dataframe.sum().sum()
            - self.battery.charge
        )


class BatterySimulation(object):
    """
    Base class for running a battery simulation.
    """

    def __init__(
        self,
        battery,
        load_intervalframe,
        battery_intervalframe=None,
        *args,
        **kwargs
    ):
        """
        :param battery: Battery
        :param load_intervalframe: ValidationIntervalFrame
        :param battery_intervalframe: BatteryIntervalFrame
        """
        self.cached_operations = []
        self.cached_operation_timestamps = []
        self.battery = battery
        self.load_intervalframe = load_intervalframe
        if battery_intervalframe is None:
            battery_intervalframe = BatteryIntervalFrame(
                BatteryIntervalFrame.default_dataframe
            )
        self.battery_intervalframe = battery_intervalframe

    @property
    def pre_intervalframe(self):
        """
        Return a ValidationIntervalFrame representing load before battery.
        """
        return self.load_intervalframe

    @property
    def post_intervalframe(self):
        """
        Return a ValidationIntervalFrame representing load after battery.
        """
        return self.load_intervalframe + self.battery_intervalframe

    @property
    def combined_dataframe(self):
        """
        Return origin load plus battery operations in a single dataframe.
        """
        df = self.battery_intervalframe.dataframe
        df["state of charge"] = df["charge"] / df["capacity"]
        return pd.merge(
            df,
            self.load_intervalframe.dataframe,
            how="inner",
            left_index=True,
            right_index=True,
        ).rename(columns={"kw_x": "kw", "kw_y": "load kw"})

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
    def current_charge(self):
        """
        Return the most-recent charge value from self.battery_intervalframe
        and self.cached_operations.
        """
        if self.cached_operations:
            return self.cached_operations[-1]["charge"]
        else:
            return self.battery_intervalframe.current_charge

    @property
    def next_interval_timestamp(self):
        """
        Return the timestamp after the latest timestamp in
        self.battery_intervalframe.dataframe or self.operation_timestamps.
        """
        if self.cached_operation_timestamps:
            return self.cached_operation_timestamps[-1] + self.load_period
        elif not self.battery_intervalframe.dataframe.empty:
            return (
                self.battery_intervalframe.dataframe.index[-1]
                + self.load_period
            )
        else:
            return self.load_start_timestamp

    @staticmethod
    @lru_cache(maxsize=None)
    def _generate_battery_operations(
        power, duration, load_period, battery, charge
    ):
        """
        Generate battery operations based off of input power and duration,
        a load's period, battery, and battery's beginning charge level.

        Calculations are cached for speed-up purposes.

        :param power: input power (kw)
        :param duration: timedelta
        :param load_period: timedelta
        :param battery: Battery
        :param charge: beginning charge level (kwh)
        """
        if power >= 0:  # charge
            charge_limit = battery.capacity
        else:  # discharge
            charge_limit = 0

        operations = []
        remaining_time = duration
        current_charge = charge
        while remaining_time > timedelta():
            power_limit = battery.get_target_power(
                duration=load_period,
                current_charge=current_charge,
                target_charge=charge_limit,
            )

            if power >= 0:  # charge
                operational_power = min(power, power_limit)
            else:  # discharge
                operational_power = max(power, power_limit)

            current_charge = battery.get_next_charge(
                power=operational_power,
                duration=load_period,
                current_charge=current_charge,
            )

            operations.append(
                OrderedDict(
                    [
                        ("kw", operational_power),
                        ("charge", current_charge),
                        ("capacity", battery.capacity),
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
        self.battery_intervalframe.dataframe = pd.concat(
            [
                self.battery_intervalframe.dataframe,
                pd.DataFrame(
                    self.cached_operations, self.cached_operation_timestamps
                ),
            ]
        )
        self.cached_operations = []
        self.cached_operation_timestamps = []

    def operate_battery(self):
        """
        Logic for operating batteries.
        """
        raise NotImplementedError()

    def compare_peak_loads(self):
        """
        Return dataframe consisting of peak loads by month before and after
        application of battery charge and discharge schedules.

        :return: pandas DataFrame
        """
        before = pd.DataFrame(
            self.pre_intervalframe.maximum_frame288.dataframe.max()
        )
        after = pd.DataFrame(
            self.post_intervalframe.maximum_frame288.dataframe.max()
        )

        df = pd.merge(
            before, after, how="inner", left_index=True, right_index=True
        ).rename(columns={"0_x": "before", "0_y": "after"})
        df["net"] = df["after"] - df["before"]

        return df

    def compare_month_hours(self, month, aggfunc):
        """
        Return dataframe consisting of hourly values based on aggfunc
        before and after application of battery charge and discharge schedules.

        :param month: integer
        :param aggfunc: aggregation function
        :return: pandas DataFrame
        """
        before = self.pre_intervalframe.compute_frame288(aggfunc).dataframe[
            month
        ]
        after = self.post_intervalframe.compute_frame288(aggfunc).dataframe[
            month
        ]

        before_column = "{}_x".format(month)
        after_column = "{}_y".format(month)
        df = pd.merge(
            before, after, how="inner", left_index=True, right_index=True
        ).rename(columns={before_column: "before", after_column: "after"})
        df["net"] = df["after"] - df["before"]

        return df


class ManualScheduleBatterySimulation(BatterySimulation):
    """
    Battery operations generator for manual charging and discharging.
    """

    def operate_battery(self, power, duration):
        """
        Operate battery beginning at latest interval in self.intervalframe at
        specified power for specified duration. Intervals are recorded
        according to the load period.

        Generated operations are written to BatteryIntervalFrame immediately.

        :param power: float (kw)
        :param duration: timedelta
        """
        self.cached_operation_timestamps += [
            self.next_interval_timestamp + (x * self.load_period)
            for x in range(0, int(duration / self.load_period))
        ]
        self.cached_operations += self._generate_battery_operations(
            power=power,
            duration=duration,
            load_period=self.load_period,
            battery=self.battery,
            charge=self.current_charge,
        )
        self._commit_battery_operations()


class FixedScheduleBatterySimulation(BatterySimulation):
    """
    Battery operations generator for fixed charging and discharging based on
    288 schedules (set operations per hours of the day each month).

    - self.charge_schedule dictates kw values such that anytime a meter
    is reporting a lower power level, the battery attempts to charge.
    - self.discharge_schedule dictates kw values such that anytime a meter
    is reporting a higher power level, the battery attempts to discharge.
    """

    def __init__(
        self,
        battery,
        load_intervalframe,
        charge_schedule,
        discharge_schedule,
        battery_intervalframe=None,
        *args,
        **kwargs
    ):
        """
        Tracks battery operations and writes a single update using
        self._commit_battery_operations() for speed.

        :param battery: Battery
        :param load_intervalframe: ValidationIntervalFrame
        :param charge_schedule: ValidationFrame288
        :param discharge_schedule: ValidationFrame288
        :param battery_intervalframe: BatteryIntervalFrame
        """
        self.charge_schedule = charge_schedule
        self.discharge_schedule = discharge_schedule

        if battery_intervalframe is None:
            battery_intervalframe = BatteryIntervalFrame(
                BatteryIntervalFrame.default_dataframe
            )
        self.battery_intervalframe = battery_intervalframe

        super().__init__(
            battery, load_intervalframe, battery_intervalframe, *args, **kwargs
        )

    def operate_battery(self, power, duration):
        """
        Operate battery beginning at latest interval in self.intervalframe at
        specified power for specified duration. Intervals are recorded
        according to the load period.

        Generated operations cached and not written until
        self._commit_battery_operations is called for speed purposes.

        :param power: float (kw)
        :param duration: timedelta
        """
        self.cached_operation_timestamps += [
            self.next_interval_timestamp + (x * self.load_period)
            for x in range(0, int(duration / self.load_period))
        ]
        self.cached_operations += self._generate_battery_operations(
            power=power,
            duration=duration,
            load_period=self.load_period,
            battery=self.battery,
            charge=self.current_charge,
        )

    def generate_full_sequence(self):
        """
        Generate full charge/discharge sequence.

            - When power is below self.charge_schedule, battery will
            attempt to charge.
            - When power is above self.discharge_schedule, battery will
            attempt to discharge.
        """
        for index, row in self.load_intervalframe.filter_by_datetime(
            start=self.next_interval_timestamp
        ).dataframe.iterrows():
            if index - self.next_interval_timestamp > timedelta():
                # fill gaps with no operations
                time_gap = index - self.next_interval_timestamp
                self.operate_battery(0, time_gap)

            charge_threshold = self.charge_schedule.dataframe[index.month][
                index.hour
            ]
            discharge_threshold = self.discharge_schedule.dataframe[
                index.month
            ][index.hour]
            if row.kw < charge_threshold:  # charge battery
                try:
                    power_level = floor(charge_threshold - row.kw)
                except OverflowError:
                    power_level = charge_threshold - row.kw
                self.operate_battery(power_level, self.load_period)
            elif row.kw > discharge_threshold:  # discharge battery
                try:
                    power_level = floor(discharge_threshold - row.kw)
                except OverflowError:
                    power_level = discharge_threshold - row.kw
                self.operate_battery(power_level, self.load_period)
            else:  # no operation
                self.operate_battery(0, self.load_period)

        self._commit_battery_operations()
