from cached_property import cached_property
from collections import OrderedDict
from datetime import timedelta
from itertools import repeat
from functools import lru_cache
from math import ceil, floor
from multiprocessing import Pool
import numpy as np
import pandas as pd

from beo_datastore.libs.intervalframe import (
    ValidationFrame288,
    ValidationIntervalFrame,
)
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
    def combined_dataframe(self):
        """
        Return load plus battery operations in a dataframe.
        """
        return pd.merge(
            self.dataframe,
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
        """
        self.cached_operations = []
        self.cached_operation_timestamps = []
        self.cached_battery_charge = battery.charge
        self.charge_schedule = charge_schedule
        self.discharge_schedule = discharge_schedule

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
    @lru_cache(maxsize=None)
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
                self.operate_battery(
                    floor(charge_threshold - row.kw), self.load_period
                )
            elif row.kw > discharge_threshold:  # discharge battery
                self.operate_battery(
                    floor(discharge_threshold - row.kw), self.load_period
                )
            else:  # no operation
                self.operate_battery(0, self.load_period)

        self._commit_battery_operations()

    def compare_peak_loads(self):
        """
        Return dataframe consisting of peak loads by month before and after
        application of battery charge and discharge schedules.

        :return: pandas DataFrame
        """
        before = pd.DataFrame(
            self.load_intervalframe.maximum_frame288.dataframe.max()
        )
        after = pd.DataFrame(
            self.combined_intervalframe.maximum_frame288.dataframe.max()
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
        before = self.load_intervalframe.compute_frame288(aggfunc).dataframe[
            month
        ]
        after = self.combined_intervalframe.compute_frame288(
            aggfunc
        ).dataframe[month]

        before_column = "{}_x".format(month)
        after_column = "{}_y".format(month)
        df = pd.merge(
            before, after, how="inner", left_index=True, right_index=True
        ).rename(columns={before_column: "before", after_column: "after"})
        df["net"] = df["after"] - df["before"]

        return df


class PeakShavingSimulator(object):
    """
    Simulator to run various optimization scenarios for when to charge and
    discharge and at what power thresholds.
    """

    @classmethod
    def get_peak_power(
        cls,
        battery,
        load_intervalframe,
        month,
        charge_threshold,
        discharge_threshold,
    ):
        """
        Simulate battery operations on a single month and return resulting peak
        load at input discharge threshold. This is useful for finding the
        optimal discharge threshold resulting in the smallest resulting peak
        load.

        :param battery: Battery
        :param load_intervalframe: ValidationIntervalFrame
        :param month: integer
        :param charge_threshold: integer
        :param discharge_threshold: integer
        :return: (discharge threshold, resulting peak load)
        """
        load_intervalframe = load_intervalframe.filter_by_months({month})

        charge_schedule = ValidationFrame288.convert_matrix_to_frame288(
            [[charge_threshold] * 24] * 12
        )
        discharge_schedule = ValidationFrame288.convert_matrix_to_frame288(
            [[discharge_threshold] * 24] * 12
        )

        battery_intervalframe = FixedScheduleBatteryIntervalFrame(
            battery=battery,
            load_intervalframe=load_intervalframe,
            charge_schedule=charge_schedule,
            discharge_schedule=discharge_schedule,
        )
        battery_intervalframe.generate_full_sequence()

        combined_maximum_frame288 = (
            battery_intervalframe.combined_intervalframe.maximum_frame288
        )
        return (
            discharge_threshold,
            combined_maximum_frame288.dataframe[month].max(),  # peak load
        )

    @classmethod
    def optimize_discharge_threshold(
        cls,
        battery,
        load_intervalframe,
        month,
        charge_threshold,
        number_of_checks=None,
    ):
        """
        Based on a given month and given charge threshold, find best discharge
        threshold for peak shaving.

        :param battery: Battery
        :param load_intervalframe: ValidationIntervalFrame
        :param month: integer
        :param charge_threshold: fixed level to charge below (int)
        :param number_of_checks: number of discharge thresholds to try (int)
        :return: (discharge threshold, peak load)
        """
        intervalframe = load_intervalframe.filter_by_months({month})
        max_load = intervalframe.maximum_frame288.dataframe[month].max()

        # OPTIMIZE: Can fewer thresholds be checked?
        if number_of_checks is None:
            # tries discharge thresholds at 1kw increments
            number_of_checks = int(battery.rating)
        discharge_thresholds = set(
            [int(max_load - x) for x in range(1, number_of_checks + 1)]
        )

        # get resulting peak powers (multiprocess)
        with Pool() as pool:
            results = pool.starmap(
                cls.get_peak_power,
                zip(
                    repeat(battery),
                    repeat(intervalframe),
                    repeat(month),
                    repeat(charge_threshold),
                    discharge_thresholds,
                ),
            )

        # return highest threshold with lowest resulting peak
        ranked_results = {}
        for discharge_threshold, peak_power in results:
            if peak_power not in ranked_results.keys():
                ranked_results[peak_power] = set()
            ranked_results[peak_power].add(discharge_threshold)
        return (
            max(ranked_results[min(ranked_results.keys())]),  # threshold
            min(ranked_results.keys()),  # resulting peak power
        )

    @classmethod
    def optimize_schedules_with_exports(cls, battery, load_intervalframe):
        """
        Creates optimal monthly charge and discharge schedules to shave peak
        loads based on charging using meter energy exports only.

        :param battery: Battery
        :param load_intervalframe: ValidationIntervalFrame
        :return: (charge schedule, discharge schedule)
            (ValidationFrame288, ValidationFrame288)
        """
        # run optimization on smaller dataset for speed
        load_intervalframe = load_intervalframe.resample_intervalframe(
            "60min", np.mean
        )

        results = {"charge_threshold": {}, "discharge_threshold": {}}
        for month in set(load_intervalframe.dataframe.index.month):
            results["charge_threshold"][month] = 0
            results["discharge_threshold"][
                month
            ], _ = cls.optimize_discharge_threshold(
                battery, load_intervalframe, month, 0
            )

        return (
            ValidationFrame288.convert_matrix_to_frame288(
                [
                    [results["charge_threshold"].get(month, 0)] * 24
                    for month in range(1, 13)
                ]
            ),
            ValidationFrame288.convert_matrix_to_frame288(
                [
                    [results["discharge_threshold"].get(month, 0)] * 24
                    for month in range(1, 13)
                ]
            ),
        )

    @classmethod
    def optimize_schedules_with_grid(
        cls, battery, load_intervalframe, verbose=False
    ):
        """
        Creates optimal monthly charge and discharge schedules to shave peak
        loads based on charging using energy exports and grid energy.

        :param battery: Battery
        :param load_intervalframe: ValidationIntervalFrame
        :param verbose: if True, print optimization steps
        :return: (charge schedule, discharge schedule)
            (ValidationFrame288, ValidationFrame288)
        """
        # run optimization on smaller dataset for speed
        load_intervalframe = load_intervalframe.resample_intervalframe(
            "60min", np.mean
        )

        results = {"charge_threshold": {}, "discharge_threshold": {}}
        for month in set(load_intervalframe.dataframe.index.month):
            if verbose:
                print("Month: {}".format(month))
            month_intervalframe = load_intervalframe.filter_by_months({month})
            peak_load = np.max(month_intervalframe.dataframe).kw

            # OPTIMIZE: Is there a bettery starting list of charge thresholds?
            # create list of charge thresholds to simulate
            increment = ceil(min(peak_load, battery.rating) / 5)
            if peak_load < battery.rating:
                charge_thresholds = range(1, int(peak_load), increment)
            else:
                charge_thresholds = range(
                    max(1, int(peak_load - battery.rating)),
                    int(peak_load),
                    increment,
                )

            lowest_peak = float("inf")
            for charge_threshold in charge_thresholds:
                discharge_threshold, peak = cls.optimize_discharge_threshold(
                    battery, load_intervalframe, month, charge_threshold
                )
                if verbose:
                    print(
                        "Charge Threshold: {}, Discharge Threshold: {}, "
                        "Net Load: {}, Peak Load: {}".format(
                            charge_threshold,
                            discharge_threshold,
                            peak,
                            peak_load,
                        )
                    )

                if peak < lowest_peak:
                    results["charge_threshold"][month] = charge_threshold
                    results["discharge_threshold"][month] = discharge_threshold
                    lowest_peak = peak
                elif peak == peak_load:
                    continue
                else:
                    break

        return (
            ValidationFrame288.convert_matrix_to_frame288(
                [
                    [results["charge_threshold"].get(month, 0)] * 24
                    for month in range(1, 13)
                ]
            ),
            ValidationFrame288.convert_matrix_to_frame288(
                [
                    [results["discharge_threshold"].get(month, 0)] * 24
                    for month in range(1, 13)
                ]
            ),
        )
