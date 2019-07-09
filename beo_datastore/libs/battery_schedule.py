from datetime import timedelta
from itertools import repeat
from math import ceil
from multiprocessing import Pool
import numpy as np

from beo_datastore.libs.battery import FixedScheduleBatterySimulation
from beo_datastore.libs.intervalframe import ValidationFrame288


def create_fixed_schedule(
    start_hour, end_limit_hour, power_limit_1, power_limit_2
):
    """
    Return ValidationFrame288 where:
        - inside of start_hour and end_limit_hour (not including
        end_limit_hour) power_limit_1 is used.
        - outside of start_hour and end_hour, power_limit_2 is used.

    :param start_hour: value from 0 to 23
    :param end_hour: value from 0 to 23
    :param power_limit_1: kw (float)
    :param power_limit_2: kw (float)
    :return: ValidationFrame288
    """
    if end_limit_hour < start_hour:
        end_limit_hour, start_hour = start_hour, end_limit_hour
        power_limit_1, power_limit_2 = power_limit_2, power_limit_1

    return ValidationFrame288.convert_matrix_to_frame288(
        [
            [power_limit_2] * (start_hour - 0)
            + [power_limit_1] * (end_limit_hour - start_hour)
            + [power_limit_2] * (24 - end_limit_hour)
        ]
        * 12
    )


def create_optimized_cns_schedule(
    cns_frame288, number_of_hours, charge=True, threshold=None
):
    """
    Create a ValidationFrame288 schedule that would charge/discharge
    battery at 100% at best hours based on Clean Net Short
    ValidationFrame288.

    :param cns_frame288: ValidationFrame288
    :param number_of_hours: number of best hours to charge
    :param charge: True to create charge schedule, False to create
        discharge schedule
    :param threshold: set to charge/discharge threshold
    :return: ValidationFrame288
    """
    dataframe = cns_frame288.dataframe

    matrix = []
    for month in dataframe.columns:
        month_df = dataframe[month]
        if charge:
            if threshold is None:
                threshold = float("inf")
            best_values = sorted(month_df)[:number_of_hours]
            matrix.append(
                [
                    threshold if x in best_values else -float("inf")
                    for x in month_df
                ]
            )
        else:  # discharge
            if threshold is None:
                threshold = -float("inf")
            best_values = list(reversed(sorted(month_df)))[:number_of_hours]
            matrix.append(
                [
                    threshold if x in best_values else float("inf")
                    for x in month_df
                ]
            )

    return ValidationFrame288.convert_matrix_to_frame288(matrix)


class PeakShavingScheduleOptimizer(object):
    """
    A collection of optimization scenarios for when to charge and discharge and
    at what power thresholds in order to maximally shave peak load.
    """

    @classmethod
    def _get_peak_power(
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

        battery_simulation = FixedScheduleBatterySimulation(
            battery=battery,
            load_intervalframe=load_intervalframe,
            charge_schedule=charge_schedule,
            discharge_schedule=discharge_schedule,
        )
        battery_simulation.generate_full_sequence()

        combined_maximum_frame288 = (
            battery_simulation.post_intervalframe.maximum_frame288
        )
        return (
            discharge_threshold,
            combined_maximum_frame288.dataframe[month].max(),  # peak load
        )

    @classmethod
    def _optimize_discharge_threshold(
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
                cls._get_peak_power,
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
            timedelta(hours=1), np.mean
        )

        results = {"charge_threshold": {}, "discharge_threshold": {}}
        for month in set(load_intervalframe.dataframe.index.month):
            results["charge_threshold"][month] = 0
            results["discharge_threshold"][
                month
            ], _ = cls._optimize_discharge_threshold(
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
            timedelta(hours=1), np.mean
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
                discharge_threshold, peak = cls._optimize_discharge_threshold(
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
