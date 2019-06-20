import attr
from cached_property import cached_property
import copy
from datetime import datetime
from functools import reduce
import pandas as pd

from beo_datastore.libs.battery import (
    Battery,
    FixedScheduleBatteryIntervalFrame,
)
from beo_datastore.libs.bill import OpenEIRateData, ValidationBill
from beo_datastore.libs.intervalframe import ValidationFrame288


class DERSimulator(object):
    """
    Base class for DER simulators.
    """

    pass


@attr.s(frozen=True)
class AggregateBatterySimulator(DERSimulator):
    """
    Run battery operations across many load profiles.
    """

    simulation_objects = attr.ib()
    battery = attr.ib(type=Battery)
    start = attr.ib(type=datetime)
    end_limit = attr.ib(type=datetime)
    charge_schedule = attr.ib(type=ValidationFrame288)
    discharge_schedule = attr.ib(type=ValidationFrame288)

    def __attrs_post_init__(self):
        object.__setattr__(
            self, "battery_intervalframes", self.generate_battery_sequences()
        )

    @property
    def load_intervalframes(self):
        return [
            x.intervalframe.filter_by_datetime(self.start, self.end_limit)
            for x in self.simulation_objects
        ]

    @cached_property
    def before_intervalframes(self):
        return [x.load_intervalframe for x in self.battery_intervalframes]

    @cached_property
    def aggregate_before_intervalframe(self):
        return reduce(lambda x, y: x + y, self.before_intervalframes)

    @cached_property
    def after_intervalframes(self):
        return [x.combined_intervalframe for x in self.battery_intervalframes]

    @cached_property
    def aggregate_after_intervalframe(self):
        return reduce(lambda x, y: x + y, self.after_intervalframes)

    @cached_property
    def battery_intervalframe_dict(self):
        return {
            obj: intervalframe
            for obj, intervalframe in zip(
                self.simulation_objects, self.battery_intervalframes
            )
        }

    @cached_property
    def aggregate_energy_loss(self):
        return sum([x.energy_loss for x in self.battery_intervalframes])

    def generate_battery_sequences(self):
        """
        Run battery simulation against all simulation objects. Copy battery
        instance for each simulation to begin simulation from the same initial
        battery charge each time.
        """
        battery_intervalframes = []
        for intervalframe in self.load_intervalframes:
            battery_intervalframe = FixedScheduleBatteryIntervalFrame(
                battery=copy.copy(self.battery),
                load_intervalframe=intervalframe,
                charge_schedule=self.charge_schedule,
                discharge_schedule=self.discharge_schedule,
            )
            battery_intervalframe.generate_full_sequence()
            battery_intervalframes.append(battery_intervalframe)

        return battery_intervalframes

    def get_battery_intervalframe(self, simulation_object):
        """
        Return BatteryIntervalFrame relating to simulation_object.
        """
        return self.battery_intervalframe_dict[simulation_object]


@attr.s(frozen=True)
class AggregateBillCalculator(object):
    """
    Run bill calculations across a simulation's many before and after load
    profiles.
    """

    simulation = attr.ib(type=DERSimulator)
    date_ranges = attr.ib()
    openei_rate_data = attr.ib(type=OpenEIRateData)

    def __attrs_post_init__(self):
        object.__setattr__(
            self,
            "before_bills",
            self.generate_bills(self.simulation.before_intervalframes),
        )
        object.__setattr__(
            self,
            "after_bills",
            self.generate_bills(self.simulation.after_intervalframes),
        )

    @cached_property
    def before_bill_totals(self):
        return [[x.total for x in y.values()] for y in self.before_bills]

    @cached_property
    def before_bill_grand_total(self):
        return sum([sum(x) for x in self.before_bill_totals])

    @cached_property
    def after_bill_totals(self):
        return [[x.total for x in y.values()] for y in self.after_bills]

    @cached_property
    def after_bill_grand_total(self):
        return sum([sum(x) for x in self.after_bill_totals])

    @cached_property
    def before_bills_dict(self):
        return {
            obj: intervalframe
            for obj, intervalframe in zip(
                self.simulation.simulation_objects, self.before_bills
            )
        }

    @cached_property
    def after_bills_dict(self):
        return {
            obj: intervalframe
            for obj, intervalframe in zip(
                self.simulation.simulation_objects, self.after_bills
            )
        }

    def generate_bills(self, intervalframes):
        simulation_bills = []
        for intervalframe in intervalframes:
            results = {}
            for start, end_limit in self.date_ranges:
                results[start.month] = ValidationBill(
                    intervalframe.filter_by_datetime(start, end_limit),
                    self.openei_rate_data,
                )
            simulation_bills.append(results)

        return simulation_bills


@attr.s(frozen=True)
class AggregateGHGCalculator(object):
    """
    Run GHG calculations across a simulation's many before and after load
    profiles.
    """

    simulation = attr.ib(type=DERSimulator)
    ghg_frame288 = attr.ib(type=ValidationFrame288)

    @cached_property
    def ghg_before_frame288(self):
        return (
            self.simulation.aggregate_before_intervalframe.total_frame288
            * self.ghg_frame288
        )

    @cached_property
    def ghg_after_frame288(self):
        return (
            self.simulation.aggregate_after_intervalframe.total_frame288
            * self.ghg_frame288
        )

    @cached_property
    def comparison_table(self):
        df = pd.merge(
            pd.DataFrame(self.ghg_before_frame288.dataframe.sum()),
            pd.DataFrame(self.ghg_after_frame288.dataframe.sum()),
            how="inner",
            left_index=True,
            right_index=True,
        )
        return df.append(df.sum().rename("Total")).rename(
            columns={"0_x": "before", "0_y": "after"}
        )
