import attr
from attr.validators import instance_of
from cached_property import cached_property
import copy
from datetime import datetime
from functools import reduce
from itertools import repeat
from multiprocessing import Pool
import pandas as pd

from beo_datastore.libs.battery import (
    Battery,
    FixedScheduleBatteryIntervalFrame,
)
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
    battery = attr.ib(validator=instance_of(Battery))
    start = attr.ib(validator=instance_of(datetime))
    end_limit = attr.ib(validator=instance_of(datetime))
    charge_schedule = attr.ib(validator=instance_of(ValidationFrame288))
    discharge_schedule = attr.ib(validator=instance_of(ValidationFrame288))
    multiprocess = attr.ib(validator=instance_of(bool))

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

    @staticmethod
    def _generate_battery_sequence(
        battery, load_intervalframe, charge_schedule, discharge_schedule
    ):
        """
        Instantiate a FixedScheduleBatteryIntervalFrame and generate full
        sequence of battery operations.
        """
        battery_intervalframe = FixedScheduleBatteryIntervalFrame(
            battery=battery,
            load_intervalframe=load_intervalframe,
            charge_schedule=charge_schedule,
            discharge_schedule=discharge_schedule,
        )
        battery_intervalframe.generate_full_sequence()

        return battery_intervalframe

    def generate_battery_sequences(self):
        """
        Run battery simulation against all simulation objects. Copy battery
        instance for each simulation to begin simulation from the same initial
        battery charge each time.
        """
        if self.multiprocess:
            with Pool() as pool:
                battery_copies = [
                    copy.copy(self.battery) for _ in self.load_intervalframes
                ]
                battery_intervalframes = pool.starmap(
                    self._generate_battery_sequence,
                    zip(
                        battery_copies,
                        self.load_intervalframes,
                        repeat(self.charge_schedule),
                        repeat(self.discharge_schedule),
                    ),
                )
        else:
            battery_intervalframes = []
            for intervalframe in self.load_intervalframes:
                battery_intervalframes.append(
                    self._generate_battery_sequence(
                        copy.copy(self.battery),
                        intervalframe,
                        self.charge_schedule,
                        self.discharge_schedule,
                    )
                )

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

    # TODO: Break RatePlan dependency. A lib should not import from a Django
    # model and each simulation may be under a different RatePlan.

    simulation = attr.ib(validator=instance_of(DERSimulator))
    date_ranges = attr.ib()
    rate_plan = attr.ib()
    multiprocess = attr.ib(validator=instance_of(bool))

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
    def pre_DER_bill_totals(self):
        """
        Return Pandas DataFrame containing bill totals for pre-DER scenario.
        """
        df = pd.DataFrame(
            [[x.total for x in y.values()] for y in self.before_bills]
        ).T
        df.columns = self.simulation.simulation_objects
        df.index = [x[0] for x in self.date_ranges]

        return df

    @cached_property
    def pre_DER_bill_grand_total(self):
        """
        Return sum of all bills for pre-DER scenario.
        """
        return self.pre_DER_bill_totals.sum().sum()

    @cached_property
    def post_DER_bill_totals(self):
        """
        Return Pandas DataFrame containing bill totals for post-DER scenario.
        """
        df = pd.DataFrame(
            [[x.total for x in y.values()] for y in self.after_bills]
        ).T
        df.columns = self.simulation.simulation_objects
        df.index = [x[0] for x in self.date_ranges]

        return df

    @cached_property
    def post_DER_bill_grand_total(self):
        """
        Return sum of all bills for post-DER scenario.
        """
        return self.post_DER_bill_totals.sum().sum()

    @cached_property
    def net_DER_bill_totals(self):
        """
        Return Pandas DataFrame containing net difference of pre-DER bills
        minus post-DER bills.
        """
        return self.pre_DER_bill_totals - self.post_DER_bill_totals

    def generate_bills(self, intervalframes):
        simulation_bills = []
        for intervalframe in intervalframes:
            results = self.rate_plan.generate_many_bills(
                intervalframe=intervalframe,
                date_ranges=self.date_ranges,
                multiprocess=self.multiprocess,
            )
            simulation_bills.append(results)

        return simulation_bills


@attr.s(frozen=True)
class AggregateGHGCalculator(object):
    """
    Run GHG calculations across a simulation's many before and after load
    profiles.
    """

    simulation = attr.ib(validator=instance_of(DERSimulator))
    ghg_frame288 = attr.ib(validator=instance_of(ValidationFrame288))

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
