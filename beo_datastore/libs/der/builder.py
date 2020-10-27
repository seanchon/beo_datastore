from abc import ABC, abstractmethod
import attr
from cached_property import cached_property
from collections import OrderedDict
from datetime import datetime, timedelta
from functools import reduce
from multiprocessing import Pool
import pandas as pd
from typing import Any

from beo_datastore.libs.load.intervalframe import (
    PowerIntervalFrame,
    ValidationIntervalFrame,
)


class DER(ABC):
    """
    The physical characteristics of a Distributed Energy Resource (DER). This
    class contains the attributes of a DER at creation and any associated
    methods based on its physical properties.
    """

    pass


class DERStrategy(ABC):
    """
    The behavioral characteristics of a DER. This is a property that
    describes how the DER is used, typically on an hour-by-hour basis.
    """

    pass


class DataFrameQueue(ABC, PowerIntervalFrame):
    """
    For operations that require sequential inserts into a DataFrame, it is much
    faster to write to bulk update a DataFrame versus sequential updates to
    the DataFrame.
    """

    def __init__(self, *args, **kwargs):
        self.queued_operations = []
        super().__init__(*args, **kwargs)

    @property
    def latest_interval_dict(self) -> OrderedDict:
        """
        Latest interval represented as an OrderedDict.
        """
        if self.queued_operations:
            return self.queued_operations[-1]
        elif not self.dataframe.empty:
            return OrderedDict(
                self.dataframe.reset_index()
                .rename(columns={"index": "start"})
                .iloc[-1]
            )
        else:
            return OrderedDict()

    @property
    def latest_interval_timestamp(self) -> datetime:
        """
        Current interval timestamp.
        """
        return self.latest_interval_dict.get("start", self.start_timestamp)

    def get_latest_value(self, column: str, default: Any):
        """
        Returns latest value from self.dataframe or self.queued_operations.

        :param column: name of dataframe column or queued_operations key
        :param default: default value if dataframe and queued_operations are
            both empty
        """
        return self.latest_interval_dict.get(column, default)

    def append_operation(self, operation: OrderedDict) -> None:
        """
        Appends operations to queued_operations  for performance benefits
        associated with a bulk update of a pandas DataFrame.

        This must be followed by self.commit_operations() to perform a
        bulk update to self.dataframe.

        operations are in the format:

        [
            OrderedDict([('kw', 5), ('charge', 3.125), ('capacity', 20.0)]),
            OrderedDict([('kw', 5), ('charge', 4.25), ('capacity', 20.0)])
        ]
        """
        self.queued_operations.append(operation)

    def commit_operations(self) -> None:
        """
        After performing self.append_operations() many times, this
        method can be run to perform a single state update, which saves time.
        """
        if self.queued_operations:
            self.dataframe = pd.concat(
                [
                    self.dataframe,
                    pd.DataFrame(self.queued_operations).set_index("start"),
                ]
            )
            self.queued_operations = []


@attr.s(frozen=True)
class DERProduct(ABC):
    """
    The DERProduct (a.k.a. DER simulation) is the end result of applying a DER
    to a building's load profile.
    """

    der = attr.ib(type=DER)
    der_strategy = attr.ib(type=DERStrategy)
    pre_der_intervalframe = attr.ib(type=PowerIntervalFrame)
    der_intervalframe = attr.ib(type=PowerIntervalFrame)
    post_der_intervalframe = attr.ib(type=PowerIntervalFrame)

    def compare_peak_loads(self) -> pd.DataFrame:
        """
        Return dataframe consisting of peak loads by month before and after
        application of battery charge and discharge schedules.

        :return: pandas DataFrame
        """
        before = pd.DataFrame(
            self.pre_der_intervalframe.maximum_frame288.dataframe.max()
        )
        after = pd.DataFrame(
            self.post_der_intervalframe.maximum_frame288.dataframe.max()
        )

        df = pd.merge(
            before, after, how="inner", left_index=True, right_index=True
        ).rename(columns={"0_x": "before", "0_y": "after"})
        df["net"] = df["after"] - df["before"]

        return df

    def compare_month_hours(self, month: int, aggfunc) -> pd.DataFrame:
        """
        Return dataframe consisting of hourly values based on aggfunc
        before and after application of battery charge and discharge schedules.

        :param month: integer
        :param aggfunc: aggregation function
        :return: pandas DataFrame
        """
        before = self.pre_der_intervalframe.compute_frame288(
            aggfunc
        ).dataframe[month]
        after = self.post_der_intervalframe.compute_frame288(
            aggfunc
        ).dataframe[month]

        before_column = "{}_x".format(month)
        after_column = "{}_y".format(month)
        df = pd.merge(
            before, after, how="inner", left_index=True, right_index=True
        ).rename(columns={before_column: "before", after_column: "after"})
        df["net"] = df["after"] - df["before"]

        return df


@attr.s(frozen=True)
class AggregateDERProduct(ABC):
    """
    The AggregateDERProduct is a container for many DERProducts. Since
    ValidationIntervalFrames can be added to one another, this container is
    meant to return various ValidationIntervalFrames in aggregate.

    :param der_products: dict containing {id: DERProduct} key, value pairs.
    """

    der_products = attr.ib(type=dict)

    @der_products.validator
    def validate_der_products(self, attribute, value):
        for der_product in value.values():
            if not isinstance(der_product, DERProduct):
                raise TypeError(
                    "All values of der_products must be a DERProduct."
                )

    @cached_property
    def pre_der_results(self) -> dict:
        """
        Dictionary of key, value pairs where each key is an id and each value
        is a PowerIntervalFrame before introducing a DER.
        """
        return {
            k: v.pre_der_intervalframe for k, v in self.der_products.items()
        }

    @cached_property
    def post_der_results(self) -> dict:
        """
        Dictionary of key, value pairs where each key is an id and each value
        is a PowerIntervalFrame after introducing a DER.
        """
        return {
            k: v.post_der_intervalframe for k, v in self.der_products.items()
        }

    @cached_property
    def pre_der_intervalframe(self) -> PowerIntervalFrame:
        """
        Sum of all pre_der_intervalframes in self.der_products.
        """
        return reduce(
            lambda x, y: x + y,
            [x.pre_der_intervalframe for x in self.der_products.values()],
        )

    @cached_property
    def der_intervalframe(self) -> ValidationIntervalFrame:
        """
        Sum of all der_intervalframes in self.der_products.
        """
        return reduce(
            lambda x, y: x + y,
            [x.der_intervalframe for x in self.der_products.values()],
        )

    @cached_property
    def post_der_intervalframe(self) -> PowerIntervalFrame:
        """
        Sum of all post_der_intervalframes in self.der_products.
        """
        return reduce(
            lambda x, y: x + y,
            [x.post_der_intervalframe for x in self.der_products.values()],
        )


@attr.s(frozen=True)
class DERSimulationBuilder(ABC):
    """
    The DERSimulationBuilder class specifies the base attributes and methods
    for creating a DERProduct (a.k.a. DER simulation).
    """

    der = attr.ib(type=DER)
    der_strategy = attr.ib(type=DERStrategy)

    @abstractmethod
    def run_simulation(self, intervalframe: PowerIntervalFrame) -> DERProduct:
        """
        Runs a DER simulation given a pre-DER intervalframe based on self.der
        and self.der_strategy and returns a DERProduct. For DER simulations
        that are performed on an interval-by-interval basis, see
        DERSimulationSequenceBuilder.
        """
        pass


@attr.s(frozen=True)
class DERSimulationSequenceBuilder(DERSimulationBuilder):
    """
    The DERSimulationSequenceBuilder class extends the DERSimulationBuilder
    scaffolding to generate a DERProduct from calcuations performed on an
    interval-by-interval basis.
    """

    @abstractmethod
    def get_der_intervalframe(self) -> DataFrameQueue:
        """
        Returns a DER intervalframe for use with a single simulation
        """
        pass

    def get_pre_der_intervalframe(
        self, intervalframe: PowerIntervalFrame
    ) -> PowerIntervalFrame:
        """
        Returns the DER simulation's pre-DER intervalframe.
        """
        return intervalframe

    @abstractmethod
    def get_noop(
        self, interval_start: datetime, der_intervalframe: DataFrameQueue
    ) -> OrderedDict:
        """
        Returns a no-op for the given interval
        """
        pass

    @abstractmethod
    def operate_der(
        self,
        interval_start: datetime,
        interval_load: float,
        duration: timedelta,
        der_intervalframe: DataFrameQueue,
    ) -> OrderedDict:
        """
        Generate a DER operation over a given interval based off of current DER
        state. The return value is an OrderedDict representing the operation of
        the DER during the given interval.

        :param interval_start: start of the interval
        :param interval_load: the pre-DER intervalframe's kw reading for the
          interval
        :param duration: the length of the interval
        :param der_intervalframe: DataFrameQueue holding DER state
        """
        pass

    def get_post_der_intervalframe(
        self,
        pre_der_intervalframe: PowerIntervalFrame,
        der_intervalframe: PowerIntervalFrame,
    ) -> PowerIntervalFrame:
        """
        Returns the DER simulation's post-DER intervalframe. By default the
        post-DER intervalframe is the sum of the pre-DER intervalframe and the
        DER intervalframe. Additional modifications can be made in subclasses.
        """
        return pre_der_intervalframe + der_intervalframe

    def run_simulation(self, intervalframe: PowerIntervalFrame) -> DERProduct:
        """
        Runs a DER simulation given a pre-DER intervalframe. Each row of the
        pre-DER intervalframe is iterated over and the state of the DER/load
        is modified according to DER-specific logic.

        :param intervalframe: the pre-DER intervalframe
        """
        intervalframe = intervalframe.power_intervalframe
        der_intervalframe = self.get_der_intervalframe()
        interval_duration = intervalframe.period

        for index, row in intervalframe.dataframe.iterrows():
            interval_start = index
            next_timestamp = (
                der_intervalframe.latest_interval_timestamp + interval_duration
            )

            # fill gaps with no-ops
            while interval_start - next_timestamp > timedelta():
                noop = self.get_noop(
                    interval_start=next_timestamp,
                    der_intervalframe=der_intervalframe,
                )
                der_intervalframe.append_operation(noop)
                next_timestamp += interval_duration

            if not interval_duration:
                # if the interval duration is 0, return a no-op
                operation = self.get_noop(
                    interval_start=interval_start,
                    der_intervalframe=der_intervalframe,
                )
            else:
                # otherwise get the next DER operation
                operation = self.operate_der(
                    interval_start=interval_start,
                    interval_load=row.kw,
                    duration=interval_duration,
                    der_intervalframe=der_intervalframe,
                )

            der_intervalframe.append_operation(operation)

        der_intervalframe.commit_operations()
        pre_der_intervalframe = self.get_pre_der_intervalframe(intervalframe)
        return DERProduct(
            der=self.der,
            der_strategy=self.der_strategy,
            pre_der_intervalframe=pre_der_intervalframe,
            der_intervalframe=der_intervalframe,
            post_der_intervalframe=self.get_post_der_intervalframe(
                pre_der_intervalframe, der_intervalframe
            ),
        )


@attr.s(frozen=True)
class DERSimulationDirector:
    """
    The DERSimulationDirector is responsible for executing the building steps
    in a particular sequence.
    """

    builder = attr.ib(type=DERSimulationBuilder)

    def run_single_simulation(
        self,
        intervalframe: ValidationIntervalFrame,
        start: datetime = pd.Timestamp.min,
        end_limit: datetime = pd.Timestamp.max,
    ) -> DERProduct:
        """
        Create a single DERProduct from a single ValidationIntervalFrame.
        """
        return self.builder.run_simulation(
            intervalframe=intervalframe.filter_by_datetime(start, end_limit)
        )

    def run_many_simulations(
        self,
        intervalframe_dict: dict,
        start: datetime = pd.Timestamp.min,
        end_limit: datetime = pd.Timestamp.max,
        multiprocess: bool = False,
    ) -> AggregateDERProduct:
        """
        Create a single AggregateDERProduct from many ValidationIntervalFrames.

        :param intervalframe_dict: dict with {id: ValidationIntervalFrame}
            pairs
        :param multiprocess: True to multiprocess
        """

        intervalframe_ids = list(intervalframe_dict.keys())
        intervalframes = [
            x.filter_by_datetime(start, end_limit).power_intervalframe
            for x in intervalframe_dict.values()
        ]

        if multiprocess:
            with Pool() as pool:
                der_simulations = pool.starmap(
                    self.builder.run_simulation, zip(intervalframes)
                )
        else:
            der_simulations = []
            for intervalframe in intervalframes:
                der_simulations.append(
                    self.builder.run_simulation(intervalframe=intervalframe)
                )

        return AggregateDERProduct(
            der_products={
                intervalframe_id: der_simulation
                for intervalframe_id, der_simulation in zip(
                    intervalframe_ids, der_simulations
                )
            }
        )
