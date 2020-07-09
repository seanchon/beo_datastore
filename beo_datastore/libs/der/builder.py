from abc import ABC, abstractmethod
import attr
from attr.validators import instance_of
from cached_property import cached_property
from datetime import datetime
from functools import reduce
from multiprocessing import Pool
import pandas as pd

from beo_datastore.libs.intervalframe import (
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


@attr.s(frozen=True)
class DERProduct(ABC):
    """
    The DERProduct (a.k.a. DER simulation) is the end result of applying a DER
    to a building's load profile.
    """

    der = attr.ib(validator=instance_of(DER))
    der_strategy = attr.ib(validator=instance_of(DERStrategy))
    pre_intervalframe = attr.ib(validator=instance_of(PowerIntervalFrame))
    der_intervalframe = attr.ib(validator=instance_of(PowerIntervalFrame))
    post_intervalframe = attr.ib(validator=instance_of(PowerIntervalFrame))

    @property
    def pre_der_intervalframe(self) -> PowerIntervalFrame:
        """
        Alias for self.pre_intervalframe.
        """
        return self.pre_intervalframe

    @property
    def post_der_intervalframe(self) -> PowerIntervalFrame:
        """
        Alias for self.post_intervalframe.
        """
        return self.post_intervalframe

    def compare_peak_loads(self) -> pd.DataFrame:
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

    def compare_month_hours(self, month: int, aggfunc) -> pd.DataFrame:
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


@attr.s(frozen=True)
class AggregateDERProduct(ABC):
    """
    The AggregateDERProduct is a container for many DERProducts. Since
    ValidationIntervalFrames can be added to one another, this container is
    meant to return various ValidationIntervalFrames in aggregate.

    :param der_products: dict containing {id: DERProduct} key, value pairs.
    """

    der_products = attr.ib(validator=instance_of(dict))

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
        return {k: v.pre_intervalframe for k, v in self.der_products.items()}

    @cached_property
    def post_der_results(self) -> dict:
        """
        Dictionary of key, value pairs where each key is an id and each value
        is a PowerIntervalFrame after introducing a DER.
        """
        return {k: v.post_intervalframe for k, v in self.der_products.items()}

    @cached_property
    def pre_intervalframe(self) -> PowerIntervalFrame:
        """
        Sum of all pre_intervalframes in self.der_products.
        """
        return reduce(
            lambda x, y: x + y,
            [x.pre_intervalframe for x in self.der_products.values()],
        )

    @property
    def pre_der_intervalframe(self) -> PowerIntervalFrame:
        """
        Alias for self.pre_intervalframe.
        """
        return self.pre_intervalframe

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
    def post_intervalframe(self) -> PowerIntervalFrame:
        """
        Sum of all post_intervalframes in self.der_products.
        """
        return reduce(
            lambda x, y: x + y,
            [x.post_intervalframe for x in self.der_products.values()],
        )

    @property
    def post_der_intervalframe(self) -> PowerIntervalFrame:
        """
        Alias for self.post_intervalframe.
        """
        return self.post_intervalframe


@attr.s(frozen=True)
class DERSimulationBuilder(ABC):
    """
    The DERSimulationBuilder interface specifies methods for creating the
    operations of a DERProduct (a.k.a. DER simulation).
    """

    der = attr.ib(validator=instance_of(DER))
    der_strategy = attr.ib(validator=instance_of(DERStrategy))

    @abstractmethod
    def operate_der(
        self, intervalframe: ValidationIntervalFrame
    ) -> DERProduct:
        """
        Simulate DER operations.
        """
        pass


@attr.s(frozen=True)
class DERSimulationDirector:
    """
    The DERSimulationDirector is responsible for executing the building steps
    in a particular sequence.
    """

    builder = attr.ib(validator=instance_of(DERSimulationBuilder))

    def operate_single_der(
        self,
        intervalframe: ValidationIntervalFrame,
        start: datetime = pd.Timestamp.min,
        end_limit: datetime = pd.Timestamp.max,
    ) -> DERProduct:
        """
        Create a single DERProduct from a single ValidationIntervalFrame.
        """
        return self.builder.operate_der(intervalframe=intervalframe)

    def operate_many_ders(
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
                    self.builder.operate_der, zip(intervalframes)
                )
        else:
            der_simulations = []
            for intervalframe in intervalframes:
                der_simulations.append(
                    self.builder.operate_der(intervalframe=intervalframe)
                )

        return AggregateDERProduct(
            der_products={
                intervalframe_id: der_simulation
                for intervalframe_id, der_simulation in zip(
                    intervalframe_ids, der_simulations
                )
            }
        )
