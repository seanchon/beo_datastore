from datetime import timedelta
import numpy as np
import pandas as pd

from django.utils.functional import cached_property

from navigader_core.load.intervalframe import (
    EnergyIntervalFrame,
    PowerIntervalFrame,
    ValidationIntervalFrame,
)


class ProcurementFrame288Mixin(object):
    @cached_property
    def average_frame288(self):
        """
        ValidationFrame288 of hourly average values.
        """
        return self.compute_frame288(aggfunc=np.mean)

    @cached_property
    def minimum_frame288(self):
        """
        ValidationFrame288 of hourly minimum values.
        """
        return self.compute_frame288(aggfunc=np.min)

    @cached_property
    def maximum_frame288(self):
        """
        ValidationFrame288 of hourly maximum values.
        """
        return self.compute_frame288(aggfunc=np.max)

    @cached_property
    def total_frame288(self):
        """
        ValidationFrame288 of hourly totals.
        """
        return self.compute_frame288(aggfunc=sum)

    @cached_property
    def count_frame288(self):
        """
        ValidationFrame288 of counts.
        """
        return self.compute_frame288(aggfunc=len)


class ProcurementRateIntervalFrame(
    ValidationIntervalFrame, ProcurementFrame288Mixin
):
    """
    Base class for storing procurement rates (i.e. CAISO rates) on an
    interval-by-interval basis.
    """

    default_dataframe = pd.DataFrame(
        columns=["$/kwh"], index=pd.to_datetime([])
    )
    default_aggregation_column = "$/kwh"

    def get_procurement_cost_intervalframe(self, intervalframe):
        """
        Return ProcurementCostIntervalFrame based on self and
        EnergyIntervalFrame/PowerIntervalFrame.

        :param intervalframe: EnergyIntervalFrame or PowerIntervalFrame
        :return: ProcurementCostIntervalFrame
        """
        if self.period == timedelta(0) or intervalframe.period == timedelta(0):
            return ProcurementCostIntervalFrame()

        if not isinstance(
            intervalframe, EnergyIntervalFrame
        ) and not isinstance(intervalframe, PowerIntervalFrame):
            raise TypeError(
                "intervalframe must be EnergyIntervalFrame or "
                "PowerIntervalFrame"
            )

        # resample to match period of self
        power_intervalframe = intervalframe.power_intervalframe.resample_intervalframe(
            self.period
        )

        # create dataframe with "kwh" and "$" columns
        dataframe = pd.merge(
            self.dataframe,
            power_intervalframe.energy_intervalframe.dataframe,
            left_index=True,
            right_index=True,
        )
        dataframe["$"] = dataframe["$/kwh"] * dataframe["kwh"]
        dataframe = dataframe.drop(
            columns=self.default_dataframe.columns.to_list()
        )

        return ProcurementCostIntervalFrame(dataframe=dataframe)


class ProcurementCostIntervalFrame(
    ValidationIntervalFrame, ProcurementFrame288Mixin
):
    """
    Base class for storing procurement costs (i.e. CAISO costs) on an
    interval-by-interval basis.
    """

    default_dataframe = pd.DataFrame(
        columns=["kwh", "$"], index=pd.to_datetime([])
    )
    default_aggregation_column = "$"

    def get_procurement_rate_intervalframe(self):
        """
        Return ProcurementRateIntervalFrame based on values from "kwh" column
        and "$" column.

        :return: ProcurementRateIntervalFrame
        """
        dataframe = self.dataframe.copy()
        dataframe["$/kwh"] = dataframe["$"] / dataframe["kwh"]
        dataframe = dataframe.drop(
            columns=self.default_dataframe.columns.to_list()
        )

        return ProcurementRateIntervalFrame(dataframe=dataframe)
