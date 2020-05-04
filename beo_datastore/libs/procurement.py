import pandas as pd

from beo_datastore.libs.intervalframe import (
    EnergyIntervalFrame,
    PowerIntervalFrame,
    ValidationIntervalFrame,
)


class ProcurementRateIntervalFrame(ValidationIntervalFrame):
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
        # get power_intervalframe for resampling
        if isinstance(intervalframe, PowerIntervalFrame):
            power_intervalframe = intervalframe
        elif isinstance(intervalframe, EnergyIntervalFrame):
            power_intervalframe = intervalframe.power_intervalframe
        else:
            raise TypeError(
                "intervalframe must be EnergyIntervalFrame or "
                "PowerIntervalFrame"
            )

        # resample to match period of self
        power_intervalframe = power_intervalframe.upsample_intervalframe(
            target_period=self.period, method="ffill"
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


class ProcurementCostIntervalFrame(ValidationIntervalFrame):
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
