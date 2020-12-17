import attr
from cached_property import cached_property
import pandas as pd

from navigader_core.der.builder import (
    DER,
    DERProduct,
    DERSimulationBuilder,
    DERStrategy,
)
from navigader_core.load.dataframe import set_dataframe_year
from navigader_core.load.intervalframe import (
    EnergyContainer,
    GasIntervalFrame,
    PowerIntervalFrame,
)
from navigader_core.load.openei import TMY3Parser


KWH_PER_THERM = 29.3
HEAT_PUMP_COEFFICIENT_OF_PERFORMANCE = 3


@attr.s(frozen=True)
class FuelSwitching(DER):
    """
    FuelSwitching configuration core logic
    """

    space_heating = attr.ib(type=bool)
    water_heating = attr.ib(type=bool)

    def convert_therms_to_kwh(self, gas_frame: GasIntervalFrame):
        """
        Converts therm usage values to equivalent heat-pump kWh values by
        multiplying by 29.3 (kWh per therm) and again by the user-provided heat-
        pump coefficient of performance (COP)
        """
        renamed = gas_frame.dataframe.rename(columns={"therms": "kwh"})
        return renamed * KWH_PER_THERM / HEAT_PUMP_COEFFICIENT_OF_PERFORMANCE

    def combine_customer_kwh_curves(self, dataframe: pd.DataFrame):
        """
        Combines the customer's 8760 kWh curves into a single curve, using the
        configuration object to decide which curves to include.

        :param dataframe: the customer's kWh hourly curves
        """
        if self.space_heating and self.water_heating:
            kwh = dataframe.space_heating + dataframe.water_heating
        elif self.space_heating:
            kwh = dataframe.space_heating
        elif self.water_heating:
            kwh = dataframe.water_heating
        else:
            raise Exception(
                "FuelSwitching configuration must have one or both of "
                "`space_heating` and `water_heating` attributes set to `True`"
            )

        kwh.name = "kw"
        return kwh.to_frame()


@attr.s(frozen=True)
class FuelSwitchingStrategy(DERStrategy):
    """
    FuelSwitching strategy core logic
    """

    tmy3_file = attr.ib(type=TMY3Parser)

    @cached_property
    def date_index(self) -> pd.DatetimeIndex:
        """
        Computes a pd.DatetimeIndex object with a single row for every day
        represented in the TMY3 file
        """
        gas_df = self.tmy3_file.gas_dataframe
        return pd.date_range(
            start=gas_df.index.min(), end=gas_df.index.max(), freq="D"
        )

    @cached_property
    def day_totals(self):
        """
        Returns a dataframe with a single row for every day represented in the
        TMY3 file, with 3 columns:
          - "total": the total gas usage on each day
          - "space_heating": the total gas used for space heating on each day
          - "water_heating": the total gas used for water heating on each day
        """
        gas_df = self.tmy3_file.gas_dataframe
        totals = [
            gas_df[gas_df.index.dayofyear == timestamp.dayofyear].sum()
            for timestamp in self.date_index
        ]

        return pd.DataFrame(totals, index=self.date_index)

    def get_day_totals(self, timestamp: pd.Timestamp):
        """
        Returns the day totals for a given day, provided with a timestamp taken
        from the date_index
        """
        return self.day_totals.loc[timestamp]

    @cached_property
    def _gas_type_percentages(self):
        """
        Returns a dataframe with a single row for every day represented in the
        TMY3 file, with columns "space_heating" and "water_heating" that have
        values describing the percentage of gas in a given day that went towards
        the column-category. The two values will almost always add to 1. The
        exception is when no gas is used during a given day for space- or water-
        heating, in which case both columns are set to 0.
        """
        percentages = []

        for timestamp in self.date_index:
            day_totals = self.get_day_totals(timestamp)
            total_gas = day_totals.space_heating + day_totals.water_heating

            if total_gas == 0:
                percent_space_heating = 0
                percent_water_heating = 0
            else:
                percent_space_heating = day_totals.space_heating / total_gas
                percent_water_heating = day_totals.water_heating / total_gas

            percentages.append((percent_space_heating, percent_water_heating))

        return pd.DataFrame(
            percentages,
            index=self.date_index,
            columns=("space_heating", "water_heating"),
        )

    def gas_type_percentages(self, intervalframe: GasIntervalFrame):
        """
        Returns the gas_type_percentages dataframe aligned to an intervalframe's
        year
        """
        year = intervalframe.year_mode
        return set_dataframe_year(self._gas_type_percentages, year)

    @cached_property
    def _normalized_tmy3(self):
        """
        Produces normalized space- and water-heating gas curves using the OpenEI
        data. The following is done for every day of the year:
            1. Sum the space- and water-heating column values
            2. Divide all hourly values by the sums
        """
        gas_df = self.tmy3_file.gas_dataframe.copy()

        def divide_if_not_zero(mask, column: str, sums):
            """
            Handles dividing the gas dataframe by a daily sum value if the value
            is non-zero, and sets the the dataframe to 0 if the sum is 0.
            """
            sum_for_column = getattr(sums, column)
            if sum_for_column == 0:
                gas_df.loc[mask, column] = 0
            else:
                gas_df.loc[mask, column] /= sum_for_column

        for timestamp in self.date_index:
            day_mask = gas_df.index.dayofyear == timestamp.dayofyear
            day_sum = self.get_day_totals(timestamp)
            divide_if_not_zero(day_mask, "total", day_sum)
            divide_if_not_zero(day_mask, "space_heating", day_sum)
            divide_if_not_zero(day_mask, "water_heating", day_sum)

        return gas_df

    def normalized_tmy3(self, intervalframe: GasIntervalFrame):
        """
        Returns the normalized_curves dataframe aligned to an intervalframe's
        year
        """
        year = intervalframe.year_mode
        return set_dataframe_year(self._normalized_tmy3, year)


@attr.s(frozen=True)
class FuelSwitchingSimulationBuilder(DERSimulationBuilder):
    """
    Generates DERProducts a.k.a. FuelSwitching Simulations.
    """

    der = attr.ib(type=FuelSwitching)
    der_strategy = attr.ib(type=FuelSwitchingStrategy)

    def run_simulation(self, intervalframe: EnergyContainer) -> DERProduct:
        """
        Runs a fuel switching simulation on the provided EnergyContainer object.
        Note that this `run_simulation` method differs from the parent class's
        function declaration in that its `intervalframe` param is an
        EnergyContainer object instead of a PowerIntervalFrame. This is because
        fuel switching deals with multiple intervalframes for a single customer:
        one for the customer's gas usage and another for their electricity
        usage. The two intervalframes are bundled together in the
        EnergyContainer

        :param intervalframe: the EnergyContainer object wrapping a customer's
            hourly electrical and daily gas usage intervals
        """
        gas_frame = intervalframe.gas
        customer_kwh_8760 = self.get_customer_kwh_by_gas_type(
            tmy3_normalized=self.der_strategy.normalized_tmy3(gas_frame),
            customer_kwh_daily=self.convert_customer_therms_to_kwh(gas_frame),
        )
        der_intervalframe = PowerIntervalFrame(
            self.der.combine_customer_kwh_curves(customer_kwh_8760)
        )
        return DERProduct(
            der=self.der,
            der_strategy=self.der_strategy,
            pre_der_intervalframe=intervalframe.kw,
            der_intervalframe=der_intervalframe,
            post_der_intervalframe=intervalframe.kw + der_intervalframe,
        )

    def convert_customer_therms_to_kwh(self, gas_frame: GasIntervalFrame):
        """
        Returns a customer's gas usage data converted to its kWh equivalents
        and broken down by gas-type. Returned dataframe should have (up to) 366
        rows and two columns.

        :param gas_frame: the customer GasIntervalFrame
        """
        percentages = self.der_strategy.gas_type_percentages(gas_frame)
        kwh_equivalents = self.der.convert_therms_to_kwh(gas_frame)
        kwh_equivalents["space_heating"] = (
            kwh_equivalents.kwh * percentages.space_heating
        )
        kwh_equivalents["water_heating"] = (
            kwh_equivalents.kwh * percentages.water_heating
        )
        return kwh_equivalents.rename(columns={"kwh": "total_kwh"})

    @staticmethod
    def get_customer_kwh_by_gas_type(
        tmy3_normalized: pd.DataFrame, customer_kwh_daily: pd.DataFrame
    ):
        """
        Returns the customer's 8760 kWh curves broken down by gas type, given
        the normalized TMY3 data and the customer's daily kWh equivalents.
        Multiply hourly normalized gas curves by daily kWh equivalents by
        appending an index to the curves equal to the day of the year, setting
        the kWh equivalents index to its day of the year, multiply using those
        indices and then drop the appended index and irrelevant columns
        """
        tmy3_daily_index = tmy3_normalized.index.dayofyear
        tmy3_n = tmy3_normalized.set_index(tmy3_daily_index, append=True)
        customer_kwh_daily.index = customer_kwh_daily.index.dayofyear
        return (
            tmy3_n.mul(customer_kwh_daily, level=1)
            .reset_index(drop=True, level=1)
            .filter(items=("space_heating", "water_heating"))
        )
