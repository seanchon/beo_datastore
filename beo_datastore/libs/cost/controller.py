import attr
from cached_property import cached_property
from datetime import datetime, timedelta
import pandas as pd

from beo_datastore.libs.der.builder import AggregateDERProduct
from beo_datastore.libs.load.intervalframe import (
    PowerIntervalFrame,
    ValidationFrame288,
)
from beo_datastore.libs.procurement import (
    ProcurementCostIntervalFrame,
    ProcurementRateIntervalFrame,
)

# File constants
RA_DOLLARS_PER_KW = 6


class DERCostCalculation(object):
    """
    Base class for DERCostCalculations. A DERCostCalculation takes a
    AggregateDERProduct as an input and calculates a net impact of that DER
    plus supporting calculations.

    A DERCostCalculation serves as an input to a report, so it should provide a
    standardized set of results.
    """

    @property
    def pre_DER_total(self):
        """
        The cost calculation of a pre-DER scenario.
        """
        raise NotImplementedError(
            "pre_DER_total must be set in {}".format(self.__class__)
        )

    @property
    def post_DER_total(self):
        """
        The cost calculation of a post-DER scenario.
        """
        raise NotImplementedError(
            "post_DER_total must be set in {}".format(self.__class__)
        )

    @property
    def net_impact(self):
        """
        The cost calculation of a post-DER scenario minus the cost calculation
        of a pre-DER scenario.
        """
        return self.post_DER_total - self.pre_DER_total


@attr.s(frozen=True)
class AggregateBillCalculation(DERCostCalculation):
    """
    Run bill calculations across a AggregateDERProduct's many before and
    after load profiles.
    """

    # TODO: Break RatePlan dependency. A lib should not import from a Django
    # model and each simulation may be under a different RatePlan.

    agg_simulation = attr.ib(type=AggregateDERProduct)
    rate_data = attr.ib()

    @property
    def rate_plan(self):
        return self.rate_data

    def __add__(self, other):
        """
        Allow AggregateBillCalculation objects to be combined.
        """
        for name in ["rate_plan", "date_ranges"]:
            if getattr(self, name) != getattr(other, name):
                raise ValueError(
                    "{} should equal {}.".format(
                        getattr(self, name), getattr(other, name)
                    )
                )

        return self.__class__(
            agg_simulation=self.agg_simulation + other.agg_simulation,
            rate_plan=self.rate_plan,
            date_ranges=self.date_ranges,
            pre_bills={**self.pre_bills, **other.pre_bills},
            post_bills={**self.post_bills, **other.post_bills},
        )

    @cached_property
    def date_ranges(self):
        return self.create_date_ranges(
            intervalframe=self.agg_simulation.pre_der_intervalframe
        )

    @cached_property
    def pre_bills(self):
        return self.generate_bills(
            meters=self.agg_simulation.pre_der_results,
            rate_plan=self.rate_plan,
        )

    @cached_property
    def post_bills(self):
        return self.generate_bills(
            meters=self.agg_simulation.post_der_results,
            rate_plan=self.rate_plan,
        )

    @cached_property
    def pre_DER_total(self):
        """
        Return sum of all bills for pre-DER scenario.
        """
        return self.pre_DER_bill_totals.sum().sum()

    @cached_property
    def post_DER_total(self):
        """
        Return sum of all bills for post-DER scenario.
        """
        return self.post_DER_bill_totals.sum().sum()

    @cached_property
    def pre_DER_bill_totals(self):
        """
        Return Pandas DataFrame containing bill totals for pre-DER scenario.
        """
        pre_bill_totals = {}
        for id_, bills in self.pre_bills.items():
            pre_bill_totals[id_] = {}
            for date, bill in bills.items():
                pre_bill_totals[id_][date] = bill.total

        return pd.DataFrame(pre_bill_totals)

    @cached_property
    def post_DER_bill_totals(self):
        """
        Return Pandas DataFrame containing bill totals for post-DER scenario.
        """
        post_bill_totals = {}
        for id_, bills in self.post_bills.items():
            post_bill_totals[id_] = {}
            for date, bill in bills.items():
                post_bill_totals[id_][date] = bill.total

        return pd.DataFrame(post_bill_totals)

    @cached_property
    def net_DER_bill_totals(self):
        """
        Return Pandas DataFrame containing net difference of pre-DER bills
        minus post-DER bills.
        """
        return self.post_DER_bill_totals - self.pre_DER_bill_totals

    @staticmethod
    def create_date_ranges(intervalframe):
        """
        Based on a ValidationIntervalFrame, create date ranges representing
        the first and last day of every month detected.

        :param intervalframe: ValidationIntervalFrame
        :return: list of start, end_limit datetime tuples
        """
        date_ranges = []
        for month, year in intervalframe.distinct_month_years:
            if month == 12:
                month_end_limit = datetime(year + 1, 1, 1)
            else:
                month_end_limit = datetime(year, month + 1, 1)
            date_ranges.append((datetime(year, month, 1), month_end_limit))

        return date_ranges

    @classmethod
    def create(cls, agg_simulation, rate_data):
        """
        Create pre-and-post-DER bills for all simulations.

        :param agg_simulation: AggregateDERProduct
        :param rate_plan: RatePlan object
        :param multiprocess: True to multiprocess
        :return: AggregateBillCalculation
        """
        return cls(agg_simulation=agg_simulation, rate_data=rate_data)

    @classmethod
    def generate_bills(cls, meters, rate_plan, multiprocess=False):
        simulation_bills = {}
        for meter, intervalframe in meters.items():
            date_ranges = cls.create_date_ranges(intervalframe)
            results = rate_plan.generate_many_bills(
                intervalframe=intervalframe,
                date_ranges=date_ranges,
                multiprocess=multiprocess,
            )
            simulation_bills[meter] = results

        return simulation_bills


@attr.s(frozen=True)
class AggregateGHGCalculation(DERCostCalculation):
    """
    Run GHG calculations across a AggregateDERProduct's many before and
    after load profiles.
    """

    agg_simulation = attr.ib(type=AggregateDERProduct)
    # GHG ValidationFrame288
    rate_data = attr.ib(type=ValidationFrame288)

    @property
    def ghg_frame288(self):
        return self.rate_data

    def __add__(self, other):
        """
        Allow AggregateGHGCalculation objects to be combined.
        """
        if self.ghg_frame288 != other.ghg_frame288:
            raise ValueError(
                "{} should equal {}.".format(
                    self.ghg_frame288, other.ghg_frame288
                )
            )

        return self.__class__(
            agg_simulation=self.agg_simulation + other.agg_simulation,
            rate_data=self.ghg_frame288,
        )

    @cached_property
    def pre_DER_total(self):
        """
        Return total tons of CO2 pre-DER.
        """
        return self.pre_DER_ghg_frame288.dataframe.sum().sum()

    @cached_property
    def post_DER_total(self):
        """
        Return total tons of CO2 post-DER.
        """
        return self.post_DER_ghg_frame288.dataframe.sum().sum()

    @cached_property
    def pre_DER_ghg_frame288(self):
        """
        Return 288 frame of month-hour GHG emissions pre-DER.
        """
        return (
            self.agg_simulation.pre_der_intervalframe.total_frame288
            * self.ghg_frame288
        )

    @cached_property
    def post_DER_ghg_frame288(self):
        """
        Return 288 frame of month-hour GHG emissions post-DER.
        """
        return (
            self.agg_simulation.post_der_intervalframe.total_frame288
            * self.ghg_frame288
        )

    @classmethod
    def create(cls, agg_simulation, rate_data):
        """
        Alias for __init__().
        """
        return cls(agg_simulation=agg_simulation, rate_data=rate_data)


@attr.s(frozen=True)
class AggregateResourceAdequacyCalculation(DERCostCalculation):
    """
    Run RA calculations across a AggregateDERProduct's many before and
    after load profiles.
    """

    agg_simulation = attr.ib(type=AggregateDERProduct)
    # system profile ValidationIntervalFrame
    rate_data = attr.ib(type=PowerIntervalFrame)

    @property
    def system_profile_intervalframe(self):
        return self.rate_data

    @cached_property
    def system_profile_year(self):
        years = set(self.system_profile_intervalframe.dataframe.index.year)
        if len(years) != 1:
            raise AttributeError(
                "Unique year not detected in {}.".format(years)
            )
        else:
            return years.pop()

    @cached_property
    def pre_DER_total(self):
        """
        Return sum of all monthly system peaks pre-DER (kW).
        """
        system_peaks = self.pre_DER_system_intervalframe.maximum_frame288
        return system_peaks.dataframe.max().sum()

    @cached_property
    def pre_DER_total_cost(self):
        """
        Return sum of all monthly system peaks pre-DER ($).
        """
        return self.pre_DER_total * RA_DOLLARS_PER_KW

    @cached_property
    def post_DER_total(self):
        """
        Return sum of all monthly system peaks post-DER. (kW)
        """
        system_peaks = self.post_DER_system_intervalframe.maximum_frame288
        return system_peaks.dataframe.max().sum()

    @cached_property
    def post_DER_total_cost(self):
        """
        Return sum of all monthly system peaks post-DER ($).
        """
        return self.post_DER_total * RA_DOLLARS_PER_KW

    @cached_property
    def net_impact(self):
        """
        Return total RA impact (post scenario - pre scenario) (kW).
        """
        return self.post_DER_total - self.pre_DER_total

    @cached_property
    def net_impact_cost(self):
        """
        Return total RA impact (post scenario - pre scenario) ($).
        """
        return self.net_impact * RA_DOLLARS_PER_KW

    @cached_property
    def pre_DER_system_intervalframe(self):
        """
        Return pre-DER SystemProfileIntervalFrame.
        """
        return self.system_profile_intervalframe

    @cached_property
    def post_DER_system_intervalframe(self):
        """
        Add PowerIntervalFrame consisting of net kW changes due to a DER.
        The PowerIntervalFrame index year will be changed so that the
        SystemProfile dates align with PowerIntervalFrame dates.
        """
        intervalframe = self.agg_simulation.der_intervalframe
        if (
            intervalframe.end_limit_timestamp - intervalframe.start_timestamp
        ) > timedelta(days=366):
            raise RuntimeError("PowerIntervalFrame must be one year or less.")

        # shift BatteryIntervalFrame year to align with SystemProfile
        updated_index = intervalframe.dataframe.index.map(
            lambda t: t.replace(year=self.system_profile_year)
        )
        intervalframe.dataframe.index = updated_index

        return self.system_profile_intervalframe + intervalframe

    @classmethod
    def create(cls, agg_simulation, rate_data):
        """
        Alias for __init__().
        """
        return cls(agg_simulation=agg_simulation, rate_data=rate_data)


@attr.s(frozen=True)
class AggregateProcurementCostCalculation(DERCostCalculation):
    """
    Run procurement cost calculations across a AggregateDERProduct's many
    before and after load profiles.Procurement rates can change on a regular
    interval (ex. 5-minute, 15-minute, 60-minute basis).
    """

    agg_simulation = attr.ib(type=AggregateDERProduct)
    # procurement rate ValidationIntervalFrame
    rate_data = attr.ib(type=ProcurementRateIntervalFrame)

    @property
    def procurement_rate_intervalframe(self):
        return self.rate_data

    @cached_property
    def pre_DER_total(self):
        """
        Total procurement costs pre-DER.
        """
        return self.pre_DER_procurement_cost_intervalframe.dataframe["$"].sum()

    @cached_property
    def post_DER_total(self):
        """
        Total procurement costs post-DER.
        """
        return self.post_DER_procurement_cost_intervalframe.dataframe[
            "$"
        ].sum()

    @cached_property
    def pre_DER_procurement_cost_intervalframe(self):
        """
        ProcurementCostIntervalFrame with pre-DER costs on an
        interval-by-interval basis.

        Interval readings will be converted to EnergyIntervalFrame for proper
        calculation.
        """
        pre_der_intervalframe = (
            self.agg_simulation.pre_der_intervalframe.energy_intervalframe
        )

        df = pd.concat(
            [
                pre_der_intervalframe.dataframe,
                self.procurement_rate_intervalframe.dataframe,
            ],
            axis=1,
            join="inner",
        )
        df["$"] = df["kwh"] * df["$/kwh"]
        df = df.drop(columns=["$/kwh"])

        return ProcurementCostIntervalFrame(dataframe=df)

    @cached_property
    def post_DER_procurement_cost_intervalframe(self):
        """
        ProcurementCostIntervalFrame with post-DER costs on an
        interval-by-interval basis.

        Interval readings will be converted to EnergyIntervalFrame for proper
        calculation.
        """
        post_der_intervalframe = (
            self.agg_simulation.post_der_intervalframe.energy_intervalframe
        )

        df = pd.concat(
            [
                post_der_intervalframe.dataframe,
                self.procurement_rate_intervalframe.dataframe,
            ],
            axis=1,
            join="inner",
        )
        df["$"] = df["kwh"] * df["$/kwh"]
        df = df.drop(columns=["$/kwh"])

        return ProcurementCostIntervalFrame(dataframe=df)

    @classmethod
    def create(cls, agg_simulation, rate_data):
        """
        Alias for __init__().
        """
        return cls(agg_simulation=agg_simulation, rate_data=rate_data)
