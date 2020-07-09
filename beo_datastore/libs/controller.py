import attr
from attr.validators import instance_of
from cached_property import cached_property
from datetime import timedelta
import pandas as pd

from beo_datastore.libs.der.builder import AggregateDERProduct
from beo_datastore.libs.intervalframe import (
    PowerIntervalFrame,
    ValidationFrame288,
)
from beo_datastore.libs.procurement import (
    ProcurementCostIntervalFrame,
    ProcurementRateIntervalFrame,
)


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
        Return the cost calculation of a pre-DER scenario.
        """
        raise NotImplementedError(
            "pre_DER_total must be set in {}".format(self.__class__)
        )

    @property
    def post_DER_total(self):
        """
        Return the cost calculation of a post-DER scenario.
        """
        raise NotImplementedError(
            "post_DER_total must be set in {}".format(self.__class__)
        )

    @property
    def net_impact(self):
        """
        Return the cost calculation of a post-DER scenario minus the cost
        calculation of a pre-DER scenario.
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

    agg_simulation = attr.ib(validator=instance_of(AggregateDERProduct))
    rate_plan = attr.ib()
    date_ranges = attr.ib(validator=instance_of(list))
    pre_bills = attr.ib(validator=instance_of(dict))
    post_bills = attr.ib(validator=instance_of(dict))

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

    @classmethod
    def create(
        cls, agg_simulation, rate_plan, date_ranges, multiprocess=False
    ):
        """
        Create pre-and-post-DER bills for all simulations.

        :param agg_simulation: AggregateDERProduct
        :param rate_plan: RatePlan object
        :param date_ranges: list of start, end_limit datetime tuples
        :param multiprocess: True to multiprocess
        :return: AggregateBillCalculation
        """
        pre_bills = cls.generate_bills(
            meters=agg_simulation.pre_der_results,
            rate_plan=rate_plan,
            date_ranges=date_ranges,
            multiprocess=multiprocess,
        )
        post_bills = cls.generate_bills(
            meters=agg_simulation.post_der_results,
            rate_plan=rate_plan,
            date_ranges=date_ranges,
            multiprocess=multiprocess,
        )

        return cls(
            agg_simulation=agg_simulation,
            rate_plan=rate_plan,
            date_ranges=date_ranges,
            pre_bills=pre_bills,
            post_bills=post_bills,
        )

    @classmethod
    def generate_bills(
        cls, meters, rate_plan, date_ranges, multiprocess=False
    ):
        simulation_bills = {}
        for meter, intervalframe in meters.items():
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

    agg_simulation = attr.ib(validator=instance_of(AggregateDERProduct))
    ghg_frame288 = attr.ib(validator=instance_of(ValidationFrame288))

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
            ghg_frame288=self.ghg_frame288,
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

    @cached_property
    def comparison_table(self):
        """
        Return table of monthly pre-DER and post-DER values.
        """
        df = pd.merge(
            pd.DataFrame(self.pre_DER_ghg_frame288.dataframe.sum()),
            pd.DataFrame(self.post_DER_ghg_frame288.dataframe.sum()),
            how="inner",
            left_index=True,
            right_index=True,
        )
        return df.append(df.sum().rename("Total")).rename(
            columns={"0_x": "before", "0_y": "after"}
        )

    @classmethod
    def create(cls, agg_simulation, ghg_frame288):
        """
        Alias for __init__().
        """
        return cls(agg_simulation=agg_simulation, ghg_frame288=ghg_frame288)


@attr.s(frozen=True)
class AggregateResourceAdequacyCalculation(DERCostCalculation):
    """
    Run RA calculations across a AggregateDERProduct's many before and
    after load profiles.
    """

    agg_simulation = attr.ib(validator=instance_of(AggregateDERProduct))
    system_profile_intervalframe = attr.ib(
        validator=instance_of(PowerIntervalFrame)
    )

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
        Return sum of all monthly system peaks pre-DER.
        """
        system_peaks = self.pre_DER_system_intervalframe.maximum_frame288
        return system_peaks.dataframe.max().sum()

    @cached_property
    def post_DER_total(self):
        """
        Return sum of all monthly system peaks post-DER.
        """
        system_peaks = self.post_DER_system_intervalframe.maximum_frame288
        return system_peaks.dataframe.max().sum()

    @cached_property
    def net_impact(self):
        """
        Return total RA impact (post scenario - pre scenario) (kW).
        """
        return self.post_DER_total - self.pre_DER_total

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
        intervalframe = self.agg_simulation.net_intervalframe
        if (
            intervalframe.end_limit_timestamp - intervalframe.start_timestamp
        ) > timedelta(days=366):
            raise RuntimeError("PowerIntervalFrame must be one year or less.")

        # shift BatteryIntervalFrame year to align with SystemProfile
        updated_index = intervalframe.dataframe.index.map(
            lambda t: t.replace(year=self.system_profile_year)
        )
        intervalframe.dataframe.index = updated_index

        return intervalframe + self.system_profile_intervalframe

    @cached_property
    def comparison_table(self):
        """
        Return table of monthly pre-DER and post-DER values.
        """
        pre_DER_max_288 = self.pre_DER_system_intervalframe.maximum_frame288
        post_DER_max_288 = self.post_DER_system_intervalframe.maximum_frame288
        df = pd.merge(
            pd.DataFrame(pre_DER_max_288.dataframe.max()),
            pd.DataFrame(post_DER_max_288.dataframe.max()),
            how="inner",
            left_index=True,
            right_index=True,
        )
        return df.append(df.sum().rename("Total")).rename(
            columns={"0_x": "before", "0_y": "after"}
        )

    @classmethod
    def create(cls, agg_simulation, system_profile_intervalframe):
        """
        Alias for __init__().
        """
        return cls(
            agg_simulation=agg_simulation,
            system_profile_intervalframe=system_profile_intervalframe,
        )


@attr.s(frozen=True)
class AggregateProcurementCostCalculation(DERCostCalculation):
    """
    Run procurement cost calculations across a AggregateDERProduct's many
    before and after load profiles.Procurement rates can change on a regular
    interval (ex. 5-minute, 15-minute, 60-minute basis).
    """

    agg_simulation = attr.ib(validator=instance_of(AggregateDERProduct))
    procurement_rate_intervalframe = attr.ib(
        validator=instance_of(ProcurementRateIntervalFrame)
    )

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
    def create(cls, agg_simulation, procurement_rate_intervalframe):
        """
        Alias for __init__().
        """
        return cls(
            agg_simulation=agg_simulation,
            procurement_rate_intervalframe=procurement_rate_intervalframe,
        )
