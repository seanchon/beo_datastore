from functools import reduce
import pandas as pd
import re
import warnings

from beo_datastore.libs.intervalframe import (
    ValidationDataFrame,
    ValidationFrame288,
)

from reference.reference_model.models import DataUnit, RateUnit


class OpenEIRateData(object):
    """
    Container class for extracting fixed, energy, and demand charges from
    OpenEI's U.S. Utility Rate Database (JSON data).

    Homepage: https://openei.org/wiki/Utility_Rate_Database
    JSON data: https://openei.org/apps/USURDB/download/usurdb.json.gz
    """

    def __init__(self, rate_data):
        """
        Load a single OpenEI Utility Rate Object.

        :param rate_data: dict
        """
        self.rate_data = rate_data

    @property
    def fixed_rate_keys(self):
        """
        Return set of fixed-rate keys found in self.rate_data.
        """
        return {x for x in self.rate_data.keys() if "fixed" in x.lower()}

    @property
    def fixed_rates(self):
        """
        Return fixed rates applied on a per day, per month, etc. basis.
        """
        return self.rate_data.get("fixedKeyVals", [])

    @property
    def fixed_rate_unit(self):
        """
        Return fixed rate unit (ex. $/day, $/month).
        """
        fixed_charge_units = self.rate_data.get("fixedChargeUnits", None)

        # defaults to $/day
        if fixed_charge_units is None:
            return RateUnit.objects.get(
                numerator__name__iexact="$", denominator__name__iexact="day"
            )
        else:
            return RateUnit.objects.get(
                numerator__name__iexact=fixed_charge_units.split("/")[0],
                denominator__name__iexact=fixed_charge_units.split("/")[1],
            )

    @property
    def fixed_meter_charge(self):
        """
        Return fixed meter charge per bill ($/month).
        """
        return self.rate_data.get("fixedChargeFirstMeter", 0)

    @property
    def energy_rate_keys(self):
        """
        Return set of energy-rate keys found in self.rate_data.
        """
        return {x for x in self.rate_data.keys() if "energy" in x.lower()}

    @property
    def energy_rates(self):
        """
        Return energy rates applied on a per kWh basis.
        """
        return self.rate_data.get("energyRateStrux", [])

    @property
    def energy_rate_unit(self):
        """
        RateUnit for energy rates.
        """
        return RateUnit.objects.get(
            numerator__name__iexact="$", denominator__name__iexact="kwh"
        )

    @property
    def energy_weekday_schedule(self):
        """
        Return the weekday schedule for energy charges.

        :return: ValidationFrame288
        """
        return self.get_tou_schedule("energyWeekdaySched")

    @property
    def energy_weekend_schedule(self):
        """
        Return the weekend schedule for energy charges.

        :return: ValidationFrame288
        """
        return self.get_tou_schedule("energyWeekendSched")

    @property
    def demand_rate_keys(self):
        """
        Return set of demand-rate keys found in self.rate_data.
        """
        return {x for x in self.rate_data.keys() if "demand" in x.lower()}

    @property
    def demand_min(self):
        """
        Minimum demand allowable for collection of rates.
        """
        return self.rate_data.get("demandMin", 0)

    @property
    def demand_max(self):
        """
        Maximum demand allowable for collection of rates.
        """
        return self.rate_data.get("demandMax", float("inf"))

    @property
    def demand_rates(self):
        """
        TOU-based demand rates.
        """
        return self.rate_data.get("demandRateStrux", [])

    @property
    def demand_rate_unit(self):
        """
        RateUnit for TOU-based demand rates.
        """
        demand_rate_units = self.rate_data.get("demandRateUnits", None)

        # defaults to $/kW
        if demand_rate_units is None:
            return RateUnit.objects.get(
                numerator__name__iexact="$", denominator__name__iexact="kW"
            )
        else:
            return RateUnit.objects.get(
                numerator__name__iexact="$",
                denominator__name__iexact=demand_rate_units,
            )

    @property
    def demand_weekday_schedule(self):
        """
        Return the weekday schedule for demand charges.

        :return: ValidationFrame288
        """
        return self.get_tou_schedule("demandWeekdaySched")

    @property
    def demand_weekend_schedule(self):
        """
        Return the weekend schedule for demand charges.

        :return: ValidationFrame288
        """
        return self.get_tou_schedule("demandWeekendSched")

    @property
    def flat_demand_rates(self):
        """
        Month-based (seasonal) demand rates.
        """
        return self.rate_data.get("flatDemandStrux", [])

    @property
    def flat_demand_rate_unit(self):
        """
        RateUnit for month-based (seasonal) demand rates.
        """
        flat_demand_units = self.rate_data.get("flatDemandUnits", None)

        # defaults to $/kW
        if flat_demand_units is None:
            return RateUnit.objects.get(
                numerator__name__iexact="$", denominator__name__iexact="kW"
            )
        else:
            return RateUnit.objects.get(
                numerator__name__iexact="$",
                denominator__name__iexact=flat_demand_units,
            )

    @property
    def flat_demand_schedule(self):
        """
        Return ValidationFrame288 representation of month-based (seasonal)
        demand schedule.
        """
        return ValidationFrame288.convert_matrix_to_frame288(
            [
                [x] * 23
                for x in self.rate_data.get("flatDemandMonths", [None] * 12)
            ]
        )

    def get_tou_schedule(self, lookup_key):
        """
        Return a 12 x 24 lookup table of TOU demand or energy schedules.

        :param lookup_key: choice of "demandWeekdaySched",
            "demandWeekendSched", "energyWeekdaySched", "energyWeekendSched"
        :return: ValidationFrame288 object
        """
        if lookup_key not in [
            "demandWeekdaySched",
            "demandWeekendSched",
            "energyWeekdaySched",
            "energyWeekendSched",
        ]:
            raise KeyError(
                "Choices are demandWeekdaySched, demandWeekendSched, "
                "energyWeekdaySched, energyWeekendSched."
            )

        tou_matrix = self.rate_data.get(lookup_key, None)

        return ValidationFrame288.convert_matrix_to_frame288(tou_matrix)


class ValidationBill(ValidationDataFrame):
    """
    Container class for pandas DataFrames with the following columns:

        1. id
        2. category
        3. description
        4. count
        5. count_unit
        6. rate
        7. rate_unit
        8. pro_rata
        9. total
    """

    default_dataframe = pd.DataFrame(
        columns=[
            "category",
            "description",
            "tou_period",
            "count",
            "count_unit",
            "rate",
            "rate_unit",
            "pro_rata",
            "total",
        ]
    )

    def __init__(self, intervalframe, openei_rate_data):
        """
        Creates a ValidationBill with a ValidationIntervalFrame object and
        RateCollection object.

        :param intervalframe: ValidationIntervalFrame
        :param openei_rate_data: OpenEIRateData
        """
        self.validate_intervalframe(intervalframe)
        self.intervalframe = intervalframe
        self.openei_rate_data = openei_rate_data
        self.dataframe = self.default_dataframe

        # compute bill only if self.dataframe is empty
        self.compute_bill()

    @property
    def start_datetime(self):
        """
        Return earliest timestamp as datetime object.
        """
        return self.intervalframe.start_datetime

    @property
    def end_datetime(self):
        """
        Return latest timestamp as datetime object.
        """
        return self.intervalframe.end_datetime

    @property
    def total(self):
        """
        Return total of all charges.
        """
        return self.dataframe["total"].sum()

    @property
    def total_dataframe(self):
        """
        Return self.dataframe with additional total line.
        """
        count_df = pd.DataFrame(
            self.dataframe["count"].sum(), index=["Total"], columns=["count"]
        )
        total_df = pd.DataFrame(
            self.dataframe["total"].sum(), index=["Total"], columns=["total"]
        )

        return self.dataframe.append(
            pd.merge(
                count_df,
                total_df,
                how="inner",
                left_index=True,
                right_index=True,
            ),
            sort=False,
        ).fillna("")

    @staticmethod
    def validate_intervalframe(intervalframe):
        """
        Validate intervalframe does not contain too many days.
        """
        if intervalframe.days > 35:
            warning_message = (
                "intervalframe contains more than a month's worth of data "
                "({} days), which can create erroneous "
                "bills.".format(intervalframe.days)
            )
            warnings.warn(warning_message)

    @staticmethod
    def validate_units(count_unit, rate_unit):
        """
        Validate that count_unit is a DataUnit and rate_unit is a RateUnit
        and that multiplying them yields a dollar amount.

        :param count_unit: DataUnit
        :param rate_unit: RateUnit
        """
        if not isinstance(count_unit, DataUnit) or not isinstance(
            rate_unit, RateUnit
        ):
            raise TypeError(
                "count_unit must be a DataUnit and rate_unit must be a "
                "RateUnit."
            )
        if (
            count_unit != rate_unit.denominator
            or rate_unit.numerator.name != "$"
        ):
            raise TypeError(
                "Multiplying count_unit by rate_unit must yield a dollar "
                "amount."
            )

    @staticmethod
    def extract_rate(rate_string):
        """
        Return first found decimal in rate_string.

        :param rate_string: string
        :return: float
        """
        return float(re.findall(r"[-+]?\d*\.\d+|\d+", rate_string)[0])

    def compute_bill(self):
        """
        Compute bill only if self.dataframe is empty.
        """
        if self.dataframe.empty:
            self.compute_fixed_meter_charges()
            self.compute_fixed_rate_charges()
            self.compute_energy_rate_charges()
            self.compute_demand_rate_charges()
            self.compute_flat_demand_rate_charges()

    def add_charge(
        self,
        category,
        description,
        count,
        count_unit,
        rate,
        rate_unit,
        tou_period=None,
        pro_rata=1,
    ):
        """
        Add charge to ValidationBill.

        :param category: string
        :param description: string
        :param count: float
        :param count_unit: DataUnit
        :param rate: float
        :param rate_unit: RateUnit
        :param tou_period: string/int
        :param pro_rata: float proportion to prorate (ex. .80)
        """
        self.validate_units(count_unit, rate_unit)

        self.dataframe = self.dataframe.append(
            {
                "category": category,
                "description": description,
                "tou_period": tou_period,
                "count": count,
                "count_unit": count_unit,
                "rate": rate,
                "rate_unit": rate_unit,
                "pro_rata": pro_rata,
                "total": count * rate * pro_rata,
            },
            ignore_index=True,
        )

    def compute_fixed_meter_charges(self):
        """
        Add fixed meter charge to bill if applicable.
        """
        if self.openei_rate_data.fixed_meter_charge:
            self.add_charge(
                category="fixed",
                description="Fixed Charge First Meter",
                count=1,
                count_unit=DataUnit.objects.get(name="month"),
                rate=self.openei_rate_data.fixed_meter_charge,
                rate_unit=RateUnit.objects.get(
                    numerator__name__iexact="$",
                    denominator__name__iexact="month",
                ),
            )

    def compute_fixed_rate_charges(self):
        """
        Extract fixed rates from self.openei_rate_data and fixed counts from
        self.intervalframe to compute fixed charges.
        """
        # TODO: Remove optional fixed charges/credits
        for rate in self.openei_rate_data.fixed_rates:
            if "/day" in rate.get("key"):
                rate_unit = RateUnit.objects.get(
                    numerator__name__iexact="$",
                    denominator__name__iexact="day",
                )
            else:
                rate_unit = self.openei_rate_data.fixed_rate_unit
            period = rate_unit.denominator

            if period.name == "month":
                count = 1
            elif period.name == "day":
                count = self.intervalframe.days
            else:
                raise LookupError(
                    "Period {} not expected for fixed charge.".format(period)
                )

            self.add_charge(
                category="fixed",
                description=rate.get("key"),
                count=count,
                count_unit=period,
                rate=self.extract_rate(rate.get("val")),
                rate_unit=rate_unit,
            )

    @staticmethod
    def get_energy_count(
        intervalframe, weekday_schedule, weekend_schedule, tou_key
    ):
        """
        Return energy counts (kWh) based off of a tou_key.

        :param intervalframe: ValidationIntervalFrame
        :param weekday_schedule: ValidationFrame288
        :param weekend_schedule: ValidationFrame288
        :param tou_key: int
        :return: float
        """
        weekday_totals = intervalframe.filter_by_weekday().total_frame288
        filtered_weekday_totals = weekday_totals * weekday_schedule.get_mask(
            tou_key
        )

        weekend_totals = intervalframe.filter_by_weekend().total_frame288
        filtered_weekend_totals = weekend_totals * weekend_schedule.get_mask(
            tou_key
        )

        return (
            (filtered_weekday_totals + filtered_weekend_totals)
            .dataframe.sum()
            .sum()
        )

    def get_billing_energy_count(self, tou_key):
        """
        Return billing energy counts (kWh) based off of a tou_key.

        :param tou_key: int
        :return: float
        """
        return self.get_energy_count(
            self.intervalframe,
            self.openei_rate_data.energy_weekday_schedule,
            self.openei_rate_data.energy_weekend_schedule,
            tou_key,
        )

    def get_energy_tou_key_value(self, tou_key):
        """
        Return TOU description if it exists. (ex. 'TOU-winter:Off-Peak')
        """
        tou_key_values = self.openei_rate_data.rate_data.get(
            "energyKeyVals", []
        )

        if not tou_key_values:
            return ""
        else:
            return tou_key_values[tou_key].get("key", "")

    def compute_energy_rate_charges(self):
        """
        Extract energy rates from self.openei_rate_data and energy counts from
        self.intervalframe to compute energy charges.
        """
        for tou_key, rates in enumerate(self.openei_rate_data.energy_rates):
            # initialize counts
            energy_count = self.get_billing_energy_count(tou_key)
            billed_so_far = 0

            for tier in rates.get("energyRateTiers", []):
                # 1. extract rate data
                rate = tier.get("rate", 0)
                if rate == 0:
                    warnings.warn("Rate missing. 0 $/kwh used instead.")
                max_kwh_per_day = tier.get("max", float("inf"))
                max_kwh_per_bill = max_kwh_per_day * self.intervalframe.days

                # 2. calculate energy usage per tier
                if energy_count <= 0:
                    if billed_so_far == 0 and energy_count < 0:
                        # bill all net production in first tier
                        # TODO: are NEM export bills tiered?
                        billing_count = energy_count
                    else:
                        # nothing left to bill
                        break
                else:
                    if (energy_count + billed_so_far) >= max_kwh_per_bill:
                        # bill only to max kWh per tier
                        billing_count = max_kwh_per_bill - billed_so_far
                    else:
                        # bill remaining energy count
                        billing_count = energy_count

                # 3. apply charge
                description = "Energy Charge"
                if self.get_energy_tou_key_value(tou_key):
                    description += " - {}".format(
                        self.get_energy_tou_key_value(tou_key)
                    )
                if max_kwh_per_day != float("inf"):
                    description += " ({} max kWh/day)".format(max_kwh_per_day)

                self.add_charge(
                    category="energy",
                    description=description,
                    count=billing_count,
                    count_unit=DataUnit.objects.get(name="kwh"),
                    rate=rate,
                    rate_unit=self.openei_rate_data.energy_rate_unit,
                    tou_period=tou_key,
                )

                # 4. update counts
                energy_count = energy_count - billing_count
                billed_so_far = billed_so_far + billing_count

    @staticmethod
    def get_demand_peak(
        intervalframe, weekday_schedule, weekend_schedule, tou_key
    ):
        """
        Return TOU-based demand peak power (kW) based off of a tou_key.

        :param tou_key: int
        :return: float
        """
        weekday_peaks = intervalframe.filter_by_weekday().maximum_frame288
        weekend_peaks = intervalframe.filter_by_weekend().maximum_frame288

        weekday_max = (
            (weekday_peaks * weekday_schedule.get_mask(tou_key))
            .dataframe.max()
            .max()
        )
        weekend_max = (
            (weekend_peaks * weekend_schedule.get_mask(tou_key))
            .dataframe.max()
            .max()
        )

        return max(weekday_max, weekend_max)

    def get_billing_demand_peak(self, tou_key):
        """
        Return billing TOU-based demand peak power (kW) based off of a tou_key.

        :param tou_key: int
        :return: float
        """
        return self.get_demand_peak(
            self.intervalframe,
            self.openei_rate_data.demand_weekday_schedule,
            self.openei_rate_data.demand_weekend_schedule,
            tou_key,
        )

    def get_demand_days(self, tou_key):
        """
        Return the number of billing days matching tou_key for prorating
        TOU-based demand charges.

        :param tou_key: int
        :return: int
        """
        weekday_schedule = self.openei_rate_data.demand_weekday_schedule
        months_1 = weekday_schedule.get_mask(tou_key).dataframe.any()
        months_1 = months_1.index[months_1 == 1].to_list()

        weekend_schedule = self.openei_rate_data.demand_weekend_schedule
        months_2 = weekend_schedule.get_mask(tou_key).dataframe.any()
        months_2 = months_2.index[months_2 == 1].to_list()

        return self.intervalframe.filter_by_months(
            months=set(months_1 + months_2)
        ).days

    def compute_demand_rate_charges(self):
        """
        Extract demand rates from self.openei_rate_data and demand peaks from
        self.intervalframe to compute demand charges.
        """
        for tou_key, rates in enumerate(self.openei_rate_data.demand_rates):
            for tier in rates.get("demandRateTiers", []):
                # TODO: Are there tiered demand charges?
                rate = tier.get("rate", 0)
                max_demand_per_tier = tier.get("max", float("inf"))
                demand_peak = self.get_billing_demand_peak(tou_key)
                demand_days = self.get_demand_days(tou_key)
                if rate and demand_peak and demand_days:
                    description = "Demand Charge"
                    if demand_days != self.intervalframe.days:
                        description += " ({}/{} pro rata)".format(
                            demand_days, self.intervalframe.days
                        )
                    if max_demand_per_tier != float("inf"):
                        description += " ({} max kW/tier)".format(
                            max_demand_per_tier
                        )

                    self.add_charge(
                        category="demand",
                        description=description,
                        count=demand_peak,
                        count_unit=DataUnit.objects.get(name="kw"),
                        rate=rate,
                        rate_unit=self.openei_rate_data.demand_rate_unit,
                        tou_period=tou_key,
                        pro_rata=(demand_days / self.intervalframe.days),
                    )

    @staticmethod
    def get_flat_demand_peak(intervalframe, schedule, tou_key):
        """
        Return flat-demand peak power (kW) based off of a tou_key.

        :param intervalframe: ValidationIntervalFrame
        :param schedule: array of ints
        :param tou_key: int
        :return: float
        """
        peaks = intervalframe.maximum_frame288
        return (peaks * schedule.get_mask(tou_key)).dataframe.max().max()

    def get_billing_flat_demand_peak(self, tou_key):
        """
        Return billing flat-demand peak power (kW) based off of a tou_key.

        :param tou_key: int
        :return: float
        """
        return self.get_flat_demand_peak(
            self.intervalframe,
            self.openei_rate_data.flat_demand_schedule,
            tou_key,
        )

    def get_flat_demand_days(self, tou_key):
        """
        Return the number of billing days matching tou_key for prorating flat
        demand charges.

        :param tou_key: int
        :return: int
        """
        schedule = self.openei_rate_data.flat_demand_schedule
        months = schedule.get_mask(tou_key).dataframe.any()
        months = months.index[months == 1].to_list()

        return self.intervalframe.filter_by_months(months=months).days

    def compute_flat_demand_rate_charges(self):
        """
        Extract flat demand rates from self.openei_rate_data and demand peaks
        from self.intervalframe to compute flat demand charges.
        """
        for tou_key, rates in enumerate(
            self.openei_rate_data.flat_demand_rates
        ):
            for tier in rates.get("flatDemandTiers", []):
                # TODO: Are there tiered demand charges?
                rate = tier.get("rate", 0)
                max_demand_per_tier = tier.get("max", float("inf"))
                demand_peak = self.get_billing_flat_demand_peak(tou_key)
                demand_days = self.get_flat_demand_days(tou_key)
                if rate and demand_peak and demand_days:
                    description = "Flat Demand Charge"
                    if demand_days != self.intervalframe.days:
                        description += " ({}/{} pro rata)".format(
                            demand_days, self.intervalframe.days
                        )
                    if max_demand_per_tier != float("inf"):
                        description += " ({} max kW/tier)".format(
                            max_demand_per_tier
                        )

                    rate_unit = self.openei_rate_data.flat_demand_rate_unit
                    self.add_charge(
                        category="demand",
                        description=description,
                        count=demand_peak,
                        count_unit=DataUnit.objects.get(name="kw"),
                        rate=rate,
                        rate_unit=rate_unit,
                        tou_period=tou_key,
                        pro_rata=(demand_days / self.intervalframe.days),
                    )


class BillingCollection(object):
    """
    Container class for a collection of one or more ValidationBills.
    """

    def __init__(self, bills):
        """
        Initialize with a collection of ValidationBills.

        :param bills: list of ValidationBills
        """
        self.validate_bills(bills)
        self.bills = bills

    @property
    def start_datetime(self):
        """
        Return earliest timestamp as datetime object.
        """
        return self.intervalframe.start_datetime

    @property
    def end_datetime(self):
        """
        Return latest timestamp as datetime object.
        """
        return self.intervalframe.end_datetime

    @property
    def total(self):
        """
        Return total of all bill totals.
        """
        return reduce(lambda x, y: x + y, [x.total for x in self.bills])

    @property
    def dataframe(self):
        """
        Return billing dataframe representing all bills.
        """
        return reduce(
            lambda x, y: x.append(y), [x.dataframe for x in self.bills]
        )

    @property
    def total_dataframe(self):
        """
        Return self.dataframe with additional total line.
        """
        count_df = pd.DataFrame(
            self.dataframe["count"].sum(), index=["Total"], columns=["count"]
        )
        total_df = pd.DataFrame(
            self.dataframe["total"].sum(), index=["Total"], columns=["total"]
        )

        return self.dataframe.append(
            pd.merge(
                count_df,
                total_df,
                how="inner",
                left_index=True,
                right_index=True,
            ),
            sort=False,
        ).fillna("")

    @property
    def intervalframe(self):
        """
        Return ValidationIntervalFrame representing all bills.
        """
        return reduce(
            lambda x, y: x.merge_intervalframe(y),
            [x.intervalframe for x in self.bills],
        )

    @property
    def openei_rate_dict(self):
        """
        Return list of OpenEIRateData dicts representing all bills.
        """
        return [x.openei_rate_data.rate_data for x in self.bills]

    @staticmethod
    def validate_bills(bills):
        """
        Validates all bills are ValidationBills.
        """
        for bill in bills:
            if not isinstance(bill, ValidationBill):
                raise TypeError("{} must be a ValidationBill".format(bill))
