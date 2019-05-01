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
        return {x for x in self.rate_data.keys() if "fixed" in x}

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
                numerator__name="$", denominator__name="day"
            )
        else:
            return RateUnit.objects.get(
                numerator__name=fixed_charge_units.split("/")[0],
                denominator__name=fixed_charge_units.split("/")[1],
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
        return {x for x in self.rate_data.keys() if "energy" in x}

    @property
    def energy_rates(self):
        """
        Return energy rates applied on a per kWh basis.
        """
        return self.rate_data.get("energyRateStrux", [])

    @property
    def energy_weekday_schedule(self):
        """
        Returns the weekday schedule for energy charges.

        :return: ValidationFrame288
        """
        return self.get_tou_schedule("energyWeekdaySched")

    @property
    def energy_weekend_schedule(self):
        """
        Returns the weekend schedule for energy charges.

        :return: ValidationFrame288
        """
        return self.get_tou_schedule("energyWeekendSched")

    @property
    def demand_rate_keys(self):
        """
        Return set of demand-rate keys found in self.rate_data.
        """
        return {x for x in self.rate_data.keys() if "demand" in x}

    @staticmethod
    def convert_matrix_to_frame288(matrix):
        """
        Converts a 12 x 24 matrix commonly found in OpenEI data to a
        ValidationFrame288 object.

        :param matrix: 12 x 24 matrix (array of arrays)
        :return: ValidationFrame288
        """
        dataframe = pd.DataFrame(matrix)
        dataframe.index = dataframe.index + 1
        dataframe.index = pd.to_numeric(dataframe.index)
        dataframe.columns = pd.to_numeric(dataframe.columns)

        if not dataframe.empty:
            return ValidationFrame288(dataframe.transpose())
        else:
            return ValidationFrame288(ValidationFrame288.default_dataframe)

    def get_tou_schedule(self, lookup_key):
        """
        Returns a 12 x 24 lookup table of TOU demand or energy schedules.

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

        return self.convert_matrix_to_frame288(tou_matrix)


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
        8. total
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

        # compute bill
        self.compute_fixed_meter_charges()
        self.compute_fixed_rate_charges()
        self.compute_energy_rate_charges()

    @staticmethod
    def validate_intervalframe(intervalframe):
        """
        Validates intervalframe does not contain too many days.
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
        Validates that count_unit is a DataUnit and rate_unit is a RateUnit
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
        Returns first found decimal in rate_string.

        :param rate_string: string
        :return: float
        """
        return float(re.findall(r"[-+]?\d*\.\d+|\d+", rate_string)[0])

    def add_charge(
        self,
        category,
        description,
        count,
        count_unit,
        rate,
        rate_unit,
        tou_period=None,
    ):
        """
        Adds charge to ValidationBill.

        :param category: string
        :param description: string
        :param count: float
        :param count_unit: DataUnit
        :param rate: float
        :param rate_unit: RateUnit
        :param tou_period: string/int
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
                "total": count * rate,
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
                    numerator__name="$", denominator__name="month"
                ),
            )

    def compute_fixed_rate_charges(self):
        """
        Extracts fixed rates from self.openei_rate_data and fixed counts from
        self.intervalframe to compute fixed charges.
        """
        # TODO: Remove optional fixed charges/credits
        for rate in self.openei_rate_data.fixed_rates:
            if "/day" in rate.get("key"):
                rate_unit = RateUnit.objects.get(
                    numerator__name="$", denominator__name="day"
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

    def get_energy_count(self, tou_key):
        """
        Return energy counts (kWh) based off of a tou_key.

        :param tou_key: int
        :return: int
        """
        weekday_totals = self.intervalframe.filter_by_weekday().total_frame288
        weekday_schedule = self.openei_rate_data.energy_weekday_schedule
        weekend_totals = self.intervalframe.filter_by_weekend().total_frame288
        weekend_schedule = self.openei_rate_data.energy_weekend_schedule

        filtered_weekday_totals = weekday_totals.dataframe * (
            weekday_schedule.dataframe == tou_key
        )
        filtered_weekend_totals = weekend_totals.dataframe * (
            weekend_schedule.dataframe == tou_key
        )

        return (filtered_weekday_totals + filtered_weekend_totals).sum().sum()

    def get_energy_tou_key_value(self, tou_key):
        """
        Returns TOU description if it exists. (ex. 'TOU-winter:Off-Peak')
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
        Extracts energy rates from self.openei_rate_data and energy counts from
        self.intervalframe to compute fixed charges.
        """
        count_unit = DataUnit.objects.get(name="kwh")
        rate_unit = RateUnit.objects.get(
            numerator__name="$", denominator=count_unit
        )

        for tou_key, rates in enumerate(self.openei_rate_data.energy_rates):
            # initialize counts
            energy_count = self.get_energy_count(tou_key)
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
                    count_unit=count_unit,
                    rate=rate,
                    rate_unit=rate_unit,
                    tou_period=tou_key,
                )

                # 4. update counts
                energy_count = energy_count - billing_count
                billed_so_far = billed_so_far + billing_count
