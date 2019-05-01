import pandas as pd
import re
import warnings

from beo_datastore.libs.intervalframe import ValidationDataFrame

from reference.reference_model.models import DataUnit, RateUnit


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
            "count",
            "count_unit",
            "rate",
            "rate_unit",
            "total",
        ]
    )

    def __init__(self, intervalframe, rate_collection):
        """
        Initializes a ValidationBill with a ValidationIntervalFrame object and
        RateCollection object.

        :param intervalframe: ValidationIntervalFrame
        :param rate_collection: RateCollection
        """
        self.validate_intervalframe(intervalframe)
        self.intervalframe = intervalframe
        self.rate_collection = rate_collection
        self.dataframe = self.default_dataframe

        # compute bill
        self.compute_fixed_meter_charges()
        self.compute_fixed_rate_charges()

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
        self, category, description, count, count_unit, rate, rate_unit
    ):
        """
        Adds charge to ValidationBill.

        :param category: string
        :param description: string
        :param count: float
        :param count_unit: DataUnit
        :param rate: float
        :param rate_unit: RateUnit
        """
        self.validate_units(count_unit, rate_unit)

        self.dataframe = self.dataframe.append(
            {
                "category": category,
                "description": description,
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
        if self.rate_collection.fixed_meter_charge:
            self.add_charge(
                category="fixed",
                description="Fixed Charge First Meter",
                count=1,
                count_unit=DataUnit.objects.get(name="month"),
                rate=self.rate_collection.fixed_meter_charge,
                rate_unit=RateUnit.objects.get(
                    numerator__name="$", denominator__name="month"
                ),
            )

    def compute_fixed_rate_charges(self):
        """
        Extracts fixed rates from self.rate_collection and fixed counts from
        self.intervalframe to compute fixed charges.
        """
        # TODO: Remove optional fixed charges/credits
        for rate in self.rate_collection.fixed_rates:
            if "/day" in rate.get("key"):
                rate_unit = RateUnit.objects.get(
                    numerator__name="$", denominator__name="day"
                )
            else:
                rate_unit = self.rate_collection.fixed_rate_unit
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
