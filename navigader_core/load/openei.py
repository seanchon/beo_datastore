from cached_property import cached_property
from collections import namedtuple
import numpy as np
import pandas as pd
import re
from typing import Collection, Union


tmy3_col_names = ["date", "total", "space_heating", "water_heating"]
TMY3ColumnNames = namedtuple("TMY3ColumnNames", tmy3_col_names)


class TMY3Parser:
    """
    Parses a TMY3 (Typical Meteorological Year 3) file from OpenEI
    """

    date_regex = re.compile(r"(\d{2}/\d{2})\s*(\d{2}):00:00")
    column_names = TMY3ColumnNames(
        date="Date/Time",
        total="Gas:Facility [kW](Hourly)",
        space_heating="Heating:Gas [kW](Hourly)",
        water_heating="Water Heater:WaterSystems:Gas [kW](Hourly)",
    )

    def __init__(self, dataframe: Union[pd.DataFrame, str]):
        if type(dataframe) == str:
            self.dataframe = pd.read_csv(dataframe)
        else:
            self.dataframe = dataframe

    @classmethod
    def create_tmy3_from_arrays(
        cls,
        space_heating_values: Collection[float],
        water_heating_values: Collection[float],
        start: str = "2020-01-01",
    ) -> "TMY3Parser":
        """
        Instantiates a TMY3Parser with a fabricated dataframe using data
        provided by the arguments. This is a useful test method but has little
        practical use in production.

        :param space_heating_values: array of values to use for space-heating
        :param water_heating_values: array of values to use for water-heating
        :param start: starting date
        """
        num_data = min(len(space_heating_values), len(water_heating_values))

        # Hours need to be shifted from 0-23 format to 1-24 format
        def move_forward_hour(datetime_str):
            out = cls.date_regex.search(datetime_str)
            hour = int(out.group(2))
            return f"{out.group(1)} {str(hour + 1).zfill(2)}:00:00"

        # Create the hourly DatetimeIndex by first creating a PeriodIndex with
        # a frequency of one hour, extending for the number of hours that are
        # data. The start date's year is arbitrary and will be overwritten
        dates = (
            pd.period_range(start=start, freq="H", periods=num_data)
            .to_timestamp()
            .strftime("%m/%d %H:%M:%S")
            .map(move_forward_hour)
        )

        array_pairs = zip(space_heating_values, water_heating_values)
        totals = [sum(gas) for gas in array_pairs]

        data = zip(dates, totals, space_heating_values, water_heating_values)
        col_names = cls.column_names
        df = pd.DataFrame(
            list(data),
            columns=[
                col_names.date,
                col_names.total,
                col_names.space_heating,
                col_names.water_heating,
            ],
        )

        return TMY3Parser(df)

    @classmethod
    def validate(cls, dataframe: pd.DataFrame):
        """
        Validates that the dataframe argument is a valid Hourly Load Profile. A
        dataframe is considered valid if it has all of the required columns and
        they have the proper dtype
        """
        parser = TMY3Parser(dataframe)
        errors = []
        warnings = []

        for field_name in cls.column_names._fields:
            column_name = getattr(cls.column_names, field_name)

            try:
                column_name = parser.get_unique_column_with_text(column_name)
            except LookupError:
                error_text = f'File missing expected column "{column_name}"'

                # Many of the hourly load profiles are missing gas water heating
                # data. Missing water heating data will be auto-filled with 0s
                if field_name == "water_heating":
                    warnings.append(error_text)
                else:
                    errors.append(error_text)

                # Don't perform further checks if the column is missing
                continue

            # All columns except the date column should be numeric
            if field_name != "date":
                if not issubclass(dataframe[column_name].dtype.type, np.number):
                    errors.append(
                        f"File column {column_name} has invalid data type"
                    )

        return errors, warnings

    def get_unique_column_with_text(self, text: str) -> str:
        """
        Returns a unique column from the OpenEI dataframe that contains the
        given text. If there isn't exactly 1 column with the given text,
        throws a LookupError.
        """
        columns = [col for col in self.dataframe.columns if text in col]
        if len(columns) != 1:
            raise LookupError("Unique column not found.")
        else:
            return columns[0]

    @cached_property
    def date_column(self):
        return self.get_unique_column_with_text(self.column_names.date)

    @cached_property
    def total_gas_column(self):
        return self.get_unique_column_with_text(self.column_names.total)

    @cached_property
    def space_heating_column(self):
        return self.get_unique_column_with_text(self.column_names.space_heating)

    @cached_property
    def water_heating_column(self):
        """
        This column is different in that its absence is considered a warning,
        not an error. If the column isn't found in the provided dataframe, we
        instead provide the default column name
        """
        col_name = self.column_names.water_heating
        try:
            return self.get_unique_column_with_text(col_name)
        except LookupError:
            return col_name

    @cached_property
    def full_dataframe(self) -> pd.DataFrame:
        """
        Converts the date column into a dataframe index. OpenEI provides date
        values in a yearless format with hours between 1-24. Ex:

            01/01  01:00:00
            05/21  10:00:00
            07/23  21:00:00
            12/31  24:00:00

        The values are hour-end values. Most of NavigaDER deals with hour-start
        values, so the function moves all timestamps back an hour.
        """
        data = self.dataframe.copy()

        # Add a water heating column if the data isn't already present
        if self.water_heating_column not in data.columns:
            data[self.water_heating_column] = 0

        def move_back_hour(datetime_str):
            out = self.date_regex.search(datetime_str)
            hour = int(out.group(2))
            return f"{out.group(1)} {hour - 1}:00:00"

        data[self.date_column] = pd.to_datetime(
            data[self.date_column].str.strip().map(move_back_hour),
            format="%m/%d %H:%M:%S",
        )
        data.set_index(self.date_column, inplace=True)
        data.index.rename("index", inplace=True)
        return data

    @cached_property
    def gas_dataframe(self) -> pd.DataFrame:
        """
        Filters the dataframe down to gas-specific columns, renaming them for
        convenience.
        """
        return self.full_dataframe.filter(
            items=[
                self.total_gas_column,
                self.space_heating_column,
                self.water_heating_column,
            ]
        ).rename(
            columns={
                self.total_gas_column: "total",
                self.space_heating_column: "space_heating",
                self.water_heating_column: "water_heating",
            }
        )
