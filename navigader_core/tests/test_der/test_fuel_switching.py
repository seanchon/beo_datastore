from datetime import timedelta
from itertools import chain, combinations
import os
from typing import Collection

import pandas as pd

from beo_datastore.libs.tests import NavigaderTestCase
from navigader_core.load.intervalframe import (
    EnergyContainer,
    GasIntervalFrame,
    PowerIntervalFrame,
)
from navigader_core.load.openei import TMY3Parser
from navigader_core.der.fuel_switching import (
    FuelSwitching,
    FuelSwitchingSimulationBuilder,
    FuelSwitchingStrategy,
)


KWH_PER_THERM = 29.3
HEAT_PUMP_COP = 3
THERM_KWH_MULTIPLIER = KWH_PER_THERM / HEAT_PUMP_COP


class TestFuelSwitching(NavigaderTestCase):
    """
    Tests fuel switching simulation core logic
    """

    def setUp(self):
        """
        Runs a fuel switching simulation
        """
        gas_file = self.get_file_path("sample_gas_usage.parquet")
        self.gas_frame = GasIntervalFrame(pd.read_parquet(gas_file))
        self.der = FuelSwitching(space_heating=True, water_heating=True)
        self.builder = FuelSwitchingSimulationBuilder(
            der=self.der,
            der_strategy=FuelSwitchingStrategy(
                tmy3_file=TMY3Parser(
                    self.get_file_path("tmy3_sample_residential.csv")
                ),
            ),
        )

    @staticmethod
    def get_file_path(file_path: str) -> str:
        """
        Returns the file path for a file within the `./files` directory
        """
        return os.path.join(os.path.dirname(__file__), "files", file_path)

    @staticmethod
    def create_strategy_from_arrays(
        space_heating_values: Collection[float],
        water_heating_values: Collection[float],
    ):
        return FuelSwitchingStrategy(
            TMY3Parser.create_tmy3_from_arrays(
                space_heating_values, water_heating_values
            )
        )

    def test_convert_therms_to_kwh(self):
        """
        Tests the FuelSwitching class's convert_therms_to_kwh method
        """
        args = dict(space_heating=True, water_heating=True)
        index = pd.date_range(start="2020-03-17 15:00", freq="H", periods=24)
        gas_frame = GasIntervalFrame(
            pd.DataFrame(range(24), index=index, columns=["therms"])
        )

        der = FuelSwitching(**args)
        kwh = der.convert_therms_to_kwh(gas_frame).kwh
        self.assertAlmostEqual(
            kwh, [n * THERM_KWH_MULTIPLIER for n in range(24)]
        )

    def test_get_customer_kwh_by_gas_type(self):
        """
        Tests splitting customer kWh values by gas type
        """
        num_days = 5
        num_hours = 24 * num_days
        start = "2020-01-01 00:00"
        space_heating_values = range(num_hours)
        water_heating_values = range(num_hours, 0, -1)

        hourly_df = pd.DataFrame(
            list(zip(space_heating_values, water_heating_values)),
            columns=("space_heating", "water_heating"),
            index=pd.date_range(start=start, periods=num_hours, freq="H"),
        )

        daily_df = pd.DataFrame(
            list(zip(range(num_days), range(num_days, 0, -1))),
            columns=("space_heating", "water_heating"),
            index=pd.date_range(start=start, periods=num_days, freq="D"),
        )

        customer_gas_by_type = self.builder.get_customer_kwh_by_gas_type(
            tmy3_normalized=hourly_df, customer_kwh_daily=daily_df
        )

        for n in range(1, num_days + 1):
            # assert on space heating values
            self.assertEqual(
                customer_gas_by_type[
                    customer_gas_by_type.index.dayofyear == n
                ].space_heating.to_list(),
                [i * (n - 1) + 24 * ((n - 1) ** 2) for i in range(24)],
            )

            # assert on water heating values
            m = num_days + 1 - n
            self.assertEqual(
                customer_gas_by_type[
                    customer_gas_by_type.index.dayofyear == n
                ].water_heating.to_list(),
                [24 * (m ** 2) - i * m for i in range(24)],
            )

    def test_convert_customer_therms_to_kwh(self):
        """
        Tests FuelSwitchingSimulationBuilder.convert_customer_therms_to_kwh
        method
        """
        # Generate gas data easy for testing assertions
        strategy = self.create_strategy_from_arrays([8] * 24, [2] * 24)
        builder = FuelSwitchingSimulationBuilder(
            der=self.der, der_strategy=strategy
        )

        # Filter gas frame to first day of data
        start_datetime = self.gas_frame.start_datetime
        gas_df = self.gas_frame.filter_by_datetime(
            start=start_datetime, end_limit=start_datetime + timedelta(days=1)
        )

        # kWh values should be gas values times THERM_KWH_MULTIPLIER
        kwh_values = builder.convert_customer_therms_to_kwh(gas_df)

        # Water heating is 80% of total gas usage
        self.assertEqual(
            kwh_values.space_heating,
            gas_df.dataframe.therms * THERM_KWH_MULTIPLIER * 0.8,
        )

        # Water heating is 20% of total gas usage
        self.assertEqual(
            kwh_values.water_heating,
            gas_df.dataframe.therms * THERM_KWH_MULTIPLIER * 0.2,
        )

    def test_normalize_tmy3(self):
        """
        Tests the TMY3 file normalization performed by the FuelSwitchingStrategy
        """
        normalized = self.create_strategy_from_arrays(
            space_heating_values=[10] * 24, water_heating_values=range(24)
        )._normalized_tmy3

        # All space heating values are the same, so all should be 1/24
        self.assertEqual(normalized.space_heating, 1 / 24)

        # Water heating values are n / sum(0, 1, ... 22, 23). Sum of an
        # algebraic sequence is n * (n + 1) / 2
        sum_23 = 23 * 24 / 2
        expected = [n / sum_23 for n in range(24)]
        self.assertEqual(normalized.water_heating, expected)

        # When no gas is used for a given category, the normalized curve should
        # equal 0
        normalized = self.create_strategy_from_arrays(
            space_heating_values=[0] * 24, water_heating_values=[1] * 24
        )._normalized_tmy3
        self.assertEqual(normalized.space_heating, 0)
        self.assertEqual(normalized.water_heating, 1 / 24)

    def test_gas_type_percentages(self):
        """
        Tests the gas percentages normalization performed by the
        FuelSwitchingStrategy
        """
        # Set water heating values to be space heating values in reverse
        space_heating_values = range(24)
        water_heating_values = space_heating_values[::-1]
        gas_type_percentages = self.create_strategy_from_arrays(
            space_heating_values, water_heating_values
        )._gas_type_percentages

        # Space heating and water heating totals are the same, even though they
        # differ hour to hour
        self.assertEqual(gas_type_percentages.space_heating, 0.5)
        self.assertEqual(gas_type_percentages.water_heating, 0.5)

        # No space heating means water heating is 100% of the gas usage
        gas_type_percentages = self.create_strategy_from_arrays(
            space_heating_values=[0] * 24, water_heating_values=[1] * 24
        )._gas_type_percentages
        self.assertEqual(gas_type_percentages.space_heating, 0)
        self.assertEqual(gas_type_percentages.water_heating, 1)

        # If there's no usage at all, both percentages should be 0
        gas_type_percentages = self.create_strategy_from_arrays(
            space_heating_values=[0] * 24, water_heating_values=[0] * 24
        )._gas_type_percentages
        self.assertEqual(gas_type_percentages.space_heating, 0)
        self.assertEqual(gas_type_percentages.water_heating, 0)

    def test_day_totals(self):
        """
        Tests the daily summation method of the FuelSwitchingStrategy
        """
        # No gas
        day_totals = self.create_strategy_from_arrays(
            space_heating_values=[0] * 24, water_heating_values=[0] * 24
        ).day_totals
        self.assertEqual(day_totals.total, 0)
        self.assertEqual(day_totals.water_heating, 0)
        self.assertEqual(day_totals.space_heating, 0)

        # Ascending values for one week
        space_heating_values = range(1, 24 * 7 + 1)
        water_heating_values = space_heating_values[::-1]
        day_totals = self.create_strategy_from_arrays(
            space_heating_values, water_heating_values
        ).day_totals

        sum_24 = 24 * 25 / 2
        expected_space_heating = [sum_24 + (24 * n) * 24 for n in range(7)]
        self.assertEqual(day_totals.space_heating, expected_space_heating)
        self.assertEqual(day_totals.water_heating, expected_space_heating[::-1])
        self.assertEqual(day_totals.total, sum_24 * 2 + (24 * 6) * 24)

    def test_combine_customer_kwh_curves(self):
        """
        Tests the FuelSwitching class's combine_customer_kwh_curves method
        """
        df = pd.DataFrame(
            zip(range(24), range(23, -1, -1)),
            index=pd.date_range(start="2020-01-01 00:00", freq="H", periods=24),
            columns=("space_heating", "water_heating"),
        )

        # Both spacing heating and water heating
        der = FuelSwitching(space_heating=True, water_heating=True)
        combined_curve = der.combine_customer_kwh_curves(df)
        self.assertEqual(combined_curve.kw, 23)

        # Only space heating
        der = FuelSwitching(space_heating=True, water_heating=False)
        combined_curve = der.combine_customer_kwh_curves(df)
        self.assertEqual(combined_curve.kw, range(24))

        # Only water heating
        der = FuelSwitching(space_heating=False, water_heating=True)
        combined_curve = der.combine_customer_kwh_curves(df)
        self.assertEqual(combined_curve.kw, range(23, -1, -1))

        # Neither space heating nor water heating should throw an exception
        der = FuelSwitching(space_heating=False, water_heating=False)
        self.assertRaises(Exception, der.combine_customer_kwh_curves, df)

    def test_run_simulation(self):
        """
        Tests the FuelSwitchingSimulationBuilder's run_simulation method
        """
        gas_index = self.gas_frame.dataframe.index
        kw_frame = PowerIntervalFrame(
            pd.DataFrame(range(len(gas_index)), index=gas_index, columns=["kw"])
        )
        energy_container = EnergyContainer(kw=kw_frame, gas=self.gas_frame)
        der_product = self.builder.run_simulation(energy_container)

        # Pre-DER intervalframe should be the kw frame
        self.assertIs(der_product.pre_der_intervalframe, kw_frame)

        # kW usage should only increase
        der_frame = der_product.der_intervalframe.dataframe
        self.assertTrue(der_frame[der_frame.kw < 0].empty)

        # Magnitude of energy usage in any given day should not have changed
        kwh_from_gas = self.gas_frame.dataframe.therms * THERM_KWH_MULTIPLIER
        for day in self.gas_frame.iter_days:
            self.assertAlmostEqual(
                kwh_from_gas.loc[day.to_timestamp()],
                der_frame[der_frame.index.dayofyear == day.dayofyear].kw.sum(),
            )


class TestTMY3Parser(NavigaderTestCase):
    """
    Tests the TMY3Parser
    """

    @property
    def column_names(self):
        """
        Convenience method to access the values of the TMY3Parser class's
        TMY3ColumnNames named tuple
        """
        field_names = TMY3Parser.column_names._fields
        return {getattr(TMY3Parser.column_names, name) for name in field_names}

    @property
    def non_date_column_names(self):
        """
        Convenience method to access the column names for the non-date fields
        """
        return self.column_names - {TMY3Parser.column_names.date}

    def test_validate_fails_when_missing_columns(self):
        """
        Tests that the TMY3Parser.validate class method fails when columns are
        missing from input dataframe
        """
        cols = self.column_names
        all_subsets = [combinations(cols, r) for r in range(len(cols))]
        for column_set in chain.from_iterable(all_subsets):
            df = pd.DataFrame([], columns=column_set)
            errors, warnings = TMY3Parser.validate(df)

            # Should be one error per column not included in the set
            excluded_columns = cols - set(column_set)
            for column in excluded_columns:
                expected_error = f'File missing expected column "{column}"'
                if "Water" in column:
                    self.assertIn(expected_error, warnings)
                else:
                    self.assertIn(expected_error, errors)

            # Error should not be present for columns in the set
            for column in column_set:
                self.assertNotIn(
                    f'File missing expected column "{column}"',
                    errors + warnings,
                )

    def test_validate_fails_when_columns_have_wrong_type(self):
        """
        Tests that the TMY3Parser.validate class method fails when kWh columns
        are included in the dataframe but have the wrong type
        """
        data = ["not a float"] * 4
        df = pd.DataFrame([data], columns=self.column_names)
        errors, warnings = TMY3Parser.validate(df)

        # Should be 3 errors-- date column has no assertion
        self.assertEqual(len(errors), 3)
        self.assertEqual(len(warnings), 0)

        for column in self.non_date_column_names:
            self.assertIn(f"File column {column} has invalid data type", errors)

    def test_validate_succeeds_when_dataframe_is_valid(self):
        """
        Tests that the TMY3Parser.validate class method returns empty list when
        dataframe meets criteria
        """
        df = TMY3Parser.create_tmy3_from_arrays([8] * 24, [2] * 24).dataframe
        self.assertEqual(TMY3Parser.validate(df), ([], []))

    def test_add_water_heating_column(self):
        """
        Tests that a column for water heating data is added and set entirely to
        0 if the column is not included in the original dataframe.
        """
        col_names = TMY3Parser.column_names
        parser = TMY3Parser(
            pd.DataFrame(
                [["01/01  01:00:00", 1, 0.75]],
                columns=[
                    col_names.date,
                    col_names.total,
                    col_names.space_heating,
                ],
            )
        )

        df_full = parser.full_dataframe
        df_gas = parser.gas_dataframe

        # All columns should be present
        for col_name in self.non_date_column_names:
            self.assertIn(col_name, df_full.columns)

        # Water heating should be all 0
        self.assertTrue(df_full[parser.water_heating_column] == 0)
        self.assertTrue(df_gas.water_heating == 0)

    def test_does_not_add_water_heating_column_if_provided(self):
        """
        Tests that a column for water heating data is not added if the column is
        already included in the original dataframe.
        """
        col_names = TMY3Parser.column_names
        parser = TMY3Parser(
            pd.DataFrame(
                [["01/01  01:00:00", 1, 0.75, 0.5]],
                columns=[
                    col_names.date,
                    col_names.total,
                    col_names.space_heating,
                    col_names.water_heating,
                ],
            )
        )

        df_full = parser.full_dataframe
        df_gas = parser.gas_dataframe

        # All columns should be present
        for col_name in self.non_date_column_names:
            self.assertIn(col_name, df_full.columns)

        # Water heating should be all 0
        self.assertTrue(df_full[parser.water_heating_column] == 0.5)
        self.assertTrue(df_gas.water_heating == 0.5)
