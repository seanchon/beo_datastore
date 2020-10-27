from datetime import timedelta
import itertools
import pandas as pd

from unittest import mock, TestCase

from navigader_core.load.intervalframe import PowerIntervalFrame
from navigader_core.der.solar import (
    SolarPV,
    SolarPVSimulationBuilder,
    SolarPVStrategy,
)
from navigader_core.tests.mock_response import mocked_pvwatts_requests_get

# SolarPV Configuration
ARRAY_TYPE = 0
AZIMUTH = 180
ADDRESS = 94518
TILT = 20

# SolarPVStrategy
SERVICEABLE_LOAD_RATIO = 0.85


class TestSolarPV(TestCase):
    @mock.patch("requests.get", side_effect=mocked_pvwatts_requests_get)
    def setUp(self, mock_get):
        """
        Test SolarPV transformation under:

        1. The following hypothetical load conditions:
            - 2020/01/01 midnight to 2021/12/31 midnight: 1kW
            - 2020/01/01 midnight to 2021/12/31 midnight: 2kW
        2. The following SolarPV model specifications:
            - array_type: 0 (Fixed - Open Rack)
            - azimuth: 180
            - address: 94518
            - tilt: 20
        3. The following SolarPVStrategy:
            - serviceable_load_ratio: 0.85
        """
        self.intervalframe_1 = PowerIntervalFrame(
            dataframe=pd.DataFrame(
                zip(
                    pd.date_range("2020-01-01", periods=8760, freq="H"),
                    itertools.repeat(1),
                )
            )
            .set_index(0)
            .rename(columns={1: "kw"})
        )
        self.intervalframe_1 = self.intervalframe_1.resample_intervalframe(
            target_period=timedelta(minutes=15)
        )
        self.intervalframe_2 = PowerIntervalFrame(
            dataframe=pd.DataFrame(
                zip(
                    pd.date_range("2020-01-01", periods=8760, freq="H"),
                    itertools.repeat(2),
                )
            )
            .set_index(0)
            .rename(columns={1: "kw"})
        )

        self.solar_pv = SolarPV(
            api_key="ABCDEFG",
            array_type=ARRAY_TYPE,
            azimuth=AZIMUTH,
            address=ADDRESS,
            tilt=TILT,
        )
        self.solar_pv_strategy = SolarPVStrategy(
            serviceable_load_ratio=SERVICEABLE_LOAD_RATIO
        )

        self.builder = SolarPVSimulationBuilder(
            der=self.solar_pv, der_strategy=self.solar_pv_strategy
        )

        self.der_product_1 = self.builder.run_simulation(
            intervalframe=self.intervalframe_1
        )
        self.der_product_2 = self.builder.run_simulation(
            intervalframe=self.intervalframe_2
        )

    def test_solar_offset(self):
        """
        Total energy generated should be 85% of total load. Total energy
        remaining should be 15% of total load.
        """
        total_production = self.der_product_1.der_intervalframe.total
        total_pre_der_load = self.der_product_1.pre_der_intervalframe.total
        total_post_der_load = self.der_product_1.post_der_intervalframe.total

        offset_ratio = abs(total_production) / total_pre_der_load
        self.assertAlmostEqual(offset_ratio, 0.85)

        remaining_ratio = total_post_der_load / total_pre_der_load
        self.assertAlmostEqual(remaining_ratio, 0.15)

    def test_relative_solar_sizing(self):
        """
        Total energy generation should double for a meter with twice the usage.
        """
        production_1 = self.der_product_1.der_intervalframe.total
        production_2 = self.der_product_2.der_intervalframe.total

        production_ratio = production_2 / production_1
        self.assertAlmostEqual(production_ratio, 2)

    def test_solar_interval_period(self):
        """
        Solar interval period should match input period.
        """
        self.assertEqual(
            self.intervalframe_1.period,
            self.der_product_1.post_der_intervalframe.period,
        )
        self.assertEqual(
            self.intervalframe_2.period,
            self.der_product_2.post_der_intervalframe.period,
        )
