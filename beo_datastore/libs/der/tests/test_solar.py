import itertools
import json
from os.path import abspath, dirname, join
import pandas as pd

from unittest import mock, TestCase

from beo_datastore.libs.intervalframe import PowerIntervalFrame
from beo_datastore.libs.der.solar import (
    PVWATTS_URL,
    SolarPV,
    SolarPVSimulationBuilder,
    SolarPVStrategy,
)

PVWATTS_FILE = join(dirname(abspath(__file__)), "files", "pvwatts.json")

# SolarPV Configuration
ARRAY_TYPE = 0
AZIMUTH = 180
ADDRESS = 94518
TILT = 20

# SolarPVStrategy
SERVICEABLE_LOAD_RATIO = 0.85


def mocked_requests_get(*args, **kwargs):
    """
    Return contents of PVWATTS_FILE on requests to PVWATTS_URL.
    """

    class MockResponse:
        def __init__(self, file_path, status_code):
            with open(file_path) as f:
                self.json_data = json.load(f)
            self.status_code = status_code

        def json(self):
            return self.json_data

    if args[0] == PVWATTS_URL:
        return MockResponse(PVWATTS_FILE, 200)

    return MockResponse(None, 404)


class TestSolarPV(TestCase):
    @mock.patch("requests.get", side_effect=mocked_requests_get)
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
        total_production = (
            self.der_product_1.der_intervalframe.total_frame288.dataframe.sum().sum()
        )
        total_pre_der_load = (
            self.der_product_1.pre_der_intervalframe.total_frame288.dataframe.sum().sum()
        )
        total_post_der_load = (
            self.der_product_1.post_der_intervalframe.total_frame288.dataframe.sum().sum()
        )

        offset_ratio = abs(total_production) / total_pre_der_load
        self.assertAlmostEqual(offset_ratio, 0.85)

        remaining_ratio = total_post_der_load / total_pre_der_load
        self.assertAlmostEqual(remaining_ratio, 0.15)

    def test_relative_solar_sizing(self):
        """
        Total energy generation should double for a meter with twice the usage.
        """
        production_1 = (
            self.der_product_1.der_intervalframe.total_frame288.dataframe.sum().sum()
        )
        production_2 = (
            self.der_product_2.der_intervalframe.total_frame288.dataframe.sum().sum()
        )

        production_ratio = production_2 / production_1
        self.assertAlmostEqual(production_ratio, 2)
