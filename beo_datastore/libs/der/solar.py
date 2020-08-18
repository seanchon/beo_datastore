import attr
from cached_property import cached_property
from datetime import datetime
from functools import reduce
import pandas as pd
import requests

from beo_datastore.libs.der.builder import (
    DER,
    DERProduct,
    DERSimulationBuilder,
    DERStrategy,
)
from beo_datastore.libs.intervalframe import PowerIntervalFrame


PVWATTS_URL = "https://developer.nrel.gov/api/pvwatts/v6.json"


@attr.s(frozen=True)
class SolarPV(DER):
    """
    A SolarPV models the physical characteristics of Solar Photovoltaics. These
    fields are based on the PVWatts API docs located at:
        - https://developer.nrel.gov/docs/solar/pvwatts/v6/

    The SolarPV configuration consists of a subset of available PVWatts
    parameters. See API docs for parameter descriptions and acceptable values.
    """

    # PVWatts API parameters
    address = attr.ib(type=str)
    api_key = attr.ib(type=str)
    array_type = attr.ib(type=int)
    azimuth = attr.ib(type=float)
    tilt = attr.ib(type=float)
    losses = attr.ib(type=float, default=14.08)
    module_type = attr.ib(type=int, default=0)
    system_capacity = attr.ib(type=float, default=1)
    timeframe = attr.ib(type=str, default="hourly")
    # non-parameter: pass stored response to avoid API call
    stored_response = attr.ib(type=dict, default={})

    @array_type.validator
    def _validate_array_type(self, attribute, value):
        """
        Validate array_type is 0, 1, 2, 3, or 4.
        """
        if value not in [0, 1, 2, 3, 4]:
            raise ValueError("array_type must be 0, 1, 2, 3, or 4.")

    @azimuth.validator
    def _validate_azimuth(self, attribute, value):
        """
        Validate azimuth is between 0 and 360.
        """
        if not (0 <= value < 360):
            raise ValueError("azimuth must be between 0 and 360.")

    @losses.validator
    def _validate_losses(self, attribute, value):
        """
        Validate losses is between -5 and 99.
        """
        if not (-5 <= value < 99):
            raise ValueError("losses must be between -5 and 99.")

    @module_type.validator
    def _validate_module_type(self, attribute, value):
        """
        Validate module_type is 0, 1, or 2.
        """
        if value not in [0, 1, 2]:
            raise ValueError("array_type must be 0, 1, or 2.")

    @tilt.validator
    def _validate_tilt(self, attribute, value):
        """
        Validate tilt is between 0 and 90.
        """
        if not (0 <= value <= 90):
            raise ValueError("tilt must be between 0 and 90.")

    @system_capacity.validator
    def _validate_system_capacity(self, attribute, value):
        """
        Validate system capcity is between 0.05 and 500000.
        """
        if not (0.05 <= value <= 500000):
            raise ValueError(
                "system_capacity must be between 0.05 and 500000."
            )

    @property
    def request_params(self) -> dict:
        """
        API request params generated from attributes.
        """
        request_params = attr.asdict(self)
        request_params.pop("stored_response")

        return request_params

    @cached_property
    def pvwatts_response(self) -> dict:
        """
        PVWatts API response based on input parameters.
        """
        if self.stored_response:
            return self.stored_response
        else:
            return requests.get(PVWATTS_URL, params=self.request_params).json()

    @cached_property
    def solar_yield(self) -> float:
        """
        Return total annual yield per 1 kW of system capacity. Yield is in kwh.
        """
        solar_intervalframe = self.get_annual_solar_intervalframe(year=2000)
        annual_yield = solar_intervalframe.total_frame288.dataframe.sum().sum()

        return annual_yield / self.system_capacity

    def get_annual_solar_intervalframe(self, year: int) -> PowerIntervalFrame:
        """
        Get solar production intervals provided by PVWatts with timestamps from
        provided year.
        """
        datetime_index = pd.date_range(
            "{}-01-01".format(year), periods=8760, freq="H"
        )
        solar_readings = self.pvwatts_response["outputs"]["ac"]
        dataframe = pd.DataFrame(
            zip(datetime_index, solar_readings)
        ).set_index(0)
        # convert W to kW and reverse polarity of readings
        dataframe = dataframe / -1000
        dataframe = dataframe.rename(columns={1: "kw"})

        return PowerIntervalFrame(dataframe=dataframe)

    def get_solar_intervalframe(
        self, start: datetime, end_limit: datetime
    ) -> PowerIntervalFrame:
        """
        Get solar production intervals provided by PVWatts with timestamps from
        provided date range.
        """
        # DataFrame with all years from start.year to end_limit.year
        dataframe = reduce(
            lambda x, y: x.append(y),
            [
                self.get_annual_solar_intervalframe(year).dataframe
                for year in range(start.year, end_limit.year + 1)
            ],
            pd.DataFrame(),
        )

        return PowerIntervalFrame(dataframe=dataframe).filter_by_datetime(
            start=start, end_limit=end_limit
        )


@attr.s(frozen=True)
class SolarPVStrategy(DERStrategy):
    """
    A SolarPVStrategy models the behavioral characteristics of Solar
    Photovoltaics. These fields are based on the PVWatts model located at:
        - https://developer.nrel.gov/docs/solar/pvwatts/v6/

    The SolarPVStrategy consists of a value for serviceable_load_ratio that
    will determine the desired system size and resize a solar
    PowerIntervalFrame accordingly.
    """

    serviceable_load_ratio = attr.ib(type=float)

    @serviceable_load_ratio.validator
    def _validate_serviceable_load_ratio(self, attribute, value):
        """
        Validate serviceable_load_ratio is between 0 and 1.
        """
        if not (0 < value <= 1):
            raise ValueError("serviceable_load_ratio must be between 0 and 1.")

    @staticmethod
    def get_annual_load(intervalframe: PowerIntervalFrame) -> float:
        """
        Calculate annual load of a intervalframe.
        """
        # calculate average daily load
        total_kwh = intervalframe.total_frame288.dataframe.sum().sum()
        days = (
            intervalframe.end_limit_datetime - intervalframe.start_datetime
        ).days

        return (total_kwh / days) * 365

    def get_target_system_size_ratio(
        self, annual_load: float, solar_yield: float
    ) -> float:
        """
        Get target system size ratio used for resizing SolarPV.
        """
        target_offset = annual_load * self.serviceable_load_ratio
        system_ratio = target_offset / abs(solar_yield)

        return max(system_ratio, 0)  # ignore net exporters

    def resize_solar_intervalframe(
        self,
        intervalframe: PowerIntervalFrame,
        solar_intervalframe: PowerIntervalFrame,
    ) -> PowerIntervalFrame:
        """
        Resize solar production intervals based on a meter's total annual load,
        the percent of load to service, and the solar yield of a SolarPV.
        """
        annual_load = self.get_annual_load(intervalframe)
        solar_yield = self.get_annual_load(solar_intervalframe)
        target_system_size_ratio = self.get_target_system_size_ratio(
            annual_load=annual_load, solar_yield=solar_yield
        )

        return PowerIntervalFrame(
            dataframe=solar_intervalframe.dataframe * target_system_size_ratio
        )


@attr.s(frozen=True)
class SolarPVSimulationBuilder(DERSimulationBuilder):
    """
    Generates DERProducts a.k.a. SolarPV Simulations.
    """

    der = attr.ib(type=SolarPV)
    der_strategy = attr.ib(type=SolarPVStrategy)

    def run_simulation(self, intervalframe: PowerIntervalFrame) -> DERProduct:
        solar_intervalframe = self.der.get_solar_intervalframe(
            start=intervalframe.start_datetime,
            end_limit=intervalframe.end_limit_datetime,
        )
        solar_intervalframe = self.der_strategy.resize_solar_intervalframe(
            intervalframe=intervalframe,
            solar_intervalframe=solar_intervalframe,
        )

        return DERProduct(
            der=self.der,
            der_strategy=self.der_strategy,
            pre_der_intervalframe=intervalframe,
            der_intervalframe=solar_intervalframe,
            post_der_intervalframe=(intervalframe + solar_intervalframe),
        )
