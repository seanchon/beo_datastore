import attr
from cached_property import cached_property
from datetime import datetime, timedelta
from functools import reduce
import pandas as pd
import requests

from navigader_core.load.dataframe import resample_dataframe
from navigader_core.der.builder import (
    DER,
    DERProduct,
    DERSimulationBuilder,
    DERStrategy,
)
from navigader_core.load.intervalframe import PowerIntervalFrame


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
    array_type = attr.ib(type=int)
    azimuth = attr.ib(type=float)
    tilt = attr.ib(type=float)
    api_key = attr.ib(type=str, default="")
    module_type = attr.ib(type=int, default=0)
    timeframe = attr.ib(type=str, default="hourly")
    # non-parameter: pass stored response to avoid API call
    stored_response = attr.ib(type=dict, default={}, repr=False)

    @array_type.validator
    def _validate_array_type(self, attribute, value):
        """
        Validate array_type is 0, 1, 2, 3, or 4.
        """
        if value not in [0, 1, 2, 3, 4]:
            self.raise_validation_error(attribute, "Must be 0, 1, 2, 3, or 4")

    @azimuth.validator
    def _validate_azimuth(self, attribute, value):
        """
        Validate azimuth is between 0 and 360.
        """
        if not (0 <= value < 360):
            self.raise_validation_error(attribute, "Must be between 0 and 360")

    @module_type.validator
    def _validate_module_type(self, attribute, value):
        """
        Validate module_type is 0, 1, or 2.
        """
        if value not in [0, 1, 2]:
            self.raise_validation_error(attribute, "Must be 0, 1 or 2")

    @tilt.validator
    def _validate_tilt(self, attribute, value):
        """
        Validate tilt is between 0 and 90.
        """
        if not (0 <= value <= 90):
            self.raise_validation_error(attribute, "Must be between 0 and 90")

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

        params = {"losses": 14.08, "system_capacity": 1}
        params.update(self.request_params)
        return requests.get(PVWATTS_URL, params=params, timeout=7).json()

    def get_annual_solar_intervalframe(
        self, year: int, target_period: timedelta = timedelta(hours=1)
    ) -> PowerIntervalFrame:
        """
        Get solar production intervals provided by PVWatts with timestamps from
        provided year.
        """
        datetime_index = pd.date_range(
            "{}-01-01".format(year), periods=8760, freq="H"
        )
        solar_readings = self.pvwatts_response["outputs"]["ac"]
        dataframe = pd.DataFrame(zip(datetime_index, solar_readings)).set_index(
            0
        )
        # convert W to kW and reverse polarity of readings
        dataframe = dataframe / -1000
        dataframe = dataframe.rename(columns={1: "kw"})
        # rename index
        dataframe.index.rename("index", inplace=True)

        return PowerIntervalFrame(
            dataframe=resample_dataframe(
                dataframe=dataframe, target_period=target_period
            )
        )

    def get_solar_intervalframe(
        self,
        start: datetime,
        end_limit: datetime,
        target_period: timedelta = timedelta(hours=1),
    ) -> PowerIntervalFrame:
        """
        Get solar production intervals provided by PVWatts with timestamps from
        provided date range.
        """
        # DataFrame with all years from start.year to end_limit.year
        dataframe = reduce(
            lambda x, y: x.append(y),
            [
                self.get_annual_solar_intervalframe(
                    year=year, target_period=target_period
                ).dataframe
                for year in range(start.year, end_limit.year + 1)
            ],
            pd.DataFrame(),
        )

        return PowerIntervalFrame(dataframe=dataframe).filter_by_datetime(
            start=start, end_limit=end_limit
        )

    def get_system_capacity(self, intervalframe: PowerIntervalFrame) -> float:
        """
        Get system capacity by dividing intervalframe yield by solar
        intervalframe yield over same timeframe.
        """
        solar_yield = (
            self.get_solar_intervalframe(
                start=intervalframe.start_datetime,
                end_limit=intervalframe.end_limit_datetime,
            )
            .total_frame288.dataframe.sum()
            .sum()
        )

        return intervalframe.total / solar_yield


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
        if value <= 0:
            self.raise_validation_error(attribute, "Must be greater than zero")

    @staticmethod
    def get_annual_load(intervalframe: PowerIntervalFrame) -> float:
        """
        Calculate annual load of a intervalframe.
        """
        # calculate average daily load
        days = (
            intervalframe.end_limit_datetime - intervalframe.start_datetime
        ).days

        return (intervalframe.total / days) * 365

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
            target_period=intervalframe.period,
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
