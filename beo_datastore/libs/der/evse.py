import attr
from collections import OrderedDict
from datetime import datetime, timedelta
from math import floor
import pandas as pd

from beo_datastore.libs.der.builder import (
    DataFrameQueue,
    DER,
    DERSimulationSequenceBuilder,
    DERStrategy,
)
from beo_datastore.libs.load.intervalframe import (
    PowerIntervalFrame,
    ValidationFrame288,
)
from beo_datastore.libs.utils import timedelta_to_hours


@attr.s(frozen=True)
class EVSE(DER):
    """
    An EVSE models the physical characteristic of electric vehicle service
    equipment (EVSE) a.k.a chargers in combination with electric vehicles (EVs).

    Properties of the electric vehicle (EV) are:
        - EV efficiency (miles/kwh)
        - EV gas efficiency equivalent (miles/gallon)
        - EV battery capacity (kwh)
        - EV battery efficiency (%)

    Properties of the electric vehicle service equipment (EVSE) are:
        - EVSE rating (kw)

    Additional assumptions around this model are:
        - number of electric vehicles
        - number of electric vehicle chargers
    """

    ev_mpkwh = attr.ib(type=float)
    ev_mpg_eq = attr.ib(type=float)
    ev_capacity = attr.ib(type=float)
    ev_efficiency = attr.ib(type=float)
    evse_rating = attr.ib(type=float)
    ev_count = attr.ib(type=int)
    evse_count = attr.ib(type=int)

    @ev_mpkwh.validator
    def _validate_ev_mpkwh(self, attribute, value):
        if value <= 0:
            raise ValueError("ev_mpkwh must be greater than zero.")

    @ev_mpg_eq.validator
    def _validate_ev_mpg_eq(self, attribute, value):
        if value <= 0:
            raise ValueError("ev_mpg_eq must be greater than zero.")

    @ev_capacity.validator
    def _validate_ev_capacity(self, attribute, value):
        if value <= 0:
            raise ValueError("ev_capacity must be greater than zero.")

    @ev_efficiency.validator
    def _validate_ev_efficiency(self, attribute, value):
        if not (0 < value <= 1):
            raise ValueError("ev_efficiency must be between 0 and 1.")

    @evse_rating.validator
    def _validate_evse_rating(self, attribute, value):
        if value <= 0:
            raise ValueError("evse_rating must be greater than zero.")

    @ev_count.validator
    def _validate_ev_count(self, attribute, value):
        if value <= 0:
            raise ValueError("ev_count must be greater than zero.")

    @evse_count.validator
    def _validate_evse_count(self, attribute, value):
        if value <= 0:
            raise ValueError("evse_count must be greater than zero.")

    @property
    def ev_total_capacity(self) -> float:
        """
        Total battery capacity of all EVs combined.
        """
        return self.ev_capacity * self.ev_count

    @property
    def ev_range(self) -> float:
        """
        Range in miles of a single EV under current assumptions.
        """
        return self.ev_capacity * self.ev_mpkwh

    @property
    def evse_total_rating(self) -> float:
        """
        Total rating of all EVSEs combined.
        """
        return self.evse_rating * self.evse_count

    def get_target_power(
        self, duration: timedelta, current_charge: float
    ) -> float:
        """
        Return the upper limit for a battery operation based on its physical
        properties.

        The following battery constraints apply:
            - The battery cannot charge at power beyond it rating.
            - The battery cannot charge beyond its max capacity.
            - The battery losses due to the efficiency factor are calculated on
            the charge cycle.

        :param duration: timedelta
        :param current_charge: current charge level (kwh)
        :return: target power (kw)
        """
        hours = timedelta_to_hours(duration)
        power = (self.ev_total_capacity - current_charge) / (
            hours * self.ev_efficiency
        )
        return min(power, self.evse_total_rating)


@attr.s(frozen=True)
class EVSEStrategy(DERStrategy):
    """
    A combination of a charge_schedule and a drive_schedule where the
    charge_schedule specifies month-hour thresholds at which to allow charging
    and the drive_schedule specifies month-hour miles to drive.
    """

    charge_schedule = attr.ib(type=ValidationFrame288)
    drive_schedule = attr.ib(type=ValidationFrame288)

    @charge_schedule.validator
    def _validate_charge_schedule(self, attribute, value):
        """
        Validate that charging and driving do not happen simultaneously.
        """
        # any value other than float("-inf") allows charging battery
        charge_month_hour = value.dataframe != float("-inf")

        # any value other than 0 allows driving
        drive_month_hour = self.drive_schedule.dataframe != 0

        charge_and_drive = pd.DataFrame.any(
            pd.DataFrame.any(charge_month_hour & drive_month_hour)
        )
        if charge_and_drive:
            raise AttributeError(
                "EVSEStrategy cannot contain instruction to drive and charge"
                "within the same month-hour."
            )

    def get_target_power(
        self, month: int, hour: int, meter_reading: float
    ) -> float:
        """
        Return the upper limit for a battery operation based on this strategy.

        - When meter_reading is below the charge threshold, the battery will
        attempt to charge up to the charge threshold.

        Note: Power level is rounded to nearest kw to increase hits in
        generate_battery_operations()'s lru_cache.
        """
        charge_threshold = self.charge_schedule.dataframe[month][hour]

        # get upper limit from strategy
        if meter_reading < charge_threshold:  # charge battery
            try:
                power_level = floor(charge_threshold - meter_reading)
            except OverflowError:
                power_level = charge_threshold - meter_reading
        else:  # no operation
            power_level = 0

        return power_level


class MixedFuelIntervalFrame(PowerIntervalFrame):
    """
    Container for kW and gallon_per_hour readings.
    """

    default_aggregation_column = "kw"
    default_dataframe = pd.DataFrame(
        columns=["kw", "gallon_per_hour"], index=pd.to_datetime([])
    )

    @classmethod
    def create_pre_der_intervalframe(
        cls,
        power_intervalframe: PowerIntervalFrame,
        evse: EVSE,
        evse_strategy: EVSEStrategy,
    ):
        """
        Create a pre_der_intervalframe (MixedFuelIntervalFrame) from a
        PowerIntervalFrame and EVSEStrategy.
        """
        drive_schedule_dataframe = evse_strategy.drive_schedule.compute_intervalframe(
            start=power_intervalframe.start_datetime,
            end_limit=power_intervalframe.end_limit_datetime,
            period=power_intervalframe.period,
        ).rename(
            columns={"value": "distance"}
        )

        total_combined_distance = (
            drive_schedule_dataframe["distance"] * evse.ev_count
        )
        drive_schedule_dataframe["gallon_per_hour"] = (
            total_combined_distance / evse.ev_mpg_eq
        )

        return cls(
            dataframe=pd.merge(
                power_intervalframe.dataframe,
                drive_schedule_dataframe[["gallon_per_hour"]],
                how="inner",
                left_index=True,
                right_index=True,
            )
        )


class EVSEIntervalFrame(DataFrameQueue):
    """
    Container for generating and storing EVSE operation intervals.

    Columns include:
        - distance: Miles are driven by all cars.
        - kw: Electric power used to charge cars.
        - ev_kw: Electricity used to drive.
        - gallon_per_hour: Gas used to drive (NOTE: This is only to keep track
            of cases where a car does not have enough battery to drive the
            specified distance.).
        - charge: Charge on all EV batteries.
        - capacity: Capacity of all EV batteries.
    """

    default_dataframe = pd.DataFrame(
        columns=[
            "distance",
            "kw",
            "ev_kw",
            "gallon_per_hour",
            "charge",
            "capacity",
        ],
        index=pd.to_datetime([]),
    )
    default_aggregation_column = "kw"


@attr.s(frozen=True)
class EVSESimulationBuilder(DERSimulationSequenceBuilder):
    """
    Generates DERProducts a.k.a. EVSE Simulations.
    """

    der = attr.ib(type=EVSE)
    der_strategy = attr.ib(type=EVSEStrategy)
    begin_charged = attr.ib(type=bool, default=False)

    def get_der_intervalframe(self) -> DataFrameQueue:
        return EVSEIntervalFrame()

    def get_latest_charge(self, der_intervalframe: DataFrameQueue):
        """
        Returns the current state of the EV batteries' charge. On the first
        interval, the charge is either 0 (if the EV's are not charged
        initially) or the full capacity of the EV's, as given in the DER
        configuration
        """
        latest_evse_interval = der_intervalframe.latest_interval_dict
        if latest_evse_interval:
            return latest_evse_interval["charge"]
        else:
            if self.begin_charged:
                return self.der.ev_total_capacity
            else:
                return 0

    def get_target_power(
        self,
        month: int,
        hour: int,
        meter_reading: float,
        duration: timedelta,
        current_charge: float,
    ) -> float:
        """
        Return the power level for next battery operation. This is the minimum
        of the power needed to charge the battery fully (der.get_target_power)
        and the max power the strategy allows (der_strategy.get_target_power).
        """
        power = min(
            self.der.get_target_power(
                duration=duration, current_charge=current_charge
            ),
            self.der_strategy.get_target_power(
                month=month, hour=hour, meter_reading=meter_reading
            ),
        )

        if power < 0 or power > self.der.evse_total_rating:
            raise RuntimeError(
                "Power to charge battery must be between 0 and EVSE total "
                "rating ({}kw) of all EVSEs combined.".format(
                    self.der.evse_total_rating
                )
            )
        else:
            return power

    def get_drive_distance(
        self, month: int, hour: int, duration: timedelta
    ) -> float:
        """
        Return total miles driven at a given month-hour over a duration based
        on the strategy's drive miles and number of EVs.
        """
        distance = (
            self.der_strategy.drive_schedule.dataframe[month][hour]
            * self.der.ev_count
            * (duration / timedelta(hours=1))
        )

        if distance < 0:
            raise RuntimeError("Distance to drive cannot be negative.")
        else:
            return distance

    def get_ev_kw(
        self, distance: float, duration: timedelta, charge: float
    ) -> float:
        """
        Return kw output over a duration to travel distance. This is
        constrained by the available charge.
        """
        duration_hours = timedelta_to_hours(duration)

        # kw available from battery
        battery_max_kw = charge / duration_hours
        # kw need to travel full distance in duration
        total_kw = distance / (self.der.ev_mpkwh * duration_hours)

        ev_kw = max(-battery_max_kw, -total_kw)

        if ev_kw > 0:
            raise RuntimeError("Power to drive EV cannot be positive.")
        else:
            return ev_kw

    def get_gallon_per_hour(
        self, distance: float, ev_kw: float, duration: timedelta
    ) -> float:
        """
        Return gallon_per_hour needed to drive any remaining miles. This is
        meant to account for costs not offset by EV usage.
        """
        duration_hours = timedelta_to_hours(duration)

        ev_distance = (-ev_kw * duration_hours) * self.der.ev_mpkwh
        remaining_distance = distance - ev_distance

        gallon_per_hour = remaining_distance / (
            self.der.ev_mpg_eq * duration_hours
        )

        if gallon_per_hour < 0:
            raise RuntimeError("Gallon per hour cannot be negative.")
        else:
            return gallon_per_hour

    def get_charge(
        self, kw: float, ev_kw: float, duration: timedelta, charge: float
    ) -> float:
        """
        Return next charge after a battery or EV operation.
        """
        duration_hours = timedelta_to_hours(duration)

        # charge battery with efficiency losses
        charge += kw * duration_hours * self.der.ev_efficiency
        # drive EV
        charge += ev_kw * duration_hours

        if charge < 0 or charge > self.der.ev_total_capacity:
            raise RuntimeError("Charge cannot be negative or exceed capacity.")
        else:
            return charge

    def get_noop(
        self, interval_start: datetime, der_intervalframe: DataFrameQueue
    ) -> OrderedDict:
        return OrderedDict(
            {
                "start": interval_start,
                "distance": 0,
                "kw": 0,
                "ev_kw": 0,
                "gallon_per_hour": 0,
                "charge": self.get_latest_charge(der_intervalframe),
                "capacity": self.der.ev_total_capacity,
            }
        )

    def get_pre_der_intervalframe(
        self, intervalframe: PowerIntervalFrame
    ) -> MixedFuelIntervalFrame:
        return MixedFuelIntervalFrame.create_pre_der_intervalframe(
            power_intervalframe=intervalframe,
            evse=self.der,
            evse_strategy=self.der_strategy,
        )

    def get_post_der_intervalframe(
        self,
        pre_der_intervalframe: PowerIntervalFrame,
        der_intervalframe: PowerIntervalFrame,
    ) -> PowerIntervalFrame:
        post_der_if = super().get_post_der_intervalframe(
            pre_der_intervalframe, der_intervalframe
        )
        post_der_if.dataframe["gallon_per_hour"] = der_intervalframe.dataframe[
            "gallon_per_hour"
        ]
        return post_der_if

    def operate_der(
        self,
        der_intervalframe: DataFrameQueue,
        interval_start: datetime,
        interval_load: float,
        duration: timedelta,
    ) -> OrderedDict:
        """
        Generate EVSE operation based off of EVSE state. EVSE state is an
        OrderedDict representation of the last operation.

        The return value is an OrderedDict with values for the following, which
        represent values in an EVSEIntervalFrame:
            - start
            - distance
            - kw
            - ev_kw
            - gallon_per_hour
            - charge
            - capacity
        """
        if duration <= timedelta(0):
            raise ValueError("duration must be greater than timedelta(0).")

        month = interval_start.month
        hour = interval_start.hour
        latest_charge = self.get_latest_charge(der_intervalframe)

        # miles driven by all EVs during duration
        distance = self.get_drive_distance(
            month=month, hour=hour, duration=duration
        )
        # power level to charge EV batteries
        kw = self.get_target_power(
            month=month,
            hour=hour,
            meter_reading=interval_load,
            duration=duration,
            current_charge=latest_charge,
        )
        # kw to drive EV
        ev_kw = self.get_ev_kw(
            distance=distance, duration=duration, charge=latest_charge
        )
        # gallon_per_hour of gas to drive EV (if not enough charge)
        gallon_per_hour = self.get_gallon_per_hour(
            distance=distance, ev_kw=ev_kw, duration=duration
        )
        # remaining charge after operation
        charge = self.get_charge(
            kw=kw, ev_kw=ev_kw, duration=duration, charge=latest_charge
        )

        return OrderedDict(
            {
                "start": interval_start,
                "distance": distance,
                "kw": kw,
                "ev_kw": ev_kw,
                "gallon_per_hour": gallon_per_hour,
                "charge": charge,
                "capacity": self.der.ev_total_capacity,
            }
        )
