from datetime import datetime, timedelta
import pandas as pd

from unittest import TestCase

from navigader_core.der.schedule_utils import create_diurnal_schedule
from navigader_core.load.dataframe import get_dataframe_max_difference
from navigader_core.load.intervalframe import (
    PowerIntervalFrame,
    ValidationFrame288,
)
from navigader_core.der.evse import (
    EVSE,
    EVSESimulationBuilder,
    EVSEStrategy,
)


# EVSE model specifications
ev_mpkwh = 3.3
ev_count = 15
evse_rating = 1.9
evse_count = 5
evse_utilization = 0.8
driving_distance = 20

# time durations
one_hour = timedelta(minutes=60)
quarter_hour = timedelta(minutes=15)


class TestEVSE(TestCase):
    def setUp(self):
        """
        Test EVSE operations under:

        1. The following hypothetical load conditions:
            - 2020/01/01 midnight to 2020/01/02 midnight: 0kW
        2. The following EVSE model specifications:
            - EV driving efficiency: 3.3 miles/kwh
            - EVSE rating: 150 kw
            - number of electric vehicles: 15
            - number of electric vehicle chargers: 5
        3. The following charge/drive strategy:
            - always attempt to charge from grid from 8 a.m. to 6 p.m.
            - drive 20 miles each way to/from work
        """
        self.intervalframe = PowerIntervalFrame(
            pd.DataFrame(
                zip([datetime(2020, 1, 1, x) for x in range(0, 24)], [0] * 24)
            )
            .rename(columns={0: "index", 1: "kw"})
            .set_index("index")
        )

        self.evse = EVSE(
            ev_mpkwh=ev_mpkwh,
            evse_rating=evse_rating,
            ev_count=ev_count,
            evse_count=evse_count,
            evse_utilization=evse_utilization,
        )

        # charge from 8 a.m. to 5 p.m. on solar exports only
        charge_schedule = create_diurnal_schedule(
            start_hour=8,
            end_limit_hour=17,
            power_limit_1=0.0,
            power_limit_2=float("-inf"),
        )
        drive_schedule = ValidationFrame288.convert_matrix_to_frame288(
            [
                [
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    driving_distance,  # 7 a.m.
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    driving_distance,  # 5 p.m.
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                ]
            ]
            * 12
        )
        self.evse_strategy = EVSEStrategy(charge_schedule, drive_schedule)
        self.builder = EVSESimulationBuilder(
            der=self.evse, der_strategy=self.evse_strategy
        )

    def test_get_power(self):
        """
        Calculate power operations for charging.
        """
        # no solar exports, no charge
        self.assertEqual(
            self.builder.get_target_power(
                month=1,
                hour=12,
                meter_reading=0.0,
                duration=one_hour,
                current_charge=0.0,
            ),
            0,  # no charge
        )

        # outside of charge schedule: no charge
        self.assertEqual(
            self.builder.get_target_power(
                month=1,
                hour=0,
                meter_reading=-1000.0,
                duration=one_hour,
                current_charge=0.0,
            ),
            0,
        )

        # inside of charge schedule: charge
        self.assertEqual(
            self.builder.get_target_power(
                month=1,
                hour=12,
                meter_reading=-1000.0,
                duration=one_hour,
                current_charge=0.0,
            ),
            evse_count * evse_rating,
        )

        # inside of charge schedule and mostly charged: charge, but only
        # partially
        charge = driving_distance * 2 / ev_mpkwh * evse_utilization * ev_count
        self.assertEqual(
            self.builder.get_target_power(
                month=1,
                hour=12,
                meter_reading=-1000.0,
                duration=one_hour,
                current_charge=charge - 5,
            ),
            5,
        )

        # inside of charge schedule but fully charged: no charge
        self.assertEqual(
            self.builder.get_target_power(
                month=1,
                hour=12,
                meter_reading=-1000.0,
                duration=quarter_hour,
                current_charge=charge,
            ),
            0,
        )

        # inside of charge schedule but positive meter reading: no charge
        # note this is only the case because the strategy charges off NEM only
        self.assertEqual(
            self.builder.get_target_power(
                month=1,
                hour=12,
                meter_reading=0,
                duration=one_hour,
                current_charge=0.0,
            ),
            0,
        )

        # inside of charge schedule but meter reading is insufficient to
        # fully charge: charge, but only partially. note this is only the case
        # because the strategy charges off NEM only
        self.assertEqual(
            self.builder.get_target_power(
                month=1,
                hour=12,
                meter_reading=-5,
                duration=one_hour,
                current_charge=0.0,
            ),
            5,
        )

    def test_get_drive_distance(self):
        """
        Calculate miles to drive based on an EVSEStrategy.
        """

        # outside of drive schedule
        self.assertEqual(
            self.builder.get_drive_distance(
                month=1, hour=12, duration=one_hour
            ),
            0,
        )

        # inside of drive schedule, one hour
        self.assertEqual(
            self.builder.get_drive_distance(month=1, hour=7, duration=one_hour),
            ev_count * driving_distance,
        )

        # inside of drive schedule, 15 minutes
        self.assertEqual(
            self.builder.get_drive_distance(
                month=1, hour=7, duration=quarter_hour
            ),
            ev_count * driving_distance * 0.25,
        )

    def test_get_ev_kw(self):
        """
        Calculate power operations for driving.
        """

        # no charge
        distance = 50
        self.assertEqual(
            self.builder.get_ev_kw(
                distance=distance, duration=one_hour, charge=0
            ),
            0,
        )

        # no battery constraint
        self.assertEqual(
            self.builder.get_ev_kw(
                distance=distance, duration=quarter_hour, charge=25
            ),
            -distance / (ev_mpkwh * 0.25),
        )

        # battery constraint
        insufficient_charge = 10
        self.assertEqual(
            self.builder.get_ev_kw(
                distance=distance,
                duration=quarter_hour,
                charge=insufficient_charge,
            ),
            -insufficient_charge / 0.25,  # 10 kwh / (.25 hours)
        )

    def test_get_charge(self):
        """
        Calculate next charge for battery charge and EV drive operations.
        """

        # test battery charge
        kw_in = 100
        self.assertEqual(
            self.builder.get_charge(
                kw=kw_in, ev_kw=0, duration=quarter_hour, charge=0
            ),
            kw_in * 0.25,
        )

        # test EV drive
        current_charge = 50
        kw_out = -100
        self.assertEqual(
            self.builder.get_charge(
                kw=0,
                ev_kw=kw_out,
                duration=quarter_hour,
                charge=current_charge,
            ),
            current_charge + kw_out * 0.25,  # 50 kwh + -100kw * .25 hours
        )

    def test_simulation_equality(self):
        """
        Test that the same intervalframe resampled to different periods yields
        the same simulation results.
        """
        intervalframe_60 = PowerIntervalFrame(
            pd.DataFrame(
                zip(
                    [datetime(2018, 1, 1, x) for x in range(0, 24)],
                    [-5 for _ in range(0, 24)],
                )
            )
            .rename(columns={0: "index", 1: "kw"})
            .set_index("index")
        )
        intervalframe_15 = intervalframe_60.resample_intervalframe(
            target_period=quarter_hour
        )

        simulation_60 = self.builder.run_simulation(
            intervalframe=intervalframe_60
        )
        simulation_15 = self.builder.run_simulation(
            intervalframe=intervalframe_15
        )

        # get largest difference in each dataframe
        pre_der_max_difference = get_dataframe_max_difference(
            simulation_60.pre_der_intervalframe.dataframe,
            simulation_15.pre_der_intervalframe.resample_intervalframe(
                target_period=one_hour
            ).dataframe,
        )
        post_der_max_difference = get_dataframe_max_difference(
            simulation_60.post_der_intervalframe.dataframe,
            simulation_15.post_der_intervalframe.resample_intervalframe(
                target_period=one_hour
            ).dataframe,
        )

        # account for round-off error
        self.assertLess(pre_der_max_difference, 0.1 ** 10)
        self.assertLess(post_der_max_difference, 0.1 ** 10)
