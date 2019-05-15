from datetime import datetime
import multiprocessing

from django.test import TestCase

from beo_datastore.libs.bill import BillingCollection, ValidationBill
from beo_datastore.libs.fixtures import (
    flush_intervalframe_files,
    load_intervalframe_files,
)

from cost.utility_rate.models import RatePlan
from load.customer.models import Meter


RESIDENTIAL_COUNT_ERROR_RATE = 0.005  # allow 0.5% count error rate
RESIDENTIAL_CHARGE_ERROR_RATE = 0.02  # allow 2% charge error rate
COMMERCIAL_COUNT_ERROR_RATE = 0.06  # allow 6% count error rate
COMMERCIAL_CHARGE_ERROR_RATE = 0.02  # allow 2% charge error rate


def get_error_rate(expected, actual):
    """
    Return difference of expected and actual as an error rate.
    """
    return abs(expected - actual) / expected


class TestBill(TestCase):
    """
    Validation methods to test bills.
    """

    def validate_energy_count(
        self, intervalframe, expected_count, allowable_error_rate
    ):
        """
        Compares intervalframe kwh count to expected_count with
        allowable_error_rate.

        :param intervalframe: ValidationIntervalFrame
        :param expected_count: float
        :param allowable_error_rate: float
        """
        kwh_count = intervalframe.total_frame288.dataframe.sum().sum()
        self.assertLessEqual(
            get_error_rate(expected_count, kwh_count),
            allowable_error_rate,
            "{} - {}: expected {}, actual {}".format(
                intervalframe.start_datetime,
                intervalframe.end_datetime,
                expected_count,
                kwh_count,
            ),
        )

    def validate_tou_energy_count(
        self,
        intervalframe,
        openei_rate_data,
        tou_key,
        expected_count,
        allowable_error_rate,
    ):
        """
        Compares intervalframe kwh count per tou_key to expected_count with
        allowable_error_rate.

        :param intervalframe: ValidationIntervalFrame
        :param openei_rate_data: OpenEIRateData
        :param tou_key: integer
        :param expected_count: float
        """
        kwh_count = ValidationBill.get_energy_count(
            intervalframe,
            openei_rate_data.energy_weekday_schedule,
            openei_rate_data.energy_weekend_schedule,
            tou_key,
        )
        self.assertLessEqual(
            get_error_rate(expected_count, kwh_count),
            allowable_error_rate,
            "{} - {}: expected {}, actual {}".format(
                intervalframe.start_datetime,
                intervalframe.end_datetime,
                expected_count,
                kwh_count,
            ),
        )

    def validate_charge(
        self,
        intervalframe,
        rate_date_list,
        expected_total,
        allowable_error_rate,
    ):
        """
        Generates bill based on openei_rate_data and intervalframe and compares
        to expected_total with allowable_error_rate.

        :param intervalframe: ValidationIntervalFrame
        :param rate_data_list: list of OpenEIRateData objects
        :param expected_total: float
        :param allowable_error_rate: float
        """
        bill = BillingCollection(
            [
                ValidationBill(intervalframe, openei_rate_data)
                for openei_rate_data in rate_date_list
            ]
        )
        self.assertLessEqual(
            get_error_rate(expected_total, bill.total),
            allowable_error_rate,
            "{} - {}: expected {}, actual {}".format(
                intervalframe.start_datetime,
                intervalframe.end_datetime,
                expected_total,
                bill.total,
            ),
        )


class TestMCEResidentialBill(TestBill):
    """
    PG&E SA ID 8943913372

    MCE EV Customer.
    """

    fixtures = ["reference_model", "customer", "utility_rate"]

    def setUp(self):
        """
        Based on MCE Residential Rates for Electric Vehicle Owners for the 2018
        calendar year.
        """
        load_intervalframe_files()
        self.meter = Meter.objects.get(sa_id=8943913372)
        self.intervalframe = self.meter.intervalframe

    def tearDown(self):
        flush_intervalframe_files()

    def test_kwh_counts(self):
        jobs = []
        for start, end_limit, expected_count in [
            (datetime(2018, 1, 2), datetime(2018, 1, 31), 806.241),
            (datetime(2018, 1, 31), datetime(2018, 3, 2), 550.326),
            (datetime(2018, 3, 2), datetime(2018, 4, 2), 616.827),
            (datetime(2018, 4, 2), datetime(2018, 5, 2), 471.937),
            (datetime(2018, 5, 2), datetime(2018, 6, 1), 491.285),
            (datetime(2018, 6, 1), datetime(2018, 7, 2), 558.583),
            (datetime(2018, 7, 2), datetime(2018, 8, 1), 483.802),
            (datetime(2018, 8, 1), datetime(2018, 8, 31), 471.9335),
            (datetime(2018, 8, 31), datetime(2018, 10, 2), 437.494),
            (datetime(2018, 10, 2), datetime(2018, 11, 1), 434.509),
            (datetime(2018, 11, 1), datetime(2018, 12, 3), 565.667),
        ]:
            # run bill validation in parallel
            p = multiprocessing.Process(
                target=self.validate_energy_count,
                args=(
                    self.intervalframe.filter_by_datetime(start, end_limit),
                    expected_count,
                    RESIDENTIAL_COUNT_ERROR_RATE,
                ),
            )
            jobs.append(p)
            p.start()

        for job in jobs:
            job.join()

    def test_pcia_and_franchise_fee_charges(self):
        # TODO: Investigate billing errors
        jobs = []
        for start, end_limit, expected_charge, unknown_error in [
            (datetime(2018, 1, 2), datetime(2018, 1, 31), 24.18, 0),
            (datetime(2018, 1, 31), datetime(2018, 3, 2), 16.63, 0),
            (datetime(2018, 3, 2), datetime(2018, 4, 2), 21.03, -2.54),
            (datetime(2018, 4, 2), datetime(2018, 5, 2), 16.09, -1.94),
            (datetime(2018, 5, 2), datetime(2018, 6, 1), 16.75, 0),
            (datetime(2018, 6, 1), datetime(2018, 7, 2), 19.04, 0),
            (datetime(2018, 7, 2), datetime(2018, 8, 1), 16.50, 0),
            (datetime(2018, 8, 1), datetime(2018, 8, 31), 16.09, 0),
            (datetime(2018, 8, 31), datetime(2018, 10, 2), 14.91, 0),
            (datetime(2018, 10, 2), datetime(2018, 11, 1), 14.81, 0),
            (datetime(2018, 11, 1), datetime(2018, 12, 3), 19.28, 0),
        ]:
            rate_plan = RatePlan.objects.get(
                name="2014 Vintage PCIA & Franchise Fee"
            )
            rate_collection = rate_plan.get_latest_rate_collection(start)

            # run bill validation in parallel
            p = multiprocessing.Process(
                target=self.validate_charge,
                args=(
                    self.intervalframe.filter_by_datetime(start, end_limit),
                    [rate_collection.openei_rate_data],
                    expected_charge + unknown_error,
                    RESIDENTIAL_CHARGE_ERROR_RATE,
                ),
            )
            jobs.append(p)
            p.start()

        for job in jobs:
            job.join()

    def test_energy_charges(self):
        # TODO: Investigate billing errors
        jobs = []
        for start, end_limit, expected_charge, unknown_error in [
            (datetime(2018, 1, 2), datetime(2018, 1, 31), 34.46, 0),
            (datetime(2018, 1, 31), datetime(2018, 3, 2), 23.14, 0),
            (datetime(2018, 3, 2), datetime(2018, 4, 2), 27.10, -1.12),
            (datetime(2018, 4, 2), datetime(2018, 5, 2), 23.42, 0),
            (datetime(2018, 5, 2), datetime(2018, 6, 1), 25.25, 0),
            (datetime(2018, 6, 1), datetime(2018, 7, 2), 30.33, 0),
            (datetime(2018, 7, 2), datetime(2018, 8, 1), 25.92, 0),
            (datetime(2018, 8, 1), datetime(2018, 8, 31), 25.80, 0),
            (datetime(2018, 8, 31), datetime(2018, 10, 2), 23.59, 0),
            (datetime(2018, 10, 2), datetime(2018, 11, 1), 23.32, 0),
            (datetime(2018, 11, 1), datetime(2018, 12, 3), 20.70, 0),
        ]:
            rate_plan_1 = RatePlan.objects.get(
                name="EV, Residential Rates for Electric Vehicle Owners"
            )
            rate_collection_1 = rate_plan_1.get_latest_rate_collection(start)
            rate_plan_2 = RatePlan.objects.get(name="Deep Green (Residential)")
            rate_collection_2 = rate_plan_2.get_latest_rate_collection(start)

            # run bill validation in parallel
            p = multiprocessing.Process(
                target=self.validate_charge,
                args=(
                    self.intervalframe.filter_by_datetime(start, end_limit),
                    [
                        rate_collection_1.openei_rate_data,
                        rate_collection_2.openei_rate_data,
                    ],
                    expected_charge + unknown_error,
                    RESIDENTIAL_CHARGE_ERROR_RATE,
                ),
            )
            jobs.append(p)
            p.start()

        for job in jobs:
            job.join()


class TestMCECommercialBill(TestBill):
    """
    PG&E SA ID 7720534682

    MCE E19 TOU Customer since 2018/11/05.
    """

    fixtures = ["reference_model", "customer", "utility_rate"]

    def setUp(self):
        """
        Based on MCE Residential Rates for Electric Vehicle Owners for the 2018
        calendar year.
        """
        load_intervalframe_files()
        self.meter = Meter.objects.get(sa_id=7720534682)
        self.intervalframe = self.meter.intervalframe

    def tearDown(self):
        flush_intervalframe_files()

    def test_kwh_counts(self):
        # TODO: true up counts using TOU rules
        jobs = []
        for start, end_limit, tou_expected_counts in [
            (
                datetime(2018, 1, 4),
                datetime(2018, 2, 2),
                [(3, 119356.8), (4, 165600)],
            ),
            (
                datetime(2018, 2, 2),
                datetime(2018, 3, 6),
                [(3, 115656), (4, 192446.4)],
            ),
            (
                datetime(2018, 3, 6),
                datetime(2018, 4, 4),
                [(3, 132720), (4, 190075.2)],
            ),
            (
                datetime(2018, 4, 4),
                datetime(2018, 5, 4),
                [
                    (0, 8664),
                    (1, 10344),
                    (2, 14985.6),
                    (3, 115152),
                    (4, 179035.2),
                ],
            ),
            (
                datetime(2018, 5, 4),
                datetime(2018, 6, 5),
                [(0, 61920), (1, 73728), (2, 229656)],
            ),
            (
                datetime(2018, 6, 5),
                datetime(2018, 7, 5),
                [(0, 60211.2), (1, 69648), (2, 198998.4)],
            ),
            (
                datetime(2018, 7, 5),
                datetime(2018, 8, 3),
                [(0, 63432), (1, 72403.2), (2, 196380)],
            ),
            (
                datetime(2018, 8, 3),
                datetime(2018, 9, 5),
                [(0, 64819.2), (1, 75727.2), (2, 230608.8)],
            ),
            (
                datetime(2018, 9, 5),
                datetime(2018, 10, 4),
                [(0, 58632), (1, 67641.6), (2, 177808.8)],
            ),
            (
                datetime(2018, 10, 4),
                datetime(2018, 11, 5),
                [
                    (0, 50448),
                    (1, 59640),
                    (2, 163420.8),
                    (3, 10502.4),
                    (4, 27398.4),
                ],
            ),
            (
                datetime(2018, 11, 5),
                datetime(2018, 12, 5),
                [(3, 109483.2 + 10876.8), (4, 178516.8 - 10876.8)],
            ),
        ]:
            for tou_key, expected_count in tou_expected_counts:
                rate_plan = RatePlan.objects.get(
                    name="E19, Medium General Service, Primary"
                )
                rate_collection = rate_plan.get_latest_rate_collection(start)
                intervalframe = self.intervalframe.filter_by_datetime(
                    start, end_limit
                )

                target = self.validate_tou_energy_count
                args = (
                    intervalframe,
                    rate_collection.openei_rate_data,
                    tou_key,
                    expected_count,
                    COMMERCIAL_COUNT_ERROR_RATE,
                )

                # run bill validation in parallel
                p = multiprocessing.Process(target=target, args=args)
                jobs.append(p)
                p.start()

        for job in jobs:
            job.join()

    def test_energy_charges(self):
        # TODO: add additional 2019 charges
        jobs = []
        for start, end_limit, expected_charge, unknown_error in [
            (datetime(2018, 11, 5), datetime(2018, 12, 5), 12943.28, 0),
            (datetime(2018, 12, 5), datetime(2019, 1, 5), 12833.41, 0),
            (datetime(2019, 1, 5), datetime(2019, 2, 5), 16041.27, 0),
            (datetime(2019, 2, 5), datetime(2019, 3, 7), 16611.51, 0),
        ]:
            rate_plan = RatePlan.objects.get(
                name="E19, Medium General Service, Primary"
            )
            rate_collection = rate_plan.get_latest_rate_collection(start)

            # run bill validation in parallel
            p = multiprocessing.Process(
                target=self.validate_charge,
                args=(
                    self.intervalframe.filter_by_datetime(start, end_limit),
                    [rate_collection.openei_rate_data],
                    expected_charge + unknown_error,
                    COMMERCIAL_CHARGE_ERROR_RATE,
                ),
            )
            jobs.append(p)
            p.start()

        for job in jobs:
            job.join()
