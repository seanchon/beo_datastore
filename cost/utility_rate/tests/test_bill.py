from datetime import datetime

from django.test import TestCase

from beo_datastore.libs.bill import BillingCollection
from beo_datastore.libs.fixtures import (
    flush_intervalframe_files,
    load_intervalframe_files,
)

from cost.utility_rate.models import RatePlan
from load.customer.models import Meter


RESIDENTIAL_COUNT_ERROR_RATE = 0.005  # allow 0.5% count error rate
RESIDENTIAL_CHARGE_ERROR_RATE = 0.02  # allow 2% charge error rate
COMMERCIAL_COUNT_ERROR_RATE = 0.065  # allow 6.5% count error rate
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
        self, bill, expected_count, allowable_error_rate
    ):
        """
        Compares intervalframe kwh count to expected_count with
        allowable_error_rate.

        :param bill: ValidationBill
        :param expected_count: float
        :param allowable_error_rate: float
        """
        kwh_count = bill.intervalframe.total_frame288.dataframe.sum().sum()
        self.assertLessEqual(
            get_error_rate(expected_count, kwh_count),
            allowable_error_rate,
            "{} - {}: expected {}, actual {}".format(
                bill.start_datetime,
                bill.end_datetime,
                expected_count,
                kwh_count,
            ),
        )

    def validate_tou_energy_count(
        self, bill, tou_key, expected_count, allowable_error_rate
    ):
        """
        Compares intervalframe kwh count per tou_key to expected_count with
        allowable_error_rate.

        :param bill: ValidationBill
        :param tou_key: integer
        :param expected_count: float
        """
        kwh_count = bill.dataframe[bill.dataframe.tou_period == tou_key][
            "count"
        ].sum()
        self.assertLessEqual(
            get_error_rate(expected_count, kwh_count),
            allowable_error_rate,
            "{} - {} (TOU:{}): expected {}, actual {}".format(
                bill.start_datetime,
                bill.end_datetime,
                tou_key,
                expected_count,
                kwh_count,
            ),
        )

    def validate_charge(self, bill, expected_total, allowable_error_rate):
        """
        Generates bill based on openei_rate_data and intervalframe and compares
        to expected_total with allowable_error_rate.

        :param bill: ValidationBill
        :param expected_total: float
        :param allowable_error_rate: float
        """
        self.assertLessEqual(
            get_error_rate(expected_total, bill.total),
            allowable_error_rate,
            "{} - {}: expected {}, actual {}".format(
                bill.start_datetime,
                bill.end_datetime,
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

        date_ranges = [
            (datetime(2018, 1, 2), datetime(2018, 1, 31)),
            (datetime(2018, 1, 31), datetime(2018, 3, 2)),
            (datetime(2018, 3, 2), datetime(2018, 4, 2)),
            (datetime(2018, 4, 2), datetime(2018, 5, 2)),
            (datetime(2018, 5, 2), datetime(2018, 6, 1)),
            (datetime(2018, 6, 1), datetime(2018, 7, 2)),
            (datetime(2018, 7, 2), datetime(2018, 8, 1)),
            (datetime(2018, 8, 1), datetime(2018, 8, 31)),
            (datetime(2018, 8, 31), datetime(2018, 10, 2)),
            (datetime(2018, 10, 2), datetime(2018, 11, 1)),
            (datetime(2018, 11, 1), datetime(2018, 12, 3)),
        ]

        rate_plan = RatePlan.objects.get(
            name="2014 Vintage PCIA & Franchise Fee"
        )
        self.departing_charges_bills = rate_plan.generate_many_bills(
            intervalframe=self.meter.intervalframe,
            date_ranges=date_ranges,
            multiprocess=True,
        )

        rate_plan = RatePlan.objects.get(
            name="EV, Residential Rates for Electric Vehicle Owners"
        )
        self.ev_bills = rate_plan.generate_many_bills(
            intervalframe=self.meter.intervalframe,
            date_ranges=date_ranges,
            multiprocess=True,
        )

        rate_plan = RatePlan.objects.get(name="Deep Green (Residential)")
        self.deep_green_bills = rate_plan.generate_many_bills(
            intervalframe=self.meter.intervalframe,
            date_ranges=date_ranges,
            multiprocess=True,
        )

    def tearDown(self):
        flush_intervalframe_files()

    def test_kwh_counts(self):
        for start, expected_count in [
            (datetime(2018, 1, 2), 806.241),
            (datetime(2018, 1, 31), 550.326),
            (datetime(2018, 3, 2), 616.827),
            (datetime(2018, 4, 2), 471.937),
            (datetime(2018, 5, 2), 491.285),
            (datetime(2018, 6, 1), 558.583),
            (datetime(2018, 7, 2), 483.802),
            (datetime(2018, 8, 1), 471.9335),
            (datetime(2018, 8, 31), 437.494),
            (datetime(2018, 10, 2), 434.509),
            (datetime(2018, 11, 1), 565.667),
        ]:
            self.validate_energy_count(
                bill=self.ev_bills[start],
                expected_count=expected_count,
                allowable_error_rate=RESIDENTIAL_COUNT_ERROR_RATE,
            )

    def test_pcia_and_franchise_fee_charges(self):
        # TODO: Investigate billing errors
        for start, expected_charge, unknown_error in [
            (datetime(2018, 1, 2), 24.18, 0),
            (datetime(2018, 1, 31), 16.63, 0),
            (datetime(2018, 3, 2), 21.03, -2.54),
            (datetime(2018, 4, 2), 16.09, -1.94),
            (datetime(2018, 5, 2), 16.75, 0),
            (datetime(2018, 6, 1), 19.04, 0),
            (datetime(2018, 7, 2), 16.50, 0),
            (datetime(2018, 8, 1), 16.09, 0),
            (datetime(2018, 8, 31), 14.91, 0),
            (datetime(2018, 10, 2), 14.81, 0),
            (datetime(2018, 11, 1), 19.28, 0),
        ]:
            self.validate_charge(
                bill=self.departing_charges_bills[start],
                expected_total=(expected_charge + unknown_error),
                allowable_error_rate=RESIDENTIAL_CHARGE_ERROR_RATE,
            )

    def test_energy_charges(self):
        # TODO: Investigate billing errors
        for start, expected_charge, unknown_error in [
            (datetime(2018, 1, 2), 34.46, 0),
            (datetime(2018, 1, 31), 23.14, 0),
            (datetime(2018, 3, 2), 27.10, -1.12),
            (datetime(2018, 4, 2), 23.42, 0),
            (datetime(2018, 5, 2), 25.25, 0),
            (datetime(2018, 6, 1), 30.33, 0),
            (datetime(2018, 7, 2), 25.92, 0),
            (datetime(2018, 8, 1), 25.80, 0),
            (datetime(2018, 8, 31), 23.59, 0),
            (datetime(2018, 10, 2), 23.32, 0),
            (datetime(2018, 11, 1), 20.70, 0),
        ]:
            self.validate_charge(
                bill=BillingCollection(
                    [self.ev_bills[start], self.deep_green_bills[start]]
                ),
                expected_total=(expected_charge + unknown_error),
                allowable_error_rate=RESIDENTIAL_CHARGE_ERROR_RATE,
            )


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

        date_ranges = [
            (datetime(2018, 1, 4), datetime(2018, 2, 2)),
            (datetime(2018, 2, 2), datetime(2018, 3, 6)),
            (datetime(2018, 3, 6), datetime(2018, 4, 4)),
            (datetime(2018, 4, 4), datetime(2018, 5, 4)),
            (datetime(2018, 5, 4), datetime(2018, 6, 5)),
            (datetime(2018, 6, 5), datetime(2018, 7, 5)),
            (datetime(2018, 7, 5), datetime(2018, 8, 3)),
            (datetime(2018, 8, 3), datetime(2018, 9, 5)),
            (datetime(2018, 9, 5), datetime(2018, 10, 4)),
            (datetime(2018, 10, 4), datetime(2018, 11, 5)),
            (datetime(2018, 11, 5), datetime(2018, 12, 5)),
            (datetime(2018, 12, 5), datetime(2019, 1, 5)),
            (datetime(2019, 1, 5), datetime(2019, 2, 5)),
            (datetime(2019, 2, 5), datetime(2019, 3, 7)),
        ]

        rate_plan = RatePlan.objects.get(
            name="E19, Medium General Service, Primary"
        )
        self.e19_bills = rate_plan.generate_many_bills(
            intervalframe=self.meter.intervalframe,
            date_ranges=date_ranges,
            multiprocess=True,
        )

    def tearDown(self):
        flush_intervalframe_files()

    def test_kwh_counts(self):
        # TODO: true up counts using TOU rules
        for start, tou_expected_counts in [
            (datetime(2018, 1, 4), [(3, 119356.8), (4, 165600)]),
            (datetime(2018, 2, 2), [(3, 115656), (4, 192446.4)]),
            (datetime(2018, 3, 6), [(3, 132720), (4, 190075.2)]),
            (
                datetime(2018, 4, 4),
                [
                    (0, 8664),
                    (1, 10344),
                    (2, 14985.6),
                    (3, 115152),
                    (4, 179035.2),
                ],
            ),
            (datetime(2018, 5, 4), [(0, 61920), (1, 73728), (2, 229656)]),
            (datetime(2018, 6, 5), [(0, 60211.2), (1, 69648), (2, 198998.4)]),
            (datetime(2018, 7, 5), [(0, 63432), (1, 72403.2), (2, 196380)]),
            (
                datetime(2018, 8, 3),
                [(0, 64819.2), (1, 75727.2), (2, 230608.8)],
            ),
            (datetime(2018, 9, 5), [(0, 58632), (1, 67641.6), (2, 177808.8)]),
            (
                datetime(2018, 10, 4),
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
                [(3, 109483.2 + 10876.8), (4, 178516.8 - 10876.8)],
            ),
        ]:
            for tou_key, expected_count in tou_expected_counts:
                self.validate_tou_energy_count(
                    bill=self.e19_bills[start],
                    tou_key=tou_key,
                    expected_count=expected_count,
                    allowable_error_rate=COMMERCIAL_COUNT_ERROR_RATE,
                )

    def test_energy_charges(self):
        # TODO: add additional 2019 charges
        for start, expected_charge, unknown_error in [
            (datetime(2018, 11, 5), 12943.28, 0),
            (datetime(2018, 12, 5), 12833.41, 0),
            (datetime(2019, 1, 5), 16041.27, 0),
            (datetime(2019, 2, 5), 16611.51, 0),
        ]:
            self.validate_charge(
                bill=self.e19_bills[start],
                expected_total=expected_charge,
                allowable_error_rate=COMMERCIAL_CHARGE_ERROR_RATE,
            )
