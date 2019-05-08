from datetime import datetime
import os
import multiprocessing

from django.test import TestCase

from beo_datastore.libs.bill import BillingCollection, ValidationBill
from beo_datastore.libs.intervalframe import ValidationIntervalFrame
from beo_datastore.settings import BASE_DIR

from cost.utility_rate.models import RateCollection


ALLOWABLE_ERROR_RATE = 0.02  # allow 2% error rate


def get_error_rate(expected, actual):
    """
    Return difference of expected and actual as an error rate.
    """
    return abs(expected - actual) / expected


class TestMCEResidentialBill(TestCase):
    fixtures = ["reference_model", "utility_rate"]

    def setUp(self):
        """
        Based on MCE Residential Rates for Electric Vehicle Owners for the 2018
        calendar year.
        """
        self.intervalframe = ValidationIntervalFrame.csv_file_to_intervalframe(
            csv_location=os.path.join(
                BASE_DIR,
                "cost/utility_rate/tests/data/mce_ev_residential_2018.csv",
            ),
            index_column="start",
            convert_to_datetime=True,
        )

    def validate_bill(self, intervalframe, rate_date_list, expected_total):
        """
        Generates bill based on openei_rate_data and intervalframe and compares
        to expected_total.

        :param intervalframe: ValidationIntervalFrame
        :param rate_data_list: list of OpenEIRateData objects
        :param expected_total: float
        """
        bill = BillingCollection(
            [
                ValidationBill(intervalframe, openei_rate_data)
                for openei_rate_data in rate_date_list
            ]
        )
        self.assertLessEqual(
            get_error_rate(expected_total, bill.total),
            ALLOWABLE_ERROR_RATE,
            "{} - {}: expected {}, actual {}".format(
                intervalframe.start_datetime,
                intervalframe.end_datetime,
                expected_total,
                bill.total,
            ),
        )

    def test_pcia_and_franchise_fee_charges(self):
        # TODO: Investigate billing errors
        jobs = []
        for start, end_limit, count, expected_charge, unknown_error in [
            (datetime(2018, 1, 2), datetime(2018, 1, 31), 806.241, 24.18, 0),
            (datetime(2018, 1, 31), datetime(2018, 3, 2), 550.326, 16.63, 0),
            (
                datetime(2018, 3, 2),
                datetime(2018, 4, 2),
                616.827,
                21.03,
                -2.54,
            ),
            (
                datetime(2018, 4, 2),
                datetime(2018, 5, 2),
                471.937,
                16.09,
                -1.94,
            ),
            (datetime(2018, 5, 2), datetime(2018, 6, 1), 491.285, 16.75, 0),
            (datetime(2018, 6, 1), datetime(2018, 7, 2), 558.583, 19.04, 0),
            (datetime(2018, 7, 2), datetime(2018, 8, 1), 483.802, 16.50, 0),
            (datetime(2018, 8, 1), datetime(2018, 8, 31), 471.9335, 16.09, 0),
            (datetime(2018, 8, 31), datetime(2018, 10, 2), 437.494, 14.91, 0),
            (datetime(2018, 10, 2), datetime(2018, 11, 1), 434.509, 14.81, 0),
            (datetime(2018, 11, 1), datetime(2018, 12, 3), 565.667, 19.28, 0),
        ]:
            openei_rate_data = (
                RateCollection.objects.filter(
                    rate_plan__name__contains="2014 Vintage PCIA & Franchise"
                )
                .filter(effective_date__lte=start)
                .last()
            ).openei_rate_data

            # run bill validation in parallel
            p = multiprocessing.Process(
                target=self.validate_bill,
                args=(
                    self.intervalframe.filter_by_datetime(start, end_limit),
                    [openei_rate_data],
                    expected_charge + unknown_error,
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
            openei_rate_data_1 = (
                RateCollection.objects.filter(
                    rate_plan__name__contains=(
                        "EV, Residential Rates for Electric Vehicle Owners"
                    )
                )
                .filter(effective_date__lte=start)
                .last()
            ).openei_rate_data
            openei_rate_data_2 = (
                RateCollection.objects.filter(
                    rate_plan__name__contains=("Deep Green (Residential)")
                )
                .filter(effective_date__lte=start)
                .last()
            ).openei_rate_data

            # run bill validation in parallel
            p = multiprocessing.Process(
                target=self.validate_bill,
                args=(
                    self.intervalframe.filter_by_datetime(start, end_limit),
                    [openei_rate_data_1, openei_rate_data_2],
                    expected_charge + unknown_error,
                ),
            )
            jobs.append(p)
            p.start()

        for job in jobs:
            job.join()
