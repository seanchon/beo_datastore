from datetime import datetime
import os

from django.test import TestCase

from beo_datastore.libs.fixtures import (
    flush_intervalframe_files,
    load_intervalframe_files,
)
from beo_datastore.settings import BASE_DIR

from load.customer.models import CustomerMeter, OriginFile
from load.customer.scripts import ingest_item_17
from reference.reference_model.models import LoadServingEntity


MIDNIGHT_2018 = datetime(2018, 1, 1, 0, 0)


class TestItem17Ingest(TestCase):
    fixtures = ["reference_model"]

    def setUp(self):
        """
        Copy parquet (dataframe) files to test MEDIA_ROOT.
        """
        load_intervalframe_files()

    def tearDown(self):
        """
        Remove test MEDIA_ROOT and contents.
        """
        flush_intervalframe_files()

    def test_15_min_kw_ingest(self):
        """
        Test ingest of 15-minute Item 17 file with kW readings.
        """
        file = os.path.join(
            BASE_DIR, "load/customer/tests/files/15_min_kw.csv"
        )
        ingest_item_17.run(LoadServingEntity.objects.first(), file)

        # origin file is created
        self.assertEqual(OriginFile.objects.count(), 1)

        # test value ingest/conversion
        # 1.0 kw - 0.5 kw @ 15 minutes = 0.5 kw
        meter = CustomerMeter.objects.last()
        self.assertEqual(
            meter.intervalframe.dataframe.loc[MIDNIGHT_2018]["kw"], 0.5
        )

    def test_15_min_kwh_ingest(self):
        """
        Test ingest of 15-minute Item 17 file with kWh readings.
        """
        file = os.path.join(
            BASE_DIR, "load/customer/tests/files/15_min_kwh.csv"
        )
        ingest_item_17.run(LoadServingEntity.objects.first(), file)

        # origin file is created
        self.assertEqual(OriginFile.objects.count(), 1)

        # test value ingest/conversion
        # 1.0 kwh - 0.5 kwh @ 15 minutes = 2 kw
        meter = CustomerMeter.objects.last()
        self.assertEqual(
            meter.intervalframe.dataframe.loc[MIDNIGHT_2018]["kw"], 2
        )

    def test_60_min_kw_ingest(self):
        """
        Test ingest of 60-minute Item 17 file with kW readings.
        """
        file = os.path.join(
            BASE_DIR, "load/customer/tests/files/60_min_kw.csv"
        )
        ingest_item_17.run(LoadServingEntity.objects.first(), file)

        # origin file is created
        self.assertEqual(OriginFile.objects.count(), 1)

        # test value ingest/conversion
        # 1.0 kw - 0.5 kw @ 60 minutes = 0.5 kw
        meter = CustomerMeter.objects.last()
        self.assertEqual(
            meter.intervalframe.dataframe.loc[MIDNIGHT_2018]["kw"], 0.5
        )

    def test_60_min_kwh_ingest(self):
        """
        Test ingest of 60-minute Item 17 file with kWh readings.
        """
        file = os.path.join(
            BASE_DIR, "load/customer/tests/files/60_min_kwh.csv"
        )
        ingest_item_17.run(LoadServingEntity.objects.first(), file)

        # origin file is created
        self.assertEqual(OriginFile.objects.count(), 1)

        # test value ingest/conversion
        # 1.0 kwh - 0.5 kwh @ 60 minutes = 0.5 kw
        meter = CustomerMeter.objects.last()
        self.assertEqual(
            meter.intervalframe.dataframe.loc[MIDNIGHT_2018]["kw"], 0.5
        )
