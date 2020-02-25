from datetime import datetime
import ntpath
import os

from django.test import TestCase

from beo_datastore.libs.fixtures import (
    flush_intervalframe_files,
    load_intervalframe_files,
)
from beo_datastore.libs.utils import chunks
from beo_datastore.settings import BASE_DIR

from load.customer.models import CustomerMeter, OriginFile
from load.tasks import ingest_origin_file, ingest_meters
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
        for origin_file in OriginFile.objects.all():
            if origin_file.db_exists:
                origin_file.db_drop()

    def create_origin_file(self, file):
        """
        Perform ingest of OriginFile and associated meters.
        """
        with open(file, "rb") as file:
            name = ntpath.basename(file.name)
            origin_file, _ = OriginFile.get_or_create(
                load_serving_entity=LoadServingEntity.objects.first(),
                file=file,
                name=name,
            )

        return origin_file

    def ingest_origin_file_meters(self, origin_file_id):
        """
        Runs load.tasks.ingest_origin_file_meters synchronously.
        """
        ingest_origin_file(origin_file_id)
        origin_file = OriginFile.objects.get(id=origin_file_id)
        for sa_ids in chunks(origin_file.db_get_sa_ids(), n=50):
            ingest_meters(origin_file.id, sa_ids)

    def test_15_min_kw_ingest(self):
        """
        Test ingest of 15-minute Item 17 file with kW readings.
        """
        file = os.path.join(
            BASE_DIR, "load/customer/tests/files/15_min_kw.csv"
        )
        origin_file = self.create_origin_file(file)
        self.ingest_origin_file_meters(origin_file.id)

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
        origin_file = self.create_origin_file(file)
        self.ingest_origin_file_meters(origin_file.id)

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
        origin_file = self.create_origin_file(file)
        self.ingest_origin_file_meters(origin_file.id)

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
        origin_file = self.create_origin_file(file)
        self.ingest_origin_file_meters(origin_file.id)

        # origin file is created
        self.assertEqual(OriginFile.objects.count(), 1)

        # test value ingest/conversion
        # 1.0 kwh - 0.5 kwh @ 60 minutes = 0.5 kw
        meter = CustomerMeter.objects.last()
        self.assertEqual(
            meter.intervalframe.dataframe.loc[MIDNIGHT_2018]["kw"], 0.5
        )

    def test_dupicate_origin_files_ingest(self):
        """
        Test that running the same ingest twice does not increase origin file
        count on second run.
        """
        file = os.path.join(
            BASE_DIR, "load/customer/tests/files/60_min_kwh.csv"
        )
        self.create_origin_file(file)
        origin_file_count = OriginFile.objects.count()
        self.create_origin_file(file)

        self.assertEqual(origin_file_count, OriginFile.objects.count())

    def test_duplicate_meters_ingest(self):
        """
        Test that running the same ingest twice does not increase meter count
        on second run.
        """
        file = os.path.join(
            BASE_DIR, "load/customer/tests/files/60_min_kwh.csv"
        )
        origin_file = self.create_origin_file(file)
        self.ingest_origin_file_meters(origin_file.id)
        meter_count = origin_file.meters.count()
        self.ingest_origin_file_meters(origin_file.id)

        self.assertEqual(meter_count, origin_file.meters.count())
