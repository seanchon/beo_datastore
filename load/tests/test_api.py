from faker import Factory
import os
import pandas as pd

from rest_framework import status
from rest_framework.test import APITestCase

from django.contrib.auth.models import User

from beo_datastore.libs.api.tests import BasicAuthenticationTestMixin
from beo_datastore.libs.fixtures import (
    flush_intervalframe_files,
    load_intervalframe_files,
)
from beo_datastore.settings import BASE_DIR

from reference.reference_model.models import OriginFile


class TestEndpointsLoad(APITestCase, BasicAuthenticationTestMixin):
    """
    Ensures endpoints are only accessible to logged-in users and are rendered
    without errors.
    """

    fixtures = ["reference_model", "customer"]

    def setUp(self):
        """
        Initialize endpoints to test and loads parquet files.
        """
        load_intervalframe_files()

        # create fake API user
        faker = Factory.create()
        self.user = User.objects.create(
            username=faker.user_name(), email=faker.email(), is_superuser=False
        )

        # test following endpoints
        self.endpoints = ["/v1/load/meter/"]

    def tearDown(self):
        flush_intervalframe_files()

    def test_meter_data_exists(self):
        """
        Test that all formats of meter data are served properly when requesting
        a single data type.
        """
        self.client.force_authenticate(user=self.user)

        base_endpoint = (
            "/v1/load/meter/?start=2018-01-01T00:00:00"
            "&end_limit=2018-01-02T00:00:00&data_types="
        )
        data_types = ["default", "total", "average", "maximum", "minimum"]

        for data_type in data_types:
            endpoint = base_endpoint + data_type
            response = self.client.get(endpoint, format="json")
            self.assertEqual(
                response.status_code, status.HTTP_200_OK, msg=endpoint
            )
            self.assertEqual(
                type(response.data["results"][0]["data"][data_type]),
                pd.DataFrame,
                msg=endpoint,
            )
            self.assertFalse(
                response.data["results"][0]["data"][data_type].empty,
                msg=endpoint,
            )

    def test_meter_multiple_data_exists(self):
        """
        Test that all formats of meter data are served properly when requesting
        multiple data types.
        """
        self.client.force_authenticate(user=self.user)

        base_endpoint = (
            "/v1/load/meter/?start=2018-01-01T00:00:00"
            "&end_limit=2018-01-02T00:00:00&data_types="
        )
        data_types = ["default", "total", "average", "maximum", "minimum"]

        endpoint = base_endpoint + ",".join(data_types)
        response = self.client.get(endpoint, format="json")
        self.assertEqual(
            response.status_code, status.HTTP_200_OK, msg=endpoint
        )
        for data_type in data_types:
            self.assertEqual(
                type(response.data["results"][0]["data"][data_type]),
                pd.DataFrame,
                msg=endpoint,
            )
            self.assertFalse(
                response.data["results"][0]["data"][data_type].empty,
                msg=endpoint,
            )

    def test_meter_data_does_not_exists(self):
        """
        Test that no meter data is served by default.
        """
        self.client.force_authenticate(user=self.user)

        endpoint = "/v1/load/meter/"
        response = self.client.get(endpoint, format="json")
        self.assertEqual(
            response.status_code, status.HTTP_200_OK, msg=endpoint
        )
        self.assertEqual(response.data["results"][0]["data"], {}, msg=endpoint)


class TestFileUpload(APITestCase):
    """
    Ensure expected file-upload behavior.
    """

    fixtures = ["reference_model", "customer"]

    def setUp(self):
        """
        Initialize endpoints to test and loads parquet files.
        """
        load_intervalframe_files()

        # create fake API user
        faker = Factory.create()
        self.user = User.objects.create(
            username=faker.user_name(), email=faker.email(), is_superuser=False
        )

    def test_anonymous_access_unauthorized(self):
        """
        Test HTTP_403_FORBIDDEN status for anonymous access.
        """
        endpoint = "/v1/load/origin_file/"
        response = self.client.get(endpoint, format="json")
        self.assertEqual(
            response.status_code, status.HTTP_403_FORBIDDEN, msg=endpoint
        )

    def test_post_duplicate_files(self):
        """
        Test only one file is uploaded per md5sum.
        """
        endpoint = "/v1/load/origin_file/"
        file_location = "load/tests/files/test.csv"
        self.client.force_authenticate(user=self.user)

        # 0 OriginFiles
        OriginFile.objects.all().delete()
        response = self.client.get(endpoint, format="json")
        self.assertEqual(response.data.get("count"), 0)

        # 1 OriginFile
        with open(os.path.join(BASE_DIR, file_location), "rb") as file:
            response = self.client.post(
                endpoint,
                {"file": file, "load_serving_entity_id": 2},
                format="multipart",
            )

        self.assertEqual(response.status_code, status.HTTP_204_NO_CONTENT)
        response = self.client.get(endpoint, format="json")
        self.assertEqual(response.data.get("count"), 1)

        # 1 OriginFile
        with open(os.path.join(BASE_DIR, file_location), "rb") as file:
            response = self.client.post(
                endpoint,
                {"file": file, "load_serving_entity_id": 2},
                format="multipart",
            )

        self.assertEqual(response.status_code, status.HTTP_204_NO_CONTENT)
        response = self.client.get(endpoint, format="json")
        self.assertEqual(response.data.get("count"), 1)

    def tearDown(self):
        flush_intervalframe_files()
