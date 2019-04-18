from faker import Factory

from rest_framework.test import APITestCase

from django.contrib.auth.models import User

from beo_datastore.libs.api.tests import BasicAuthenticationTestMixin
from beo_datastore.libs.fixtures import (
    flush_intervalframe_fixtures,
    load_intervalframe_fixtures,
)


class TestEndpointsCustomer(APITestCase, BasicAuthenticationTestMixin):
    """
    Ensures endpoints are only accessible to logged-in users and are rendered
    without errors.
    """

    fixtures = ["reference_model", "customer"]

    def setUp(self):
        """
        Initialize endpoints to test and loads parquet files.
        """
        load_intervalframe_fixtures()

        # create fake API user
        faker = Factory.create()
        self.user = User.objects.create(
            username=faker.user_name(), email=faker.email(), is_superuser=False
        )

        # test following endpoints
        self.endpoints = [
            "/v1/load/customer_meter/",
            "/v1/load/customer_service_drop/",
        ]

    def tearDown(self):
        flush_intervalframe_fixtures()
