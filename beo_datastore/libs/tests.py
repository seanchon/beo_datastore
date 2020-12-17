from typing import Any

import pandas as pd
from rest_framework import status
from unittest import TestCase


class BasicAuthenticationTestMixin(object):
    """
    Tests endpoints for basic authentication restriction.
    """

    def test_anonymous_access_unauthorized(self):
        """
        Test HTTP_403_FORBIDDEN status for anonymous access.
        """
        for endpoint in self.endpoints:
            response = self.client.get(endpoint, format="json")
            self.assertEqual(
                response.status_code, status.HTTP_403_FORBIDDEN, msg=endpoint
            )

    def test_user_access_ok(self):
        """
        Test HTTP_200_OK status for logged in user access.
        """
        self.client.force_authenticate(user=self.user)

        for endpoint in self.endpoints:
            response = self.client.get(endpoint, format="json")
            self.assertEqual(
                response.status_code, status.HTTP_200_OK, msg=endpoint
            )

    def test_endpoint_contains_objects(self):
        """
        Test endpoints have test data loaded as fixtures.

        Broken endpoints may still render properly in tests when no objects
        exist to populate the endpoint.
        """
        self.client.force_authenticate(user=self.user)

        for endpoint in self.endpoints:
            response = self.client.get(endpoint, format="json")
            for key, value in response.data.get("results").items():
                self.assertNotEqual(
                    response.data.get("results"),
                    [],
                    msg="endpoint: {}, key: {}".format(endpoint, key),
                )


class NavigaderTestCase(TestCase):
    def assertEqual(self, first: Any, second: Any, msg: Any = ...) -> None:
        """
        Overrides the `TestCase.assertEqual` method to handle for pandas
        DataFrame and Series arguments. If the first argument is not a pandas
        DataFrame or Series, calls the base class method.

        :param first: first item in equality check
        :param second: second item in equality check
        :param msg: failure message
        """
        if isinstance(first, pd.DataFrame) or isinstance(first, pd.Series):
            return self.assertTrue((first == second).all(), msg)
        return super().assertEqual(first, second, msg)

    def assertTrue(self, expr: Any, msg: Any = ...) -> None:
        """
        Overrides the `TestCase.assertTrue` method to handle for pandas
        DataFrame and Series arguments. If the first argument is not a pandas
        DataFrame or Series, calls the base class method.

        :param expr: expression to test for truthiness
        :param msg: failure message
        """
        if isinstance(expr, pd.DataFrame) or isinstance(expr, pd.Series):
            return self.assertTrue(expr.all(), msg)
        return super().assertTrue(expr, msg)

    def assertAlmostEqual(
        self,
        first: Any,
        second: Any,
        places: int = None,
        msg: Any = ...,
        delta: float = None,
    ) -> None:
        """

        Overrides the `TestCase.assertAlmostEqual` method to handle for pandas
        DataFrame and Series arguments. If the first argument is not a pandas
        DataFrame or Series, calls the base class method.

        :param first: first item in equality check
        :param second: second item in equality check
        :param places: number of decimal places to round to
        :param msg: failure message
        :param delta: maximum permissible delta between corresponding values
        """
        if isinstance(first, pd.DataFrame) or isinstance(first, pd.Series):
            if delta is not None:
                self.assertTrue(abs(first - second) <= delta)
            else:
                if places is None:
                    places = 7
                self.assertTrue(round(abs(first - second), places) == 0)
        else:
            super().assertAlmostEqual(first, second, places, msg, delta)
