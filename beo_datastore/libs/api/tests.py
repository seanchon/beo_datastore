from rest_framework import status


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
            # TODO: update assertion to handle dynamic-rest
            self.assertNotEqual(response.data.get("results"), [], msg=endpoint)
